from __future__ import annotations

# NOTE: Verify PDF link selector against live page before production use.
#
# The leeclerk.org site is served behind Akamai CDN which blocks automated
# requests with a 403 Access Denied. If the page returns a non-200 status,
# fetch() logs a warning and returns an empty list rather than crashing the
# pipeline. When access is restored, the PDF table structure should be
# verified: current column assumptions are owner[0], address[1], case[2],
# date[3], amount[4].

import io
import logging
import re
from datetime import datetime, timezone

import httpx
import pdfplumber
from bs4 import BeautifulSoup

from .base import BaseScraper, SurplusRecord

_BASE_URL = (
    "https://www.leeclerk.org/departments/courts/"
    "property-sales/tax-deed-sales/tax-deed-reports"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

logger = logging.getLogger(__name__)

_DATE_FORMATS = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d"]


def _parse_date(date_str: str) -> datetime:
    date_str = date_str.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str!r}")


def _parse_amount(raw: str) -> float:
    cleaned = re.sub(r"[^\d.]", "", raw)
    return float(cleaned)


class LeeCountyFLScraper(BaseScraper):
    county_slug = "lee-county-fl"
    county_label = "Lee County, FL"

    async def fetch(self) -> list[SurplusRecord]:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            page_resp = await client.get(_BASE_URL, headers=_HEADERS)

            if page_resp.status_code != 200:
                logger.warning(
                    "[lee-county-fl] Base page returned HTTP %d — "
                    "site may be blocking automated requests (Akamai CDN). "
                    "Returning empty result set.",
                    page_resp.status_code,
                )
                return []

            soup = BeautifulSoup(page_resp.text, "html.parser")
            pdf_link = None
            for tag in soup.find_all("a", href=True):
                href = tag["href"]
                if "surplus" in href.lower() or "surplus" in tag.get_text(strip=True).lower():
                    pdf_link = href
                    break

            if pdf_link is None:
                logger.warning("[lee-county-fl] No surplus PDF link found on page.")
                return []

            if not pdf_link.startswith("http"):
                base = "https://www.leeclerk.org"
                pdf_link = base + pdf_link if pdf_link.startswith("/") else base + "/" + pdf_link

            pdf_resp = await client.get(pdf_link, headers=_HEADERS)
            if pdf_resp.status_code != 200:
                logger.warning(
                    "[lee-county-fl] PDF download returned HTTP %d for %s",
                    pdf_resp.status_code,
                    pdf_link,
                )
                return []

            pdf_bytes = pdf_resp.content

        records: list[SurplusRecord] = []

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for i, row in enumerate(table):
                        if i == 0:
                            continue
                        if not row or len(row) < 5:
                            continue
                        try:
                            owner = (row[0] or "").strip()
                            address = (row[1] or "").strip()
                            case = (row[2] or "").strip()
                            date_str = (row[3] or "").strip()
                            amount_str = (row[4] or "").strip()

                            if not owner or not amount_str:
                                continue

                            sale_date = _parse_date(date_str)
                            amount = _parse_amount(amount_str)
                        except (ValueError, IndexError):
                            continue

                        if amount < 5000:
                            continue
                        if not self.is_within_window(sale_date):
                            continue

                        records.append(
                            SurplusRecord(
                                owner_name=owner,
                                property_address=address,
                                case_number=case,
                                surplus_amount=amount,
                                sale_date=sale_date,
                                county=self.county_label,
                                raw_source=pdf_link,
                            )
                        )

        return records
