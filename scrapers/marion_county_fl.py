from __future__ import annotations

# NOTE: Verify PDF link selector against live page before production use.
#
# The Marion County "Tax Deeds Surplus Funds" PDF uses plain text layout —
# pdfplumber finds 0 tables. All parsing uses extract_text() line-by-line.
#
# Actual column order (verified 2026-03-30):
#   Sale number | Sale date (YYYY-MM-DD) | Tax number | Parcel number | Current balance
#
# Owner name is NOT present in this PDF; owner_name is left blank for
# skip-trace enrichment to populate later.

import io
import re
from datetime import datetime, timezone

import httpx
import pdfplumber
from bs4 import BeautifulSoup

from .base import BaseScraper, SurplusRecord

_BASE_URL = (
    "https://www.marioncountyclerk.org/departments/"
    "records-recording/tax-deeds-and-lands-available-for-taxes/"
    "unclaimed-funds/"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

# Lines that are page headers, not data
_SKIP_PREFIXES = ("tax deeds", "report run", "sale number")


def _is_header_line(line: str) -> bool:
    low = line.lower().strip()
    return not low or any(low.startswith(p) for p in _SKIP_PREFIXES)


def _parse_date(date_str: str) -> datetime:
    date_str = date_str.strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {date_str!r}")


def _parse_amount(raw: str) -> float:
    cleaned = re.sub(r"[^\d.]", "", raw)
    return float(cleaned)


class MarionCountyFLScraper(BaseScraper):
    county_slug = "marion-county-fl"
    county_label = "Marion County, FL"

    async def fetch(self) -> list[SurplusRecord]:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            page_resp = await client.get(_BASE_URL, headers=_HEADERS)
            page_resp.raise_for_status()

            soup = BeautifulSoup(page_resp.text, "html.parser")

            # Require BOTH "surplus" and "funds" in the link text or href to
            # avoid matching the "Tax Deeds Surplus Claim Form" PDF that appears
            # earlier on the same page.
            pdf_link = None
            for tag in soup.find_all("a", href=True):
                href = tag["href"]
                text = tag.get_text(strip=True).lower()
                href_low = href.lower()
                if ("surplus" in text and "funds" in text) or (
                    "surplus" in href_low and "funds" in href_low
                ):
                    pdf_link = href
                    break

            if pdf_link is None:
                return []

            if not pdf_link.startswith("http"):
                base = "https://www.marioncountyclerk.org"
                pdf_link = base + pdf_link if pdf_link.startswith("/") else base + "/" + pdf_link

            pdf_resp = await client.get(pdf_link, headers=_HEADERS)
            pdf_resp.raise_for_status()
            pdf_bytes = pdf_resp.content

        records: list[SurplusRecord] = []

        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                for line in text.splitlines():
                    if _is_header_line(line):
                        continue
                    parts = line.split()
                    # Expected: sale_num  sale_date  tax_num  parcel_num  amount
                    if len(parts) < 5:
                        continue
                    try:
                        case = parts[0]
                        sale_date = _parse_date(parts[1])
                        # parts[2] is tax number — not used
                        parcel = parts[3]
                        amount = _parse_amount(parts[4])
                    except (ValueError, IndexError):
                        continue

                    if amount < 5000:
                        continue
                    if not self.is_within_window(sale_date):
                        continue

                    records.append(
                        SurplusRecord(
                            owner_name="",
                            property_address=parcel,
                            case_number=case,
                            surplus_amount=amount,
                            sale_date=sale_date,
                            county=self.county_label,
                            raw_source=pdf_link,
                        )
                    )

        return records
