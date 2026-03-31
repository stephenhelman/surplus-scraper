from __future__ import annotations

# NOTE: Column indices must be verified against live HTML before running in
# production. Inspect the actual page response to confirm table structure.

from datetime import datetime, timezone
from typing import Optional

import httpx
from bs4 import BeautifulSoup

from .base import BaseScraper, SurplusRecord

_URL = "https://or.occompt.com/recorder/tdsmweb/applicationSearch.jsp"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/x-www-form-urlencoded",
}


class OrangeCountyFLScraper(BaseScraper):
    county_slug = "orange-county-fl"
    county_label = "Orange County, FL"

    async def fetch(self) -> list[SurplusRecord]:
        params = {"balanceType": "SURPLUS", "maxResults": "500"}

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(_URL, data=params, headers=_HEADERS)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")
        rows = soup.select("table.resultsTable tbody tr")

        records: list[SurplusRecord] = []
        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            try:
                owner = cols[0].get_text(strip=True)
                address = cols[1].get_text(strip=True)
                case = cols[2].get_text(strip=True)
                date_str = cols[3].get_text(strip=True)
                amount_str = cols[4].get_text(strip=True).replace("$", "").replace(",", "")

                sale_date = datetime.strptime(date_str, "%m/%d/%Y").replace(
                    tzinfo=timezone.utc
                )
                amount = float(amount_str)
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
                    raw_source=_URL,
                )
            )

        return records
