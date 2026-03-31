from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional


@dataclass
class SurplusRecord:
    owner_name: str
    property_address: str
    case_number: str
    surplus_amount: float
    sale_date: datetime
    county: str
    raw_source: Optional[str] = None


class BaseScraper(abc.ABC):
    county_slug: str
    county_label: str

    @abc.abstractmethod
    async def fetch(self) -> list[SurplusRecord]:
        ...

    def is_within_window(self, date: datetime, months: int = 12) -> bool:
        if date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)
        cutoff = datetime.now(timezone.utc) - timedelta(days=months * 30)
        return date > cutoff
