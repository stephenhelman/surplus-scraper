from __future__ import annotations

from scrapers.base import SurplusRecord


def normalize(records: list[SurplusRecord]) -> list[SurplusRecord]:
    seen_cases: set[str] = set()
    cleaned: list[SurplusRecord] = []

    for r in records:
        r.owner_name = r.owner_name.strip().title()
        r.property_address = r.property_address.strip().title()
        r.case_number = r.case_number.strip()

        if r.case_number in seen_cases:
            continue
        seen_cases.add(r.case_number)
        cleaned.append(r)

    return cleaned
