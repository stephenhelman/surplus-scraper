from __future__ import annotations

import asyncio
import os
from datetime import timezone

import httpx

from scrapers.base import SurplusRecord

BATCHDATA_API_KEY = os.getenv("BATCHDATA_API_KEY", "")
SKIP_TRACE_MIN_SURPLUS = float(os.getenv("SKIP_TRACE_MIN_SURPLUS", "15000"))
MAX_ENRICH_PER_RUN = int(os.getenv("MAX_ENRICH_PER_RUN", "100"))
COST_PER_RECORD = float(os.getenv("COST_PER_RECORD", "0.35"))

_API_URL = "https://api.batchdata.com/api/v1/property/skip-trace"
_BATCH_SIZE = 50


def skip_trace_enabled() -> bool:
    return bool(BATCHDATA_API_KEY)


def record_to_dict(
    r: SurplusRecord,
    enriched: bool,
    phone1: str = "",
    phone2: str = "",
    email: str = "",
) -> dict:
    sale_date = r.sale_date
    if sale_date.tzinfo is None:
        sale_date = sale_date.replace(tzinfo=timezone.utc)
    return {
        "ownerName": r.owner_name,
        "propertyAddress": r.property_address,
        "caseNumber": r.case_number,
        "surplusAmount": r.surplus_amount,
        "saleDate": sale_date.isoformat(),
        "county": r.county,
        "rawSource": r.raw_source,
        "phone1": phone1,
        "phone2": phone2,
        "email": email,
        "status": "New",
        "notes": "",
        "lastContacted": "",
        "enriched": enriched,
    }


async def enrich_records(
    records: list[SurplusRecord],
) -> tuple[list[dict], int, float]:
    """
    Returns: (enriched_leads, eligible_count, actual_cost)
    """
    # Re-read env at call time so monkeypatching in tests works
    api_key = os.getenv("BATCHDATA_API_KEY", "")
    min_surplus = float(os.getenv("SKIP_TRACE_MIN_SURPLUS", "15000"))
    max_enrich = int(os.getenv("MAX_ENRICH_PER_RUN", "100"))
    cost_per = float(os.getenv("COST_PER_RECORD", "0.35"))

    # 1. Split by threshold
    eligible = [r for r in records if r.surplus_amount >= min_surplus]
    ineligible = [r for r in records if r.surplus_amount < min_surplus]

    # 2. Sort eligible descending
    eligible.sort(key=lambda r: r.surplus_amount, reverse=True)

    # 3. Cap
    dropped_eligible: list[SurplusRecord] = []
    if len(eligible) > max_enrich:
        dropped = len(eligible) - max_enrich
        print(
            f"[skip_trace] Capped at {max_enrich}. "
            f"{dropped} high-value records dropped."
        )
        dropped_eligible = eligible[max_enrich:]
        eligible = eligible[:max_enrich]

    # 4. Log intent
    est_cost = len(eligible) * cost_per
    print(
        f"[skip_trace] Enriching {len(eligible)} of {len(records)} records "
        f"(threshold: ${min_surplus:,.0f}, cap: {max_enrich}). "
        f"Est. cost: ${est_cost:.2f}"
    )

    # 5. Early exit if no API key
    if not api_key:
        print("[skip_trace] No API key — skipping enrichment.")
        all_as_dicts = [record_to_dict(r, enriched=False) for r in records]
        return all_as_dicts, 0, 0.0

    # 6. Enrich in batches of 50
    enriched_results: dict[str, dict] = {}  # case_number -> contact data

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    async with httpx.AsyncClient(timeout=30) as client:
        for batch_start in range(0, len(eligible), _BATCH_SIZE):
            batch = eligible[batch_start : batch_start + _BATCH_SIZE]
            payload = []
            for r in batch:
                parts = r.owner_name.split(None, 1)
                first = parts[0] if parts else ""
                last = parts[1] if len(parts) > 1 else ""
                payload.append(
                    {
                        "firstName": first,
                        "lastName": last,
                        "address": r.property_address,
                    }
                )
            try:
                resp = await client.post(
                    _API_URL,
                    json={"requests": payload},
                    headers=headers,
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", data) if isinstance(data, dict) else data
                for i, r in enumerate(batch):
                    try:
                        result = results[i] if isinstance(results, list) else {}
                        phones = result.get("phoneNumbers", [])
                        emails = result.get("emails", [])
                        enriched_results[r.case_number] = {
                            "phone1": phones[0].get("phoneNumber", "") if len(phones) > 0 else "",
                            "phone2": phones[1].get("phoneNumber", "") if len(phones) > 1 else "",
                            "email": emails[0] if emails else "",
                        }
                    except (IndexError, KeyError, AttributeError):
                        enriched_results[r.case_number] = {
                            "phone1": "",
                            "phone2": "",
                            "email": "",
                        }
            except Exception as exc:
                print(f"[skip_trace] Batch error: {exc}")
                for r in batch:
                    enriched_results[r.case_number] = {
                        "phone1": "",
                        "phone2": "",
                        "email": "",
                    }

            if batch_start + _BATCH_SIZE < len(eligible):
                await asyncio.sleep(1)

    # 7. Build output list
    output_map: dict[str, dict] = {}

    for r in eligible:
        contact = enriched_results.get(r.case_number, {})
        output_map[r.case_number] = record_to_dict(
            r,
            enriched=bool(contact.get("phone1") or contact.get("email")),
            phone1=contact.get("phone1", ""),
            phone2=contact.get("phone2", ""),
            email=contact.get("email", ""),
        )

    for r in ineligible:
        output_map[r.case_number] = record_to_dict(r, enriched=False)

    for r in dropped_eligible:
        output_map[r.case_number] = record_to_dict(r, enriched=False)

    # Preserve original order (sort by sale_date desc)
    all_records = eligible + dropped_eligible + ineligible
    all_records.sort(key=lambda r: r.sale_date, reverse=True)
    leads = [output_map[r.case_number] for r in all_records if r.case_number in output_map]

    # 8. Calculate actual cost
    successfully_enriched = sum(
        1 for case, contact in enriched_results.items()
        if contact.get("phone1") or contact.get("email")
    )
    actual_cost = successfully_enriched * cost_per

    # 9. Return
    return leads, len(eligible), actual_cost
