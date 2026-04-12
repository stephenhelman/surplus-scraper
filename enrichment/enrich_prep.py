from __future__ import annotations

import asyncio
from datetime import datetime

import httpx

from scrapers.marion_pa_lookup import lookup_parcel


def _parse_mailing_parts(mailing_address: str) -> tuple[str, str, str]:
    """
    Attempt to split a mailing address string like:
      "123 Main St, Ocala, FL 34471"
    into (city, state, zip).

    Returns ("", "FL", "") if parsing fails — state is always FL
    for Marion County records.
    """
    if not mailing_address:
        return "", "FL", ""

    parts = [p.strip() for p in mailing_address.split(",")]
    if len(parts) >= 3:
        city = parts[-2].strip()
        state_zip = parts[-1].strip().split()
        state = state_zip[0] if state_zip else "FL"
        zip_code = state_zip[1] if len(state_zip) > 1 else ""
        return city, state, zip_code

    return "", "FL", ""


async def enrich_marion_county(leads: list[dict]) -> list[dict]:
    """
    Resolves parcel numbers for Marion County FL leads using the
    Property Appraiser lookup.

    Each lead dict must contain:
      id: str           — hub's lead ID
      caseNumber: str   — parcel number
      saleDate: str     — ISO date string e.g. "2024-03-15"

    lookup_year = saleDate.year - 1  (year BEFORE the tax sale,
    so we get the former owner, not the auction buyer).

    Returns a list of dicts with keys:
      id, ownerName, mailingAddress, mailingCity, mailingState,
      mailingZip, found
    """
    results: list[dict] = []
    total = len(leads)

    async with httpx.AsyncClient(timeout=20.0) as client:
        for i, lead in enumerate(leads):
            lead_id = lead["id"]
            parcel = lead.get("caseNumber", "")
            sale_date_str = lead.get("saleDate", "")

            try:
                lookup_year = datetime.fromisoformat(sale_date_str).year - 1
            except (ValueError, TypeError):
                lookup_year = datetime.now().year - 1

            if parcel:
                result = await lookup_parcel(client, parcel, lookup_year)
            else:
                result = {"owner_name": "", "mailing_address": "", "found": False}

            found = result["found"]
            owner_name = result.get("owner_name", "")
            mailing_address = result.get("mailing_address", "")

            city, state, zip_code = _parse_mailing_parts(mailing_address)

            print(
                f"[enrich_prep] [{i + 1}/{total}] "
                f"{'✓' if found else '✗'} id={lead_id} "
                f"parcel={parcel} owner={owner_name!r}"
            )

            results.append(
                {
                    "id": lead_id,
                    "ownerName": owner_name,
                    "mailingAddress": mailing_address,
                    "mailingCity": city,
                    "mailingState": state,
                    "mailingZip": zip_code,
                    "found": found,
                }
            )

            if i < total - 1:
                await asyncio.sleep(1.0)

    return results


ENRICH_PREP_REGISTRY: dict[str, object] = {
    "marion-county-fl": enrich_marion_county,
}


async def run_enrich_prep(county: str, leads: list[dict]) -> dict:
    """
    Routes leads to the correct county handler.
    Returns a structured response the hub can parse.
    """
    if county not in ENRICH_PREP_REGISTRY:
        raise ValueError(
            f"No enrich-prep handler for county: {county}. "
            f"Supported: {list(ENRICH_PREP_REGISTRY.keys())}"
        )

    handler = ENRICH_PREP_REGISTRY[county]
    results = await handler(leads)

    total_found = sum(1 for r in results if r["found"])
    total_not_found = len(results) - total_found

    print(
        f"[enrich_prep] {county}: "
        f"{total_found}/{len(results)} resolved. "
        f"{total_not_found} not found."
    )

    return {
        "leads": results,
        "totalRequested": len(results),
        "totalFound": total_found,
        "totalNotFound": total_not_found,
    }
