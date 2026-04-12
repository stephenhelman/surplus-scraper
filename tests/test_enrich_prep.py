from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, patch

from enrichment.enrich_prep import run_enrich_prep, enrich_marion_county


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_lead(id_: str, parcel: str, sale_date: str) -> dict:
    return {"id": id_, "caseNumber": parcel, "saleDate": sale_date}


def _mock_lookup_result(found: bool, owner: str = "SMITH JOHN", address: str = "123 Oak St, Ocala, FL 34471") -> dict:
    if found:
        return {"owner_name": owner, "mailing_address": address, "found": True}
    return {"owner_name": "", "mailing_address": "", "found": False}


# ---------------------------------------------------------------------------
# run_enrich_prep: unknown county raises ValueError
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_unknown_county_raises():
    with pytest.raises(ValueError, match="No enrich-prep handler for county: fake-county-xx"):
        await run_enrich_prep("fake-county-xx", [])


# ---------------------------------------------------------------------------
# run_enrich_prep: correct shape returned for marion-county-fl
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_enrich_prep_returns_correct_shape():
    leads = [
        _make_lead("lead-1", "12345-000-00", "2024-03-15"),
        _make_lead("lead-2", "99999-000-00", "2023-07-01"),
    ]

    # lead-1 found, lead-2 not found
    def fake_lookup_shape(client, parcel, lookup_year):
        if parcel == "12345-000-00":
            return _mock_lookup_result(True, owner="SMITH JOHN", address="123 Oak St, Ocala, FL 34471")
        return _mock_lookup_result(False)

    with patch("enrichment.enrich_prep.lookup_parcel", new=AsyncMock(side_effect=fake_lookup_shape)), \
         patch("asyncio.sleep", new=AsyncMock()):
        response = await run_enrich_prep("marion-county-fl", leads)

    assert "leads" in response
    assert "totalRequested" in response
    assert "totalFound" in response
    assert "totalNotFound" in response
    assert response["totalRequested"] == 2
    assert response["totalFound"] == 1
    assert response["totalNotFound"] == 1
    assert len(response["leads"]) == 2


# ---------------------------------------------------------------------------
# totalFound / totalNotFound counts are correct
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_total_found_not_found_counts():
    # 5 leads: i=0,2,4 → found (3 found); i=1,3 → not found (2 not found)
    leads = [_make_lead(f"id-{i}", f"parcel-{i}", "2025-01-10") for i in range(5)]

    call_counter = {"n": 0}

    async def fake_lookup(client, parcel, lookup_year):
        idx = int(parcel.split("-")[1])
        return _mock_lookup_result(idx % 2 == 0)

    with patch("enrichment.enrich_prep.lookup_parcel", new=fake_lookup), \
         patch("asyncio.sleep", new=AsyncMock()):
        response = await run_enrich_prep("marion-county-fl", leads)

    assert response["totalFound"] == 3
    assert response["totalNotFound"] == 2


# ---------------------------------------------------------------------------
# All submitted leads are present in the response (none silently dropped)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_all_leads_present_in_response():
    leads = [_make_lead(f"id-{i}", f"p-{i}", "2024-06-01") for i in range(7)]

    async def fake_lookup(client, parcel, lookup_year):
        return _mock_lookup_result(False)

    with patch("enrichment.enrich_prep.lookup_parcel", new=fake_lookup), \
         patch("asyncio.sleep", new=AsyncMock()):
        response = await run_enrich_prep("marion-county-fl", leads)

    assert len(response["leads"]) == 7
    returned_ids = {r["id"] for r in response["leads"]}
    submitted_ids = {l["id"] for l in leads}
    assert returned_ids == submitted_ids


# ---------------------------------------------------------------------------
# enrich_marion_county: lookup_year = saleDate.year - 1
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_lookup_year_is_sale_date_year_minus_one():
    leads = [
        _make_lead("lead-a", "111-222-333", "2025-03-15"),
        _make_lead("lead-b", "444-555-666", "2023-11-01"),
    ]

    captured_calls: list[tuple] = []

    async def fake_lookup(client, parcel, lookup_year):
        captured_calls.append((parcel, lookup_year))
        return _mock_lookup_result(True)

    with patch("enrichment.enrich_prep.lookup_parcel", new=fake_lookup), \
         patch("asyncio.sleep", new=AsyncMock()):
        await enrich_marion_county(leads)

    assert captured_calls[0] == ("111-222-333", 2024)  # 2025 - 1
    assert captured_calls[1] == ("444-555-666", 2022)  # 2023 - 1


# ---------------------------------------------------------------------------
# enrich_marion_county: found=False returns empty strings, does NOT raise
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_not_found_lead_returns_empty_fields_no_raise():
    leads = [_make_lead("lead-x", "000-000-000", "2024-06-15")]

    async def fake_lookup(client, parcel, lookup_year):
        return _mock_lookup_result(False)

    with patch("enrichment.enrich_prep.lookup_parcel", new=fake_lookup), \
         patch("asyncio.sleep", new=AsyncMock()):
        results = await enrich_marion_county(leads)

    assert len(results) == 1
    r = results[0]
    assert r["id"] == "lead-x"
    assert r["found"] is False
    assert r["ownerName"] == ""
    assert r["mailingAddress"] == ""
    assert r["mailingCity"] == ""
    assert r["mailingState"] == "FL"
    assert r["mailingZip"] == ""


# ---------------------------------------------------------------------------
# enrich_marion_county: found=True populates fields correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_found_lead_populates_fields():
    leads = [_make_lead("lead-y", "123-456-789", "2024-09-10")]

    async def fake_lookup(client, parcel, lookup_year):
        return {
            "owner_name": "JONES MARY",
            "mailing_address": "456 Elm Ave, Gainesville, FL 32601",
            "found": True,
        }

    with patch("enrichment.enrich_prep.lookup_parcel", new=fake_lookup), \
         patch("asyncio.sleep", new=AsyncMock()):
        results = await enrich_marion_county(leads)

    r = results[0]
    assert r["found"] is True
    assert r["ownerName"] == "JONES MARY"
    assert r["mailingAddress"] == "456 Elm Ave, Gainesville, FL 32601"
    assert r["mailingCity"] == "Gainesville"
    assert r["mailingState"] == "FL"
    assert r["mailingZip"] == "32601"
