from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from scrapers.base import SurplusRecord


def _make_record(case: str, amount: float) -> SurplusRecord:
    return SurplusRecord(
        owner_name="John Doe",
        property_address="123 Main St",
        case_number=case,
        surplus_amount=amount,
        sale_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
        county="Orange County, FL",
    )


def _mock_api_response(n: int):
    """Build a fake BatchData response with phone/email for n records."""
    results = []
    for i in range(n):
        results.append(
            {
                "phoneNumbers": [
                    {"phoneNumber": f"555-000-{i:04d}"},
                    {"phoneNumber": f"555-111-{i:04d}"},
                ],
                "emails": [f"owner{i}@example.com"],
            }
        )
    return {"results": results}


@pytest.mark.asyncio
async def test_below_threshold_no_api_call(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "test-key")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "100")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    records = [_make_record("C-001", 5000.0), _make_record("C-002", 8000.0)]

    mock_response = MagicMock()
    mock_response.json.return_value = {"results": []}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: mock_client)

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    leads, eligible_count, actual_cost = await st.enrich_records(records)

    mock_client.post.assert_not_called()
    assert eligible_count == 0
    assert all(lead["phone1"] == "" for lead in leads)
    assert all(lead["phone2"] == "" for lead in leads)
    assert all(lead["email"] == "" for lead in leads)


@pytest.mark.asyncio
async def test_above_threshold_sent_to_api(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "test-key")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "100")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    records = [_make_record("C-001", 20000.0), _make_record("C-002", 25000.0)]

    mock_response = MagicMock()
    mock_response.json.return_value = _mock_api_response(2)
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: mock_client)

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    leads, eligible_count, actual_cost = await st.enrich_records(records)

    mock_client.post.assert_called_once()
    assert eligible_count == 2
    assert any(lead["phone1"] != "" for lead in leads)


@pytest.mark.asyncio
async def test_cap_enriches_top_n_by_surplus(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "test-key")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "2")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    records = [
        _make_record("C-LOW", 16000.0),
        _make_record("C-HIGH", 50000.0),
        _make_record("C-MID", 30000.0),
    ]

    captured_payloads = []

    async def fake_post(url, json=None, headers=None):
        if json:
            captured_payloads.append(json)
        mock_response = MagicMock()
        mock_response.json.return_value = _mock_api_response(len(json.get("requests", [])))
        mock_response.raise_for_status = MagicMock()
        return mock_response

    mock_client = AsyncMock()
    mock_client.post = fake_post
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: mock_client)

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    leads, eligible_count, actual_cost = await st.enrich_records(records)

    assert eligible_count == 2
    # Only 2 sent to API — should be the top 2 by amount (C-HIGH and C-MID)
    assert len(captured_payloads) == 1
    assert len(captured_payloads[0]["requests"]) == 2

    # The dropped record (C-LOW) should still appear in output but unenriched
    low_lead = next(l for l in leads if l["caseNumber"] == "C-LOW")
    assert low_lead["phone1"] == ""


@pytest.mark.asyncio
async def test_no_api_key_returns_blank_contacts_zero_cost(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "100")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    records = [_make_record("C-001", 20000.0), _make_record("C-002", 8000.0)]

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    leads, eligible_count, actual_cost = await st.enrich_records(records)

    assert actual_cost == 0.0
    assert eligible_count == 0
    assert len(leads) == 2
    assert all(lead["phone1"] == "" for lead in leads)
    assert all(lead["email"] == "" for lead in leads)


@pytest.mark.asyncio
async def test_actual_cost_equals_successfully_enriched_times_cost(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "test-key")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "100")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    records = [
        _make_record("C-001", 20000.0),
        _make_record("C-002", 25000.0),
        _make_record("C-003", 30000.0),
    ]

    # Only 2 of 3 will have phone data (simulate partial success)
    partial_results = [
        {"phoneNumbers": [{"phoneNumber": "555-0001"}], "emails": ["a@b.com"]},
        {"phoneNumbers": [], "emails": []},
        {"phoneNumbers": [{"phoneNumber": "555-0003"}], "emails": ["c@d.com"]},
    ]

    mock_response = MagicMock()
    mock_response.json.return_value = {"results": partial_results}
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_response)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: mock_client)

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    leads, eligible_count, actual_cost = await st.enrich_records(records)

    assert eligible_count == 3
    # 2 records with phone data → 2 * 0.35 = 0.70
    assert actual_cost == pytest.approx(2 * 0.35)


@pytest.mark.asyncio
async def test_no_api_key_returns_all_records_not_empty_list(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "100")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    records = [
        _make_record("C-001", 20000.0),
        _make_record("C-002", 8000.0),
        _make_record("C-003", 50000.0),
        _make_record("C-004", 3000.0),
        _make_record("C-005", 25000.0),
    ]

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    import httpx
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kw: mock_client)

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    leads, eligible_count, actual_cost = await st.enrich_records(records)

    assert len(leads) == 5
    assert all(lead["phone1"] == "" for lead in leads)
    assert all(lead["phone2"] == "" for lead in leads)
    assert all(lead["email"] == "" for lead in leads)
    assert actual_cost == 0.0
    mock_client.post.assert_not_called()


@pytest.mark.asyncio
async def test_return_type_always_tuple_list_int_float(monkeypatch):
    monkeypatch.setenv("BATCHDATA_API_KEY", "")
    monkeypatch.setenv("SKIP_TRACE_MIN_SURPLUS", "15000")
    monkeypatch.setenv("MAX_ENRICH_PER_RUN", "100")
    monkeypatch.setenv("COST_PER_RECORD", "0.35")

    import importlib
    import enrichment.skip_trace as st
    importlib.reload(st)

    result = await st.enrich_records([])

    assert isinstance(result, tuple)
    assert len(result) == 3
    leads, eligible_count, actual_cost = result
    assert isinstance(leads, list)
    assert isinstance(eligible_count, int)
    assert isinstance(actual_cost, float)
