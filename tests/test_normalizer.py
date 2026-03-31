from datetime import datetime, timezone

import pytest

from enrichment.normalizer import normalize
from scrapers.base import SurplusRecord


def _make_record(**kwargs) -> SurplusRecord:
    defaults = dict(
        owner_name="john doe",
        property_address="123 main st",
        case_number="2024-001",
        surplus_amount=20000.0,
        sale_date=datetime(2024, 6, 1, tzinfo=timezone.utc),
        county="Orange County, FL",
    )
    defaults.update(kwargs)
    return SurplusRecord(**defaults)


def test_owner_name_title_cased_and_stripped():
    records = [_make_record(owner_name="  john doe  ")]
    result = normalize(records)
    assert result[0].owner_name == "John Doe"


def test_property_address_title_cased_and_stripped():
    records = [_make_record(property_address="  123 main st  ")]
    result = normalize(records)
    assert result[0].property_address == "123 Main St"


def test_duplicate_case_numbers_removed_first_kept():
    records = [
        _make_record(case_number="2024-001", owner_name="Alice Smith"),
        _make_record(case_number="2024-001", owner_name="Bob Jones"),
        _make_record(case_number="2024-002", owner_name="Carol White"),
    ]
    result = normalize(records)
    assert len(result) == 2
    assert result[0].owner_name == "Alice Smith"
    assert result[1].owner_name == "Carol White"


def test_empty_list_returns_empty_list():
    result = normalize([])
    assert result == []


def test_case_number_stripped():
    records = [_make_record(case_number="  2024-001  ")]
    result = normalize(records)
    assert result[0].case_number == "2024-001"
