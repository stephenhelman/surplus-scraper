from datetime import datetime, timezone, timedelta

import pytest

from scrapers.registry import REGISTRY, get_scraper
from scrapers.base import BaseScraper
from scrapers.orange_county_fl import OrangeCountyFLScraper
from scrapers.lee_county_fl import LeeCountyFLScraper
from scrapers.marion_county_fl import MarionCountyFLScraper


def test_get_scraper_returns_correct_class_orange():
    scraper = get_scraper("orange-county-fl")
    assert isinstance(scraper, OrangeCountyFLScraper)


def test_get_scraper_returns_correct_class_lee():
    scraper = get_scraper("lee-county-fl")
    assert isinstance(scraper, LeeCountyFLScraper)


def test_get_scraper_returns_correct_class_marion():
    scraper = get_scraper("marion-county-fl")
    assert isinstance(scraper, MarionCountyFLScraper)


def test_get_scraper_raises_value_error_for_unknown():
    with pytest.raises(ValueError, match="unknown-county"):
        get_scraper("unknown-county")


def test_all_registered_scrapers_have_county_slug():
    for slug, cls in REGISTRY.items():
        assert hasattr(cls, "county_slug"), f"{cls.__name__} missing county_slug"
        assert cls.county_slug, f"{cls.__name__}.county_slug is empty"


def test_all_registered_scrapers_have_county_label():
    for slug, cls in REGISTRY.items():
        assert hasattr(cls, "county_label"), f"{cls.__name__} missing county_label"
        assert cls.county_label, f"{cls.__name__}.county_label is empty"


def test_is_within_window_true_for_recent_date():
    scraper = get_scraper("orange-county-fl")
    recent = datetime.now(timezone.utc) - timedelta(days=30)
    assert scraper.is_within_window(recent) is True


def test_is_within_window_false_for_old_date():
    scraper = get_scraper("orange-county-fl")
    old = datetime.now(timezone.utc) - timedelta(days=400)
    assert scraper.is_within_window(old) is False


def test_is_within_window_handles_naive_datetime():
    scraper = get_scraper("orange-county-fl")
    recent_naive = datetime.utcnow() - timedelta(days=30)
    # Should not raise, and should return True
    assert scraper.is_within_window(recent_naive) is True
