from __future__ import annotations

from .base import BaseScraper
from .orange_county_fl import OrangeCountyFLScraper
from .lee_county_fl import LeeCountyFLScraper
from .marion_county_fl import MarionCountyFLScraper

REGISTRY: dict[str, type[BaseScraper]] = {
    "orange-county-fl": OrangeCountyFLScraper,
    "lee-county-fl": LeeCountyFLScraper,
    "marion-county-fl": MarionCountyFLScraper,
}


def get_scraper(slug: str) -> BaseScraper:
    if slug not in REGISTRY:
        supported = ", ".join(REGISTRY.keys())
        raise ValueError(
            f"Unknown county slug {slug!r}. Supported slugs: {supported}"
        )
    return REGISTRY[slug]()
