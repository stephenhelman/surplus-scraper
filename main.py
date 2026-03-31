from __future__ import annotations

import logging
import os

from dotenv import load_dotenv

load_dotenv()

from fastapi import BackgroundTasks, FastAPI, HTTPException, Request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("surplus_scraper")
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import httpx

from scrapers.registry import REGISTRY, get_scraper
from enrichment.normalizer import normalize
from enrichment.skip_trace import (
    COST_PER_RECORD,
    MAX_ENRICH_PER_RUN,
    SKIP_TRACE_MIN_SURPLUS,
    enrich_records,
    skip_trace_enabled,
)

SCRAPER_SECRET = os.getenv("SCRAPER_SECRET", "")
ALLOWED_ORIGIN = os.getenv("ALLOWED_ORIGIN", "*")

if not SCRAPER_SECRET:
    raise RuntimeError(
        "SCRAPER_SECRET env var is not set — "
        "cannot authenticate callbacks"
    )

app = FastAPI(title="Surplus Scraper Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[ALLOWED_ORIGIN] if ALLOWED_ORIGIN != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RunRequest(BaseModel):
    callbackUrl: str


@app.get("/health")
async def health():
    batchdata_key = os.getenv("BATCHDATA_API_KEY", "")
    skip_trace_min = float(os.getenv("SKIP_TRACE_MIN_SURPLUS", "15000"))
    max_enrich = int(os.getenv("MAX_ENRICH_PER_RUN", "100"))
    cost_per = float(os.getenv("COST_PER_RECORD", "0.35"))

    return {
        "status": "ok",
        "supportedCounties": list(REGISTRY.keys()),
        "config": {
            "skipTraceMinSurplus": skip_trace_min,
            "maxEnrichPerRun": max_enrich,
            "costPerRecord": cost_per,
            "skipTraceEnabled": bool(batchdata_key),
        },
    }


@app.post("/run/{county}")
async def run_county(
    county: str,
    body: RunRequest,
    request: Request,
    background_tasks: BackgroundTasks,
):
    secret = request.headers.get("x-internal-secret", "")
    expected = os.getenv("SCRAPER_SECRET", "")
    if not secret or secret != expected:
        raise HTTPException(status_code=403, detail="Forbidden")

    if county not in REGISTRY:
        raise HTTPException(status_code=404, detail=f"County {county!r} not found")

    logger.info("Pipeline started: county=%s callback=%s", county, body.callbackUrl)
    background_tasks.add_task(run_pipeline, county, body.callbackUrl)
    return {"status": "started", "county": county}


def _callback_headers() -> dict:
    return {"x-internal-secret": SCRAPER_SECRET}


async def _post_callback(client: httpx.AsyncClient, callback_url: str, payload: dict) -> None:
    print(f"[callback] Sending to: {callback_url}")
    print(f"[callback] Secret present: {bool(SCRAPER_SECRET)}")
    print(f"[callback] Secret preview: {SCRAPER_SECRET[:6] if SCRAPER_SECRET else 'MISSING'}...")
    response = await client.post(callback_url, json=payload, headers=_callback_headers())
    print(f"[callback] Response status: {response.status_code}")
    if response.status_code != 200:
        print(f"[callback] Response body: {response.text}")


async def run_pipeline(county: str, callback_url: str) -> None:
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            # Phase 1 — scrape + normalize
            logger.info("[%s] Phase 1: scraping", county)
            scraper = get_scraper(county)
            raw = await scraper.fetch()
            logger.info("[%s] Scraped %d raw records", county, len(raw))

            clean = normalize(raw)
            logger.info("[%s] Normalized to %d records after deduplication", county, len(clean))

            if not clean:
                logger.warning("[%s] No records found after scraping", county)
                await _post_callback(client, callback_url, {
                    "status": "error",
                    "error": f"No surplus records found for {county}.",
                })
                return

            # Phase 2 — pre-enrichment callback
            logger.info("[%s] Phase 2: pre-enrichment callback", county)
            min_surplus = float(os.getenv("SKIP_TRACE_MIN_SURPLUS", "15000"))
            max_enrich = int(os.getenv("MAX_ENRICH_PER_RUN", "100"))
            cost_per = float(os.getenv("COST_PER_RECORD", "0.35"))

            eligible_preview = [r for r in clean if r.surplus_amount >= min_surplus]
            capped_count = min(len(eligible_preview), max_enrich)
            est_cost = round(capped_count * cost_per, 2)

            enrichment_on = skip_trace_enabled()
            capped_count_to_report = capped_count if enrichment_on else 0
            est_cost_to_report = est_cost if enrichment_on else 0.0

            logger.info(
                "[%s] skip_trace_enabled=%s eligible=%d capped=%d est_cost=$%.2f",
                county, enrichment_on, len(eligible_preview), capped_count, est_cost,
            )

            await _post_callback(client, callback_url, {
                "status": "enriching",
                "totalRecords": len(clean),
                "eligibleCount": capped_count_to_report,
                "estimatedCost": est_cost_to_report,
                "skipTraceEnabled": enrichment_on,
            })

            # Phase 3 — enrich
            logger.info("[%s] Phase 3: enriching %d records", county, len(clean))
            leads, eligible_count, actual_cost = await enrich_records(clean)
            logger.info(
                "[%s] Enrichment complete: %d leads, actual_cost=$%.2f",
                county, len(leads), actual_cost,
            )

            # Phase 4 — done callback
            logger.info("[%s] Phase 4: sending done callback", county)
            await _post_callback(client, callback_url, {
                "status": "done",
                "totalRecords": len(leads),
                "eligibleCount": eligible_count,
                "actualCost": actual_cost,
                "skipTraceEnabled": enrichment_on,
                "leads": leads,
            })
            logger.info("[%s] Pipeline complete", county)

        except Exception as exc:
            logger.exception("[%s] Pipeline error: %s", county, exc)
            try:
                await _post_callback(client, callback_url, {
                    "status": "error",
                    "error": str(exc),
                })
            except Exception:
                pass
