"""
demo.py — Standalone Scenario Runner
======================================
Exercises the full sync → infer → serve pipeline without an HTTP server.
Run after init_db.py to start with a clean, stale database.

  python init_db.py   # seed DB
  python demo.py      # watch the engine in action

Scenarios
---------
A  Deep-history query   → data is months old, no sync triggered
B  Recent query, FRESH  → data was just updated, no sync triggered
C  Recent query, STALE  → data is 2 h old, sync fires → event → inference
D  Forecast query       → instant SELECT after scenario C wrote a forecast
"""

import logging
import queue
import time
from datetime import datetime, timedelta

from db import DatabaseManager
from api_client import MockEntsoeClient
from sync_engine import EntsoeSyncEngine
from inference_worker import InferenceWorker

# ── Pretty logging ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Demo")

REGION = "DE_LU"
DB_PATH = "mock_grid_data.db"

DIVIDER = "\n" + "─" * 70


def print_header(label: str):
    print(DIVIDER)
    print(f"  {label}")
    print("─" * 70)


def main():
    # ── Bootstrap infrastructure ──────────────────────────────────────────
    event_queue  = queue.Queue()
    db           = DatabaseManager(DB_PATH)
    api          = MockEntsoeClient()
    sync_engine  = EntsoeSyncEngine(db, api, event_queue, regions=[REGION])
    worker       = InferenceWorker(event_queue, db)

    # Start inference worker daemon (sync engine BG thread is optional in demo)
    worker.start()

    # ── SCENARIO A: Deep-history query ────────────────────────────────────
    print_header("SCENARIO A — Deep-history query (6 hours ago)")
    logger.info("User requests data from 6 hours ago.")
    result = sync_engine.handle_historical_query(REGION, since_hours=6.0)
    logger.info(
        f"Result: path={result['path']}, "
        f"sync_triggered={result['sync_triggered']}, "
        f"rows={len(result['rows'])}"
    )
    assert result["path"] == "deep_history"
    assert result["sync_triggered"] is False
    logger.info("✅ SCENARIO A PASSED\n")

    # ── SCENARIO B: Recent query — data is FRESH ──────────────────────────
    print_header("SCENARIO B — Recent query, data marked FRESH (5 minutes old)")
    # logger.info("Artificially setting last_updated to 5 minutes ago...")
    db.update_metadata(REGION, datetime.now() - timedelta(minutes=5), "SUCCESS")

    result = sync_engine.handle_historical_query(REGION, since_hours=1.0)
    logger.info(
        f"Result: path={result['path']}, "
        f"sync_triggered={result['sync_triggered']}, "
        f"rows={len(result['rows'])}"
    )
    assert result["path"] == "recent_history"
    assert result["sync_triggered"] is False
    logger.info("✅ SCENARIO B PASSED\n")

    # ── SCENARIO C: Recent query — data is STALE ─────────────────────────
    print_header("SCENARIO C — Recent query, data STALE (2 hours old)")
    # logger.info("Artificially setting last_updated to 2 hours ago...")
    db.update_metadata(REGION, datetime.now() - timedelta(hours=2), "SUCCESS")

    logger.info("Issuing recent-history query — staleness gatekeeper should trigger sync...")
    result = sync_engine.handle_historical_query(REGION, since_hours=1.0)
    logger.info(
        f"Result: path={result['path']}, "
        f"sync_triggered={result['sync_triggered']}, "
        f"rows={len(result['rows'])}"
    )
    assert result["path"] == "recent_history"
    assert result["sync_triggered"] is True

    # Give the InferenceWorker time to consume the event and write a forecast
    logger.info("Waiting 3 s for InferenceWorker to generate and store forecast...")
    time.sleep(3)
    logger.info("✅ SCENARIO C PASSED\n")

    # ── SCENARIO D: Forecast query ─────────────────────────────────────────
    print_header("SCENARIO D — Forecast query")
    logger.info("User requests forecast — API does a direct DB lookup, no inference.")
    forecast = sync_engine.get_forecast(REGION)

    if forecast:
        logger.info(
            f"Forecast returned: {forecast['forecast_mw']:.0f} MW "
            f"over next {forecast['forecast_horizon_hrs']} h "
            f"(generated at {forecast['generated_at']})"
        )
        logger.info("✅ SCENARIO D PASSED\n")
    else:
        logger.warning("No forecast found — InferenceWorker may not have finished yet.")

    print(DIVIDER)
    print("  All scenarios complete.")
    print("  Run `uvicorn main:app --reload` to exercise the HTTP API.")
    print(DIVIDER + "\n")


if __name__ == "__main__":
    main()
