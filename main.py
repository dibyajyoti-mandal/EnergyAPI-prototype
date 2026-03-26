"""
main.py — FastAPI Application
==============================
Endpoints:
  GET /historical/{region_id}?since_hours=1
      → Gated by the sync engine (deep vs recent history logic)
      → Returns actual load rows from grid_data table

  GET /forecast/{region_id}
      → Lightning-fast SELECT from forecasts table
      → Inference runs in background after each sync event, not here

On startup:
  • DatabaseManager is initialised
  • SyncEngine background polling thread is launched
  • InferenceWorker daemon thread is launched
"""

import logging
import queue

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query

from db import DatabaseManager
from api_client import MockEntsoeClient
from sync_engine import EntsoeSyncEngine
from inference_worker import InferenceWorker

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-20s %(message)s",
    datefmt="%H:%M:%S",
)

REGIONS = ["DE_LU", "FR", "NL"]
DB_PATH = "mock_grid_data.db"

# ── Shared infrastructure (populated at startup) ───────────────────────────────
db:          DatabaseManager    = None   # type: ignore[assignment]
sync_engine: EntsoeSyncEngine   = None   # type: ignore[assignment]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start background threads before the server accepts requests."""
    global db, sync_engine

    event_queue = queue.Queue()

    db          = DatabaseManager(DB_PATH)
    api         = MockEntsoeClient()
    sync_engine = EntsoeSyncEngine(db, api, event_queue, regions=REGIONS)
    worker      = InferenceWorker(event_queue, db)

    sync_engine.start_background_sync()
    worker.start()

    yield   # ← server runs here

    # Graceful shutdown (daemon threads die automatically)


app = FastAPI(
    title="EnergyAPI — Real-Time Sync Engine",
    description=(
        "Prototype API that demonstrates the event-driven sync + forecast pipeline. "
        "Historical queries gate on data freshness; forecast queries are instant SELECTs."
    ),
    version="0.2.0",
    lifespan=lifespan,
)


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {
        "message": "EnergyAPI Sync Engine prototype",
        "endpoints": {
            "historical": "/historical/{region_id}?since_hours=1",
            "forecast":   "/forecast/{region_id}",
            "docs":       "/docs",
        },
    }


@app.get("/historical/{region_id}")
def get_historical(
    region_id: str,
    since_hours: float = Query(
        default=1.0,
        gt=0,
        description=(
            "How many hours back to query. "
            f"Queries > {2} h are treated as deep history (no sync triggered). "
            "Queries ≤ 2 h trigger a freshness check and sync if stale."
        ),
    ),
):
    """
    Serve historical load data for a region.

    - **Deep history** (since_hours > 2): served straight from DB.
    - **Recent history** (since_hours ≤ 2): staleness check; syncs from
      ENTSO-E if data is older than 15 minutes before returning.
    """
    if region_id.upper() not in [r.upper() for r in REGIONS]:
        raise HTTPException(status_code=404, detail=f"Region '{region_id}' not found. "
                            f"Available: {REGIONS}")

    result = sync_engine.handle_historical_query(region_id.upper(), since_hours)
    return {
        "region_id":      result["region_id"],
        "query_path":     result["path"],          # "deep_history" | "recent_history"
        "sync_triggered": result["sync_triggered"],
        "record_count":   len(result["rows"]),
        "data":           result["rows"],
    }


@app.get("/forecast/{region_id}")
def get_forecast(region_id: str):
    """
    Return the latest cached forecast for a region.

    Forecasts are generated asynchronously by the InferenceWorker after
    each successful data sync — **no inference happens on this request**.
    If no forecast exists yet, a 202 Accepted is returned to signal that
    the system is still warming up.
    """
    if region_id.upper() not in [r.upper() for r in REGIONS]:
        raise HTTPException(status_code=404, detail=f"Region '{region_id}' not found. "
                            f"Available: {REGIONS}")

    forecast = sync_engine.get_forecast(region_id.upper())
    if forecast is None:
        raise HTTPException(
            status_code=202,
            detail=(
                f"No forecast available yet for '{region_id}'. "
                "A sync must complete first to trigger inference. "
                "Try GET /historical/{region_id}?since_hours=1 to kick off a sync."
            ),
        )

    return {
        "region_id":           forecast["region_id"],
        "forecast_horizon_hrs": forecast["forecast_horizon_hrs"],
        "forecast_mw":         forecast["forecast_mw"],
        "generated_at":        forecast["generated_at"],
        "note": (
            "This forecast was generated by the InferenceWorker after the last "
            "successful ENTSO-E data sync — served directly from the forecasts table."
        ),
    }