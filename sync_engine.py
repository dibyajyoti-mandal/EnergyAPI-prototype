"""
sync_engine.py — Event-Driven Data Sync Engine
================================================
Responsibilities:
  1. Handle historical data queries:
       - Deep history  (> DEEP_HISTORY_THRESHOLD_HRS)  → serve straight from DB
       - Recent history (≤ DEEP_HISTORY_THRESHOLD_HRS) → check freshness, sync if stale
  2. Periodic background sync → fire SyncEvent on success
  3. Thread-safe sync (one region at a time via per-region locks)

The SyncEvent queue is consumed by InferenceWorker (inference_worker.py).
"""

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

logger = logging.getLogger("SyncEngine")

# ── Tunable thresholds ─────────────────────────────────────────────────────────
STALE_THRESHOLD_MINS: int = 15          # recent data older than this → trigger sync
DEEP_HISTORY_THRESHOLD_HRS: int = 2    # queries older than this → skip sync entirely
POLL_INTERVAL_SECS: int = 30           # background polling cadence


# ── Data structures ────────────────────────────────────────────────────────────

class FreshnessLevel(Enum):
    FRESH      = "fresh"        # 0–STALE_THRESHOLD_MINS
    STALE      = "stale"        # > STALE_THRESHOLD_MINS


@dataclass
class SyncEvent:
    """Fired after a successful sync; consumed by the InferenceWorker."""
    region_id:    str
    records_ingested: int
    synced_at:    datetime = field(default_factory=datetime.now)


# ── Engine ─────────────────────────────────────────────────────────────────────

class EntsoeSyncEngine:
    """
    Event-driven sync engine for ENTSO-E (or any grid) data.

    Parameters
    ----------
    db_manager  : DatabaseManager instance
    api_client  : client with .fetch_recent_data(region_id) → dict
    event_queue : shared queue.Queue consumed by InferenceWorker
    regions     : list of region IDs to watch in background polling
    """

    def __init__(self, db_manager, api_client, event_queue: queue.Queue,
                 regions: list[str] | None = None):
        self.db     = db_manager
        self.api    = api_client
        self.events = event_queue
        self.regions = regions or ["DE_LU", "FR", "NL"]

        # Per-region locks so two threads never sync the same region simultaneously
        self._region_locks: dict[str, threading.Lock] = {
            r: threading.Lock() for r in self.regions
        }

    # ── Freshness helpers ──────────────────────────────────────────────────────

    def _age_minutes(self, region_id: str) -> float:
        last = self.db.get_last_update_time(region_id)
        if last is None:
            return float("inf")
        return (datetime.now() - last).total_seconds() / 60.0

    def _evaluate_freshness(self, region_id: str) -> tuple[FreshnessLevel, float]:
        age = self._age_minutes(region_id)
        level = FreshnessLevel.FRESH if age <= STALE_THRESHOLD_MINS else FreshnessLevel.STALE
        return level, age

    # ── Core sync ──────────────────────────────────────────────────────────────

    def _perform_sync_and_fire_event(self, region_id: str) -> bool:
        """
        Pull fresh data from the API, persist it, update metadata, and fire a
        SyncEvent on the shared queue.  Thread-safe per region.
        """
        lock = self._region_locks.get(region_id) or threading.Lock()
        with lock:
            try:
                logger.info(f"[SyncEngine] Pulling fresh data for {region_id} from ENTSO-E...")
                api_response = self.api.fetch_recent_data(region_id)

                # Build synthetic records from the API response
                # (real impl would parse actual API payload here)
                n = api_response["new_records"]
                latest_mw = api_response["latest_load_mw"]
                now = datetime.now()
                records = [
                    ((now - timedelta(minutes=(n - i) * 15)).isoformat(),
                     round(latest_mw + (i * 10), 2))
                    for i in range(n)
                ]

                self.db.write_grid_data(region_id, records)
                self.db.update_metadata(region_id, now, "SUCCESS")

                logger.info(
                    f"[SyncEngine] Sync OK — ingested {n} records for {region_id}. "
                    f"Latest load: {latest_mw:.0f} MW"
                )

                # Fire event — InferenceWorker is listening
                event = SyncEvent(region_id=region_id, records_ingested=n, synced_at=now)
                self.events.put(event)
                logger.info(
                    f"[SyncEngine] SyncEvent fired for {region_id} "
                    f"→ InferenceWorker will generate forecast"
                )
                return True

            except Exception as exc:
                logger.error(f"[SyncEngine] Sync FAILED for {region_id}: {exc}")
                self.db.update_metadata(region_id, datetime.now(), "FAILED")
                return False

    # ── Public API — historical query path ────────────────────────────────────

    def handle_historical_query(self, region_id: str, since_hours: float) -> dict:
        """
        Gate-keep a historical data request.

        Deep history  (since_hours > DEEP_HISTORY_THRESHOLD_HRS):
            Data is already in the DB — return immediately, no sync.
        Recent history (since_hours ≤ DEEP_HISTORY_THRESHOLD_HRS):
            Check freshness; sync if stale, then return DB rows.

        Returns a dict with keys: region_id, path, rows, sync_triggered.
        """
        since_dt = datetime.now() - timedelta(hours=since_hours)

        # ── Deep history: just read from DB ───────────────────────────
        if since_hours > DEEP_HISTORY_THRESHOLD_HRS:
            logger.info(
                f"[SyncEngine] DEEP-HISTORY query for {region_id} "
                f"({since_hours:.1f} h ago) — serving from DB, no sync needed."
            )
            rows = self.db.get_historical_data(region_id, since_dt)
            return {
                "region_id":      region_id,
                "path":           "deep_history",
                "sync_triggered": False,
                "rows":           rows,
            }

        # ── Recent history: check freshness ───────────────────────────
        freshness, age = self._evaluate_freshness(region_id)
        logger.info(
            f"[SyncEngine] RECENT-HISTORY query for {region_id} "
            f"({since_hours:.1f} h ago). Data age: {age:.1f} min — {freshness.value.upper()}"
        )

        sync_triggered = False
        if freshness == FreshnessLevel.STALE:
            logger.warning(
                f"[SyncEngine] Data is STALE ({age:.1f} min). Triggering sync before serving..."
            )
            self._perform_sync_and_fire_event(region_id)
            sync_triggered = True
        else:
            logger.info(
                f"[SyncEngine] Data is FRESH ({age:.1f} min). Skipping sync."
            )

        rows = self.db.get_historical_data(region_id, since_dt)
        return {
            "region_id":      region_id,
            "path":           "recent_history",
            "sync_triggered": sync_triggered,
            "rows":           rows,
        }

    # ── Public API — forecast query path ──────────────────────────────────────

    def get_forecast(self, region_id: str) -> dict | None:
        """Lightning-fast SELECT from the forecasts table — no inference on request."""
        forecast = self.db.get_latest_forecast(region_id)
        if forecast:
            logger.info(
                f"[SyncEngine] Returning cached forecast for {region_id}: "
                f"{forecast['forecast_mw']:.0f} MW "
                f"(generated at {forecast['generated_at']})"
            )
        else:
            logger.warning(
                f"[SyncEngine] No forecast available yet for {region_id}. "
                "Trigger a sync first."
            )
        return forecast

    # ── Background polling thread ──────────────────────────────────────────────

    def _background_poll(self):
        logger.info(
            f"[SyncEngine] Background poll started — "
            f"interval={POLL_INTERVAL_SECS}s, regions={self.regions}"
        )
        while True:
            for region_id in self.regions:
                freshness, age = self._evaluate_freshness(region_id)
                if freshness == FreshnessLevel.STALE:
                    logger.info(
                        f"[SyncEngine][BG] {region_id} is STALE ({age:.1f} min) — syncing..."
                    )
                    self._perform_sync_and_fire_event(region_id)
                else:
                    logger.debug(
                        f"[SyncEngine][BG] {region_id} is FRESH ({age:.1f} min) — skipping."
                    )
            time.sleep(POLL_INTERVAL_SECS)

    def start_background_sync(self):
        """Launch the background polling daemon thread."""
        t = threading.Thread(target=self._background_poll, daemon=True, name="SyncEngine-BG")
        t.start()
        logger.info("[SyncEngine] Background sync thread started.")
        return t