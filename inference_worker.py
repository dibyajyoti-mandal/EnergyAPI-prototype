"""
inference_worker.py — Background Inference Worker
==================================================
Listens on a shared queue.Queue for SyncEvent messages fired by the
SyncEngine.  On each event it runs a "dummy" inference job (logs + sleep)
and writes the resulting forecast to the forecasts table.

In a real system you would:
  - load your trained model from a registry
  - build a feature tensor from the freshly-ingested grid_data rows
  - call model.predict() to get actual MW forecasts
  - write each horizon's forecast to the DB
"""

import logging
import queue
import random
import threading
import time
from datetime import datetime

logger = logging.getLogger("InferenceWorker")

FORECAST_HORIZON_HRS: int = 24     # how many hours ahead to forecast


class InferenceWorker:
    """
    Daemon worker that turns SyncEvents into stored forecasts.

    Parameters
    ----------
    event_queue : queue.Queue[SyncEvent]   shared with the SyncEngine
    db_manager  : DatabaseManager          for writing forecast rows
    """

    def __init__(self, event_queue: queue.Queue, db_manager):
        self.events = event_queue
        self.db     = db_manager

    # ── Dummy inference ───────────────────────────────────────────────────────

    def _run_dummy_inference(self, region_id: str, records_ingested: int) -> float:
        """
        Simulates a model inference job.
        In reality: load model → build features → predict.
        Here:       sleep for dramatic effect → return random MW value.
        """
        logger.info(
            f"[InferenceWorker] ▶ Starting inference job for {region_id} "
            f"using {records_ingested} freshly-ingested records..."
        )

        # Simulate feature-engineering + model forward-pass time
        time.sleep(1.0)

        # Dummy forecast: random value in a region-plausible range
        base_loads = {"DE_LU": 55000.0, "FR": 50000.0, "NL": 15000.0}
        base = base_loads.get(region_id, 40000.0)
        forecast_mw = base + random.uniform(-3000.0, 3000.0)

        logger.info(
            f"[InferenceWorker] [DUMMY] Model inference complete for {region_id}. "
            f"Predicted load for next {FORECAST_HORIZON_HRS} h: {forecast_mw:.0f} MW"
        )
        return forecast_mw

    # ── Event loop ────────────────────────────────────────────────────────────

    def _listen(self):
        logger.info("[InferenceWorker] Listening for SyncEvents on the queue...")
        while True:
            try:
                # Block until an event arrives; timeout lets us check for shutdown
                event = self.events.get(block=True, timeout=5)
            except queue.Empty:
                continue

            try:
                logger.info(
                    f"[InferenceWorker] Got SyncEvent — region={event.region_id}, "
                    f"records={event.records_ingested}, synced_at={event.synced_at:%H:%M:%S}"
                )

                forecast_mw = self._run_dummy_inference(
                    event.region_id, event.records_ingested
                )

                self.db.write_forecast(
                    region_id=event.region_id,
                    horizon_hrs=FORECAST_HORIZON_HRS,
                    forecast_mw=forecast_mw,
                )
                logger.info(
                    f"[InferenceWorker] Forecast written to DB for {event.region_id} "
                    f"→ {forecast_mw:.0f} MW over next {FORECAST_HORIZON_HRS} h"
                )

            except Exception as exc:
                logger.error(f"[InferenceWorker] Inference job failed: {exc}")
            finally:
                self.events.task_done()

    def start(self) -> threading.Thread:
        """Launch the listener as a daemon thread."""
        t = threading.Thread(target=self._listen, daemon=True, name="InferenceWorker")
        t.start()
        logger.info("[InferenceWorker] Daemon thread started.")
        return t
