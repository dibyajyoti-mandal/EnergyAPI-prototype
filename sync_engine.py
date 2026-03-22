# sync_engine.py
import logging
import threading
from datetime import datetime, timedelta
from enum import Enum
# from db import DatabaseManager
# from api_client import MockEntsoeClient

logger = logging.getLogger("SyncEngine")

class FreshnessLevel(Enum):
    FRESH = "fresh"           # 0-15 mins
    ACCEPTABLE = "acceptable" # 15-60 mins
    STALE = "stale"           # >60 mins

class EntsoeSyncEngine:
    def __init__(self, db_manager, api_client):
        self.db = db_manager
        self.api = api_client
        self._sync_lock = threading.Lock()
        
    def _evaluate_freshness(self, region_id: str):
        last_update = self.db.get_last_update_time(region_id)
        if not last_update:
            return FreshnessLevel.STALE, float('inf')
            
        age_minutes = (datetime.now() - last_update).total_seconds() / 60.0
        
        if age_minutes <= 15:
            return FreshnessLevel.FRESH, age_minutes
        elif age_minutes <= 60:
            return FreshnessLevel.ACCEPTABLE, age_minutes
        else:
            return FreshnessLevel.STALE, age_minutes

    def _perform_immediate_sync(self, region_id: str):
        """Thread-safe API call execution."""
        with self._sync_lock:
            try:
                data = self.api.fetch_recent_data(region_id)
                # In a real app, you would parse 'data' and insert into 'grid_data' table here
                self.db.update_metadata(region_id, datetime.now(), "SUCCESS")
                logger.info(f"Sync successful. Ingested {data['new_records']} new records.")
                return True
            except Exception as e:
                logger.error(f"Sync failed: {str(e)}")
                self.db.update_metadata(region_id, datetime.now(), "FAILED")
                return False

    def sync_for_model_training(self, region_id: str) -> bool:
        """The pre-training trigger called by the ML Orchestrator."""
        freshness, age = self._evaluate_freshness(region_id)
        
        if freshness == FreshnessLevel.FRESH:
            logger.info(f"Gatekeeper: Data is FRESH ({age:.1f} mins old). Safe to train instantly.")
            return True
            
        elif freshness == FreshnessLevel.ACCEPTABLE:
            logger.info(f"Gatekeeper: Data is ACCEPTABLE ({age:.1f} mins old). Safe to train. (Background sync recommended later).")
            return True
            
        else:
            logger.warning(f"Gatekeeper: Data is STALE ({age:.1f} mins old). Halting training to force sync!")
            success = self._perform_immediate_sync(region_id)
            return success