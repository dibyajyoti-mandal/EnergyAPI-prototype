# main.py
import time
import logging
from datetime import datetime, timedelta
from db import DatabaseManager
from api_client import MockEntsoeClient
from sync_engine import EntsoeSyncEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger("ML_Orchestrator")

def mock_training_job(engine: EntsoeSyncEngine, region_id: str):
    logger.info(f"\n🚀 [CRON TRIGGER] Waking up to train ML model for {region_id}...")
    
    # Phase 1: Call the Gatekeeper
    is_data_ready = engine.sync_for_model_training(region_id)
    
    # Phase 2: React to Gatekeeper's decision
    if not is_data_ready:
        logger.error("🛑 Aborting model training. Upstream data is unavailable.")
        return
        
    logger.info("🧠 Loading features from local Database...")
    time.sleep(1) # Simulate feature engineering time
    logger.info("🧠 Model trained successfully. Pushing to registry.\n")


if __name__ == "__main__":
    # Initialize infrastructure
    db = DatabaseManager("mock_grid_data.db")
    api = MockEntsoeClient()
    sync_engine = EntsoeSyncEngine(db, api)
    
    region = "DE_LU" # Germany/Luxembourg bidding zone
    
    # --- SCENARIO A: First Run (Data is infinitely Stale) ---
    logger.info("=== SCENARIO A: Initial Setup ===")
    mock_training_job(sync_engine, region)
    
    
    # --- SCENARIO B: Recent Run (Data is Fresh) ---
    logger.info("=== SCENARIO B: 10 Minutes Later ===")
    # Artificially set DB timestamp to 10 minutes ago
    db.update_metadata(region, datetime.now() - timedelta(minutes=10), "SUCCESS")
    mock_training_job(sync_engine, region)
    
    
    # --- SCENARIO C: Long Delay (Data is Stale) ---
    logger.info("=== SCENARIO C: 4 Hours Later ===")
    # Artificially set DB timestamp to 4 hours ago
    db.update_metadata(region, datetime.now() - timedelta(hours=4), "SUCCESS")
    mock_training_job(sync_engine, region)