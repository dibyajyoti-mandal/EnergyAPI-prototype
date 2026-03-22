# api_client.py
import time
import random
import logging

logger = logging.getLogger("MockAPI")

class MockEntsoeClient:
    def fetch_recent_data(self, region_id: str) -> dict:
        logger.info(f"🌐 Requesting fresh data for {region_id} from ENTSO-E...")
        
        time.sleep(2)
        
        if random.random() < 0.05:
            raise ConnectionError(f"ENTSO-E API 500 Internal Server Error for {region_id}")
            
        return {
            "region": region_id,
            "new_records": random.randint(4, 24),
            "latest_load_mw": random.uniform(45000.0, 55000.0)
        }
