# db.py
import sqlite3
import logging
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger("Database")

class DatabaseManager:
    def __init__(self, db_path=":memory:"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    def _init_db(self):
        with self.get_connection() as conn:
            # Table to track data freshness
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_metadata (
                    region_id TEXT PRIMARY KEY,
                    last_updated TIMESTAMP,
                    status TEXT
                )
            """)
            # Table to hold the actual grid data
            conn.execute("""
                CREATE TABLE IF NOT EXISTS grid_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region_id TEXT,
                    timestamp TIMESTAMP,
                    load_mw REAL
                )
            """)

    def update_metadata(self, region_id: str, timestamp: datetime, status: str):
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO sync_metadata (region_id, last_updated, status)
                VALUES (?, ?, ?)
                ON CONFLICT(region_id) DO UPDATE SET 
                last_updated=excluded.last_updated, status=excluded.status
            """, (region_id, timestamp.isoformat(), status))

    def get_last_update_time(self, region_id: str):
        with self.get_connection() as conn:
            cursor = conn.execute("SELECT last_updated FROM sync_metadata WHERE region_id = ?", (region_id,))
            row = cursor.fetchone()
            if row and row['last_updated']:
                return datetime.fromisoformat(row['last_updated'])
            return None