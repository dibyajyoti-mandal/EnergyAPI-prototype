import sqlite3
import logging
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger("Database")


class DatabaseManager:
    def __init__(self, db_path="mock_grid_data.db"):
        self.db_path = db_path
        self._init_db()

    @contextmanager
    def get_connection(self):
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.commit()
            conn.close()

    def _init_db(self):
        with self.get_connection() as conn:
            # Tracks when each region's data was last pulled from the external API
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_metadata (
                    region_id TEXT PRIMARY KEY,
                    last_updated TIMESTAMP,
                    status TEXT
                )
            """)
            # Raw time-series load data from ENTSO-E
            conn.execute("""
                CREATE TABLE IF NOT EXISTS grid_data (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region_id TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    load_mw REAL NOT NULL
                )
            """)
            # Stores the latest ML model forecasts (one row per region × horizon)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS forecasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region_id TEXT NOT NULL,
                    forecast_horizon_hrs INTEGER NOT NULL,
                    forecast_mw REAL NOT NULL,
                    generated_at TIMESTAMP NOT NULL,
                    UNIQUE(region_id, forecast_horizon_hrs)
                )
            """)

    # ------------------------------------------------------------------ #
    #  Sync metadata                                                       #
    # ------------------------------------------------------------------ #

    def update_metadata(self, region_id: str, timestamp: datetime, status: str):
        with self.get_connection() as conn:
            conn.execute("""
                INSERT INTO sync_metadata (region_id, last_updated, status)
                VALUES (?, ?, ?)
                ON CONFLICT(region_id) DO UPDATE SET
                    last_updated = excluded.last_updated,
                    status       = excluded.status
            """, (region_id, timestamp.isoformat(), status))

    def get_last_update_time(self, region_id: str):
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT last_updated FROM sync_metadata WHERE region_id = ?",
                (region_id,)
            ).fetchone()
            if row and row["last_updated"]:
                return datetime.fromisoformat(row["last_updated"])
            return None

    # ------------------------------------------------------------------ #
    #  Raw grid data                                                       #
    # ------------------------------------------------------------------ #

    def write_grid_data(self, region_id: str, records: list[tuple]):
        """Insert a list of (timestamp_iso, load_mw) tuples for a region."""
        rows = [(region_id, ts, load) for ts, load in records]
        with self.get_connection() as conn:
            conn.executemany(
                "INSERT INTO grid_data (region_id, timestamp, load_mw) VALUES (?, ?, ?)",
                rows,
            )
        logger.debug(f"[DB] Wrote {len(rows)} grid_data rows for {region_id}")

    def get_historical_data(self, region_id: str, since: datetime) -> list[dict]:
        """Return all grid_data rows for a region after `since` timestamp."""
        with self.get_connection() as conn:
            rows = conn.execute(
                """
                SELECT timestamp, load_mw
                FROM   grid_data
                WHERE  region_id = ? AND timestamp >= ?
                ORDER  BY timestamp ASC
                """,
                (region_id, since.isoformat()),
            ).fetchall()
        return [dict(r) for r in rows]

    def count_rows(self, region_id: str) -> int:
        """Total rows in grid_data for a region (used for logging)."""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM grid_data WHERE region_id = ?",
                (region_id,),
            ).fetchone()
            return row["cnt"] if row else 0

    # ------------------------------------------------------------------ #
    #  Forecasts                                                           #
    # ------------------------------------------------------------------ #

    def write_forecast(self, region_id: str, horizon_hrs: int, forecast_mw: float):
        """Upsert a forecast for the given region and horizon."""
        now = datetime.now().isoformat()
        with self.get_connection() as conn:
            conn.execute(
                """
                INSERT INTO forecasts (region_id, forecast_horizon_hrs, forecast_mw, generated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(region_id, forecast_horizon_hrs) DO UPDATE SET
                    forecast_mw  = excluded.forecast_mw,
                    generated_at = excluded.generated_at
                """,
                (region_id, horizon_hrs, round(forecast_mw, 2), now),
            )
        logger.debug(f"[DB] Upserted forecast for {region_id} horizon={horizon_hrs}h → {forecast_mw:.0f} MW")

    def get_latest_forecast(self, region_id: str) -> dict | None:
        """Return the most recently generated forecast row for a region."""
        with self.get_connection() as conn:
            row = conn.execute(
                """
                SELECT region_id, forecast_horizon_hrs, forecast_mw, generated_at
                FROM   forecasts
                WHERE  region_id = ?
                ORDER  BY generated_at DESC
                LIMIT  1
                """,
                (region_id,),
            ).fetchone()
        return dict(row) if row else None