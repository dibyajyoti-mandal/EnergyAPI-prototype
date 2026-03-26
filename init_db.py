import sqlite3
import random
from datetime import datetime, timedelta

DB_PATH = "mock_grid_data.db"
REGIONS = ["DE_LU", "FR", "NL"]


def init_and_seed_db():
    print(f"Initializing database at '{DB_PATH}'...")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # ── DDL ────────────────────────────────────────────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sync_metadata (
            region_id    TEXT PRIMARY KEY,
            last_updated TIMESTAMP,
            status       TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS grid_data (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            region_id  TEXT NOT NULL,
            timestamp  TIMESTAMP NOT NULL,
            load_mw    REAL NOT NULL
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS forecasts (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            region_id            TEXT NOT NULL,
            forecast_horizon_hrs INTEGER NOT NULL,
            forecast_mw          REAL NOT NULL,
            generated_at         TIMESTAMP NOT NULL,
            UNIQUE(region_id, forecast_horizon_hrs)
        )
    """)

    # ── Wipe existing data ─────────────────────────────────────────────
    cursor.execute("DELETE FROM sync_metadata")
    cursor.execute("DELETE FROM grid_data")
    cursor.execute("DELETE FROM forecasts")

    # ── Seed 24 h of historical load data ─────────────────────────────
    print("Generating 24 hours of historical time-series data...")
    now = datetime.now()
    base_loads = {"DE_LU": 55000.0, "FR": 50000.0, "NL": 15000.0}

    grid_records = []
    for region in REGIONS:
        base = base_loads[region]
        for hours_ago in range(24, 0, -1):          # 24 h ago → 1 h ago
            ts = now - timedelta(hours=hours_ago)
            load = base + random.uniform(-2500.0, 2500.0)
            grid_records.append((region, ts.isoformat(), round(load, 2)))

    cursor.executemany(
        "INSERT INTO grid_data (region_id, timestamp, load_mw) VALUES (?, ?, ?)",
        grid_records,
    )

    # ── Seed sync metadata — set to 2 h ago so it reads as STALE ──────
    print("Setting initial sync metadata (simulating a stale state from 2 h ago)...")
    stale_ts = now - timedelta(hours=2)
    sync_records = [(r, stale_ts.isoformat(), "SUCCESS") for r in REGIONS]

    cursor.executemany(
        "INSERT INTO sync_metadata (region_id, last_updated, status) VALUES (?, ?, ?)",
        sync_records,
    )

    conn.commit()
    conn.close()

    print(
        f"Done — inserted {len(grid_records)} grid rows "
        f"and {len(sync_records)} metadata records."
    )
    print("Run `python demo.py` to exercise the full sync→infer→serve pipeline.")


if __name__ == "__main__":
    init_and_seed_db()