# EnergyAPI-prototype

A prototype of an **event-driven real-time data sync engine** for electricity grid data, using ENTSO-E as the data source (mocked). Demonstrates staleness-gated historical queries, background polling, and async forecast generation.

---

## Architecture

```
User Request
    │
    ├─ GET /historical/{region}?since_hours=N
    │       │
    │       ├─ since_hours > 2 h ──► "deep_history"
    │       │                        Serve straight from DB. No sync.
    │       │
    │       └─ since_hours ≤ 2 h ──► "recent_history"
    │                                 Check last_updated timestamp
    │                                 │
    │                                 ├─ FRESH (< 15 min) ──► Serve from DB
    │                                 └─ STALE (≥ 15 min) ──► Pull from ENTSO-E API
    │                                                               │
    │                                                         Fire SyncEvent
    │                                                         onto queue.Queue
    │                                                               │
    │                                                      InferenceWorker (daemon)
    │                                                      runs model inference
    │                                                      writes ──► forecasts table
    │
    └─ GET /forecast/{region}
            │
            └─ SELECT from forecasts table ──► return instantly
               (no inference on this request)
```

### Key components

| File | Role |
|---|---|
| `db.py` | SQLite wrapper — `grid_data`, `sync_metadata`, `forecasts` tables |
| `sync_engine.py` | Staleness gatekeeper, API fetch, SyncEvent firing, background poller |
| `inference_worker.py` | Daemon thread — consumes SyncEvents, runs inference, writes forecasts |
| `api_client.py` | Mock ENTSO-E client (swap for real `entsoe-py` client) |
| `main.py` | FastAPI app with `/historical` and `/forecast` endpoints |
| `init_db.py` | DB seeder — 24 h of historical load data, stale metadata |
| `demo.py` | Standalone scenario runner (no HTTP server needed) |

### Tunable thresholds (`sync_engine.py`)

| Constant | Default | Meaning |
|---|---|---|
| `STALE_THRESHOLD_MINS` | 15 | Age at which recent data triggers a sync |
| `DEEP_HISTORY_THRESHOLD_HRS` | 2 | Queries older than this skip the gatekeeper |
| `POLL_INTERVAL_SECS` | 30 | Background polling cadence |

---

## Setup

```bash
# 1. Create and activate a virtual environment
python -m venv env
# Windows
env\Scripts\activate
# macOS / Linux
source env/bin/activate

# 2. Install dependencies
pip install fastapi uvicorn

# 3. Seed the database (creates mock_grid_data.db)
python init_db.py
```

---

## Running

### Option A — Standalone demo (no HTTP server)

Runs all four scenarios end-to-end and prints annotated logs:

```bash
python demo.py
```

Expected flow:
- **Scenario A** — deep-history query → served from DB instantly, no sync
- **Scenario B** — recent query, data fresh → served from DB, no sync
- **Scenario C** — recent query, data stale → sync fires → `SyncEvent` → inference → forecast written
- **Scenario D** — forecast query → instant SELECT, returns cached MW value

### Option B — HTTP API

```bash
uvicorn main:app --reload
```

| Endpoint | Description |
|---|---|
| `GET /historical/{region}?since_hours=1` | Recent history (staleness-gated) |
| `GET /historical/{region}?since_hours=6` | Deep history (no sync triggered) |
| `GET /forecast/{region}` | Cached forecast (instant) |
| `GET /docs` | Interactive Swagger UI |

Available regions: `DE_LU`, `FR`, `NL`

---

## Replacing the dummy model

`inference_worker.py` has a `_run_dummy_inference()` method that sleeps and returns a random MW value. To plug in a real model:

1. Load your trained model from a registry inside `_run_dummy_inference()`
2. Fetch features: `self.db.get_historical_data(region_id, since=...)`
3. Call `model.predict(features)` → return the forecast MW

Everything else — the event queue, DB write, and API serving — is unchanged.