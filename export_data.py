"""
Export the gold_fire_event_map view to static JSON files for the frontend dashboard.

Writes:
  dashboard/data/events.json    — array of all correlated events (map markers + tooltip data)
  dashboard/data/metadata.json  — pipeline stats (last run, counts, delta)

Run manually or as an Airflow task after validate_pipeline.
Reads DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_SQL_HTTP_PATH,
FP_CATALOG, FP_SCHEMA from .env.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from databricks import sql
from dotenv import load_dotenv

load_dotenv()

_HOST = os.environ["DATABRICKS_HOST"]
_TOKEN = os.environ["DATABRICKS_TOKEN"]
_HTTP_PATH = os.environ["DATABRICKS_SQL_HTTP_PATH"]
_CATALOG = os.getenv("FP_CATALOG", "workspace")
_SCHEMA = os.getenv("FP_SCHEMA", "fire_pipeline")
_VIEW = f"{_CATALOG}.{_SCHEMA}.gold_fire_event_map"

_OUT_DIR = Path(__file__).parent / "dashboard/data"


def _connect():
    return sql.connect(
        server_hostname=_HOST.replace("https://", ""),
        http_path=_HTTP_PATH,
        access_token=_TOKEN,
    )


def _fetch_events(cursor) -> list[dict]:
    cursor.execute(f"""
        SELECT
            map_lat,
            map_lon,
            event_lat,
            event_lon,
            CAST(score_display AS DOUBLE)   AS score_display,
            CAST(fire_frp AS DOUBLE)         AS fire_frp,
            fire_confidence,
            event_sub_event_type,
            event_description,
            event_location_full_name,
            event_source,
            CAST(event_num_sources AS INT)   AS event_num_sources,
            CAST(event_datetime AS STRING)   AS event_datetime,
            CAST(fire_acq_datetime AS STRING) AS fire_acq_datetime,
            CAST(distance_m AS DOUBLE)       AS distance_m,
            CAST(time_delta_h AS DOUBLE)     AS time_delta_h
        FROM {_VIEW}
        ORDER BY score_display DESC
    """)
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def _read_prev_total() -> int:
    path = _OUT_DIR / "metadata.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8")).get("total_events", 0)
        except Exception:
            return 0
    return 0


def _build_metadata(total: int, prev_total: int) -> dict:
    return {
        "last_run_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "total_events": total,
        "events_added_last_run": total - prev_total,
    }


def _round(val, digits=5):
    if isinstance(val, float):
        return round(val, digits)
    return val


def export():
    _OUT_DIR.mkdir(exist_ok=True)

    prev_total = _read_prev_total()

    print(f"Connecting to {_HOST} …")
    with _connect() as conn:
        with conn.cursor() as cur:
            print(f"Querying {_VIEW} …")
            events = _fetch_events(cur)

    print(f"Fetched {len(events)} events.")

    clean = [
        {k: _round(v) for k, v in row.items()}
        for row in events
    ]

    events_path = _OUT_DIR / "events.json"
    events_path.write_text(json.dumps(clean, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {events_path} ({events_path.stat().st_size // 1024} KB)")

    meta = _build_metadata(len(events), prev_total)
    meta_path = _OUT_DIR / "metadata.json"
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {meta_path}")
    print(f"  last_run={meta['last_run_utc']}  total={meta['total_events']}  added={meta['events_added_last_run']}")


if __name__ == "__main__":
    export()
