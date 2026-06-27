#!/usr/bin/env python3
"""
Air-gap bridge: export the 14-day bronze windows from local Postgres to Parquet and
push them to a Databricks Unity Catalog Volume.

Databricks Free Edition cannot reach the local Postgres (no private networking, no
inbound to the laptop), so the bronze layer crosses the gap as files: this script
runs locally, reads firms_detections + acled_events, drops the PostGIS geom column
(Power BI maps from plain lat/lon), writes Parquet, and uploads via the Databricks
SDK over outbound HTTPS. spark_pipeline_databricks.py then MERGEs the Parquet into
the bronze Delta tables.

Usage:
    python export_bronze.py                              # rolling 14-day window
    python export_bronze.py --start 2025-01-14 --end 2025-01-15  # archive range

Requires in .env:
    DATABASE_URL          postgres DSN (local bronze source)
    DATABRICKS_HOST       e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN      personal access token
    DATABRICKS_VOLUME_PATH  e.g. /Volumes/workspace/fire_pipeline/bronze_inbound
"""

import argparse
import os
import tempfile
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import psycopg2
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/satellite_tracking",
)
VOLUME_PATH = os.environ.get(
    "DATABRICKS_VOLUME_PATH", "/Volumes/workspace/fire_pipeline/bronze_inbound"
).rstrip("/")
LOOKBACK_DAYS = 14
DATA_LAG_DAYS = int(os.environ.get("DATA_LAG_DAYS", "0"))

# Columns to export — schema.sql minus the GEOGRAPHY geom column. The Databricks job
# rebuilds nothing from geom; it works purely on latitude/longitude.
FIRMS_SQL = """
    SELECT id, acq_datetime, latitude, longitude,
           bright_ti4, bright_ti5, frp, scan, track,
           satellite, confidence, daynight, type, version, ingested_at
    FROM firms_detections
    WHERE acq_datetime >= %(cutoff)s
      AND acq_datetime <  %(end_cutoff)s
"""
ACLED_SQL = """
    SELECT id, global_event_id, event_date, event_datetime,
           event_type, sub_event_type, description, num_sources,
           actor1_name, actor2_name,
           action_geo_fullname, action_geo_country,
           fatalities, latitude, longitude, source_url, ingested_at
    FROM acled_events
    WHERE event_datetime >= %(cutoff)s
      AND event_datetime <  %(end_cutoff)s
"""
FIRMS_SQL_ALL = """
    SELECT id, acq_datetime, latitude, longitude,
           bright_ti4, bright_ti5, frp, scan, track,
           satellite, confidence, daynight, type, version, ingested_at
    FROM firms_detections
"""
ACLED_SQL_ALL = """
    SELECT id, global_event_id, event_date, event_datetime,
           event_type, sub_event_type, description, num_sources,
           actor1_name, actor2_name,
           action_geo_fullname, action_geo_country,
           fatalities, latitude, longitude, source_url, ingested_at
    FROM acled_events
"""


def export_table(conn, sql: str, cutoff: datetime = None, end_cutoff: datetime = None) -> pd.DataFrame:
    if cutoff is None:
        return pd.read_sql(sql, conn)
    return pd.read_sql(sql, conn, params={"cutoff": cutoff, "end_cutoff": end_cutoff})


def upload_parquet(w: WorkspaceClient, df: pd.DataFrame, subdir: str) -> str:
    """Write df to a Parquet file and upload it into {VOLUME_PATH}/{subdir}/."""
    # Single-file directory layout; the job reads the directory via spark.read.parquet.
    target = f"{VOLUME_PATH}/{subdir}/{subdir}.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False, coerce_timestamps="us")
        with open(tmp_path, "rb") as fh:
            w.files.upload(target, fh, overwrite=True)
    finally:
        os.unlink(tmp_path)
    return target


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export bronze Postgres tables to Databricks Volume.")
    p.add_argument("--start", type=date.fromisoformat, metavar="YYYY-MM-DD",
                   help="Archive start date (inclusive). Requires --end.")
    p.add_argument("--end",   type=date.fromisoformat, metavar="YYYY-MM-DD",
                   help="Archive end date (inclusive). Requires --start.")
    p.add_argument("--all", action="store_true",
                   help="Export ALL rows from Postgres (no date filter). Use after multi-range archive ingest.")
    args = p.parse_args()
    if bool(args.start) != bool(args.end):
        p.error("--start and --end must be used together")
    if getattr(args, "all") and (args.start or args.end):
        p.error("--all cannot be combined with --start/--end")
    return args


def main() -> None:
    args = _parse_args()

    with psycopg2.connect(DB_DSN) as conn:
        if getattr(args, "all"):
            print("Exporting bronze  |  [all rows — no date filter]")
            firms = export_table(conn, FIRMS_SQL_ALL)
            acled = export_table(conn, ACLED_SQL_ALL)
        elif args.start and args.end:
            cutoff = datetime.combine(args.start, datetime.min.time()).replace(tzinfo=timezone.utc)
            end_cutoff = datetime.combine(args.end + timedelta(days=1), datetime.min.time()).replace(tzinfo=timezone.utc)
            print(f"Exporting bronze  |  [archive] {args.start} to {args.end}")
            firms = export_table(conn, FIRMS_SQL, cutoff, end_cutoff)
            acled = export_table(conn, ACLED_SQL, cutoff, end_cutoff)
        else:
            effective_now = datetime.now(timezone.utc) - timedelta(days=DATA_LAG_DAYS)
            cutoff = effective_now - timedelta(days=LOOKBACK_DAYS)
            end_cutoff = effective_now + timedelta(days=1)
            lag_note = f"  (lag={DATA_LAG_DAYS}d)" if DATA_LAG_DAYS else ""
            print(f"Exporting bronze  |  window [{cutoff.date()} → {effective_now.date()}]{lag_note}")
            firms = export_table(conn, FIRMS_SQL, cutoff, end_cutoff)
            acled = export_table(conn, ACLED_SQL, cutoff, end_cutoff)
    print(f"  firms_detections : {len(firms):,} rows")
    print(f"  acled_events     : {len(acled):,} rows")

    # WorkspaceClient reads DATABRICKS_HOST / DATABRICKS_TOKEN from the environment.
    w = WorkspaceClient()
    print(f"\nUploading to Volume {VOLUME_PATH} ...")
    print(f"  {upload_parquet(w, firms, 'firms_detections')}")
    print(f"  {upload_parquet(w, acled, 'acled_events')}")
    print("\nDone.")


if __name__ == "__main__":
    main()
