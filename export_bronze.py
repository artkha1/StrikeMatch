#!/usr/bin/env python3
"""
Air-gap bridge: export the 7-day bronze windows from local Postgres to Parquet and
push them to a Databricks Unity Catalog Volume.

Databricks Free Edition cannot reach the local Postgres (no private networking, no
inbound to the laptop), so the bronze layer crosses the gap as files: this script
runs locally, reads firms_detections + gdelt_events, drops the PostGIS geom column
(Power BI maps from plain lat/lon), writes Parquet, and uploads via the Databricks
SDK over outbound HTTPS. spark_pipeline_databricks.py then MERGEs the Parquet into
the bronze Delta tables.

Usage:
    python export_bronze.py

Requires in .env:
    DATABASE_URL          postgres DSN (local bronze source)
    DATABRICKS_HOST       e.g. https://dbc-xxxx.cloud.databricks.com
    DATABRICKS_TOKEN      personal access token
    DATABRICKS_VOLUME_PATH  e.g. /Volumes/workspace/fire_pipeline/bronze_inbound
"""

import io
import os
import tempfile
from datetime import datetime, timedelta, timezone

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
LOOKBACK_DAYS = 7

# Columns to export — schema.sql minus the GEOGRAPHY geom column. The Databricks job
# rebuilds nothing from geom; it works purely on latitude/longitude.
FIRMS_SQL = """
    SELECT id, acq_datetime, latitude, longitude,
           bright_ti4, bright_ti5, frp, scan, track,
           satellite, confidence, daynight, type, version, ingested_at
    FROM firms_detections
    WHERE acq_datetime >= %(cutoff)s
"""
GDELT_SQL = """
    SELECT id, global_event_id, event_date, event_datetime,
           cameo_code, cameo_root, goldstein_scale,
           num_mentions, num_sources, avg_tone,
           actor1_name, actor2_name,
           action_geo_type, action_geo_fullname, action_geo_country,
           latitude, longitude, source_url, ingested_at
    FROM gdelt_events
    WHERE event_datetime >= %(cutoff)s
"""


def export_table(conn, sql: str, cutoff: datetime) -> pd.DataFrame:
    return pd.read_sql(sql, conn, params={"cutoff": cutoff})


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


def main() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    print(f"Exporting bronze  |  cutoff >= {cutoff.isoformat()}")

    with psycopg2.connect(DB_DSN) as conn:
        firms = export_table(conn, FIRMS_SQL, cutoff)
        gdelt = export_table(conn, GDELT_SQL, cutoff)
    print(f"  firms_detections : {len(firms):,} rows")
    print(f"  gdelt_events     : {len(gdelt):,} rows")

    # WorkspaceClient reads DATABRICKS_HOST / DATABRICKS_TOKEN from the environment.
    w = WorkspaceClient()
    print(f"\nUploading to Volume {VOLUME_PATH} ...")
    print(f"  {upload_parquet(w, firms, 'firms_detections')}")
    print(f"  {upload_parquet(w, gdelt, 'gdelt_events')}")
    print("\nDone.")


if __name__ == "__main__":
    main()
