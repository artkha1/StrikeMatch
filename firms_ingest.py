#!/usr/bin/env python3
"""
FIRMS VIIRS I-Band 375m NRT/SP -> Parquet -> Databricks UC Volume.
Fetch CSVs, filter low-confidence + conflict-zone, write Parquet, upload.

Usage:
    python firms_ingest.py                              # rolling 14-day window
    python firms_ingest.py --start 2025-01-14 --end 2025-01-15  # archive range
Requires FIRMS_MAP_KEY, DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_VOLUME_PATH in .env
"""
import argparse
import csv
import os
import tempfile
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

load_dotenv()

FIRMS_MAP_KEY: str = os.environ["FIRMS_MAP_KEY"]
VOLUME_PATH = os.environ.get(
    "DATABRICKS_VOLUME_PATH", "/Volumes/workspace/fire_pipeline/bronze_inbound"
).rstrip("/")
FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
DATA_LAG_DAYS = int(os.environ.get("DATA_LAG_DAYS", "0"))
LOOKBACK_DAYS = 14

# NRT products serve only the most recent ~10 days; for historical queries use the
# Standard Processing (SP) archive products, which cover the full VIIRS mission.
if DATA_LAG_DAYS > 10:
    VIIRS_SOURCES = ["VIIRS_SNPP_SP", "VIIRS_NOAA20_SP"]
else:
    VIIRS_SOURCES = ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]  # NOAA-21 omitted: near-identical orbit to NOAA-20
# API hard-caps at 5 days per request for global bbox; chunk accordingly.
MAX_DAYS_PER_REQUEST = 5

# Regional bounding boxes sent directly to the FIRMS API (format: "W,S,E,N").
# One request per region per VIIRS source instead of a single global pull.
# Scope: Russia/Ukraine theater + Middle East theater (matches ACLED_COUNTRIES in acled_ingest.py).
# Per-country bounding boxes (min_lat, max_lat, min_lon, max_lon) used as a
# post-download filter to drop fires from non-conflict countries that fall inside
# a regional bbox (e.g. Greece inside "Eastern Europe / Russia").
# Keep in sync with ACLED_COUNTRIES in acled_ingest.py.
COUNTRY_BBOXES: dict[str, tuple[float, float, float, float]] = {
    # Russia/Ukraine theater
    "UP": (44.0,  52.5,  22.0,  40.5),   # Ukraine
    "RS": (41.0,  82.0,  19.0, 180.0),   # Russia
    # Middle East theater
    "IS": (29.0,  33.5,  34.0,  36.0),   # Israel
    "GZ": (31.2,  31.6,  34.2,  34.6),   # Gaza Strip
    "WE": (31.3,  32.6,  34.8,  35.6),   # West Bank
    "SY": (32.5,  37.5,  35.5,  42.5),   # Syria
    "IZ": (29.0,  38.0,  38.5,  48.5),   # Iraq
    "YM": (12.0,  19.0,  42.5,  55.0),   # Yemen
    "LE": (33.0,  34.7,  35.0,  36.7),   # Lebanon
    "IR": (25.0,  40.0,  44.0,  64.0),   # Iran
    "TU": (35.8,  42.5,  25.5,  44.5),   # Turkey
    "QA": (24.5,  26.5,  50.5,  51.7),   # Qatar
    "KU": (28.5,  30.2,  46.5,  48.5),   # Kuwait
    "SA": (16.0,  32.0,  34.5,  55.5),   # Saudi Arabia
    "BA": (25.5,  26.5,  50.3,  50.8),   # Bahrain
    "MU": (16.5,  26.5,  52.0,  60.0),   # Oman
    "JO": (29.0,  33.5,  34.5,  39.5),   # Jordan
    "AE": (22.5,  26.0,  51.0,  56.5),   # UAE
}


def _in_conflict_zone(lat: float, lon: float) -> bool:
    return any(
        mn_lat <= lat <= mx_lat and mn_lon <= lon <= mx_lon
        for mn_lat, mx_lat, mn_lon, mx_lon in COUNTRY_BBOXES.values()
    )


REGION_BBOXES: list[tuple[str, str]] = [
    # (label,                    "W,S,E,N")
    ("Eastern Europe / Russia",  "19,41,180,82"),   # Ukraine, Russia
    ("Middle East",              "25,12,64,43"),    # Israel/Gaza/WB, Lebanon, Syria, Iraq,
                                                    # Yemen, Iran, Turkey, Gulf states, Jordan
]


# -- Fetch ---------------------------------------------------------------------

def _date_chunks(start: date, end: date, max_per: int) -> list[tuple[date, int]]:
    """Split [start .. end] into (start_date, count) chunks."""
    total = (end - start).days + 1
    chunks: list[tuple[date, int]] = []
    current = start
    remaining = total
    while remaining > 0:
        count = min(remaining, max_per)
        chunks.append((current, count))
        current += timedelta(days=count)
        remaining -= count
    return chunks


def _fetch_chunk(url: str, retries: int = 3, backoff: int = 30) -> list[dict]:
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, timeout=(10, 300), stream=True) as resp:
                resp.raise_for_status()
                if "text/html" in resp.headers.get("Content-Type", ""):
                    raise ValueError("API returned HTML — check MAP key / product name")
                return list(csv.DictReader(resp.iter_lines(decode_unicode=True)))
        except (requests.RequestException, TimeoutError) as exc:
            if attempt == retries:
                raise
            print(f" [retry {attempt}/{retries}: {exc}]", end="", flush=True)
            time.sleep(backoff)
    return []  # unreachable


def fetch_source(source: str, start: date, end: date, bbox: str) -> list[dict]:
    chunks = _date_chunks(start, end, MAX_DAYS_PER_REQUEST)
    all_rows: list[dict] = []
    print(f"  {source}", end="", flush=True)
    for chunk_start, count in chunks:
        url = f"{FIRMS_BASE}/{FIRMS_MAP_KEY}/{source}/{bbox}/{count}/{chunk_start}/"
        rows = _fetch_chunk(url)
        all_rows.extend(rows)
        print(f" {chunk_start}+{count}d({len(rows):,})", end="", flush=True)
    print(f"  => {len(all_rows):,} total")
    return all_rows


# -- Parse ---------------------------------------------------------------------

def _f(raw: dict, key: str) -> float | None:
    v = raw.get(key, "").strip()
    return float(v) if v else None


def _i(raw: dict, key: str) -> int | None:
    v = raw.get(key, "").strip()
    return int(v) if v else None


def parse_row(raw: dict) -> tuple | None:
    try:
        lat = float(raw["latitude"])
        lon = float(raw["longitude"])
        t = raw["acq_time"].zfill(4)
        acq_dt = datetime.strptime(
            f"{raw['acq_date']} {t[:2]}:{t[2:]}", "%Y-%m-%d %H:%M"
        ).replace(tzinfo=timezone.utc)
    except (KeyError, ValueError):
        return None

    return (
        acq_dt, lat, lon,
        _f(raw, "bright_ti4"), _f(raw, "bright_ti5"), _f(raw, "frp"),
        _f(raw, "scan"), _f(raw, "track"),
        (raw.get("satellite") or "")[:10] or None,
        (raw.get("confidence") or "")[:10] or None,
        (raw.get("daynight") or "")[:1] or None,
        _i(raw, "type"),
        (raw.get("version") or "")[:10] or None,
    )


# -- DataFrame + upload -------------------------------------------------------

_FIRMS_COLS = [
    "acq_datetime", "latitude", "longitude",
    "bright_ti4", "bright_ti5", "frp", "scan", "track",
    "satellite", "confidence", "daynight", "type", "version",
]

_DELTA_COL_ORDER = [
    "id", "acq_datetime", "latitude", "longitude",
    "bright_ti4", "bright_ti5", "frp", "scan", "track",
    "satellite", "confidence", "daynight", "type", "version", "ingested_at",
]


def _build_dataframe(rows: list[tuple]) -> pd.DataFrame:
    df = pd.DataFrame(rows, columns=_FIRMS_COLS)
    # Stable hash ID from natural key — keeps the Delta MERGE idempotent on re-runs.
    # pd.util.hash_pandas_object is deterministic within a pandas version.
    df["id"] = pd.util.hash_pandas_object(
        df[["acq_datetime", "latitude", "longitude", "satellite"]], index=False
    ).astype("int64")
    df["ingested_at"] = pd.Timestamp.now(tz="UTC")
    return df[_DELTA_COL_ORDER]


def _upload(df: pd.DataFrame, subdir: str) -> None:
    target = f"{VOLUME_PATH}/{subdir}/{subdir}.parquet"
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name
    try:
        df.to_parquet(tmp_path, index=False, coerce_timestamps="us")
        w = WorkspaceClient()
        with open(tmp_path, "rb") as fh:
            w.files.upload(target, fh, overwrite=True)
    finally:
        os.unlink(tmp_path)
    print(f"  Uploaded → {target}  ({len(df):,} rows)")


# -- Main ----------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest FIRMS VIIRS fire detections into Databricks UC Volume.")
    p.add_argument("--start", type=date.fromisoformat, metavar="YYYY-MM-DD",
                   help="Archive start date (inclusive). Requires --end.")
    p.add_argument("--end",   type=date.fromisoformat, metavar="YYYY-MM-DD",
                   help="Archive end date (inclusive). Requires --start.")
    args = p.parse_args()
    if bool(args.start) != bool(args.end):
        p.error("--start and --end must be used together")
    return args


def main() -> None:
    args = _parse_args()

    if args.start and args.end:
        start_date, end_date = args.start, args.end
        days_ago = (date.today() - start_date).days
        viirs_sources = ["VIIRS_SNPP_SP", "VIIRS_NOAA20_SP"] if days_ago > 10 else ["VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT"]
        product_type = "SP (archive)" if days_ago > 10 else "NRT"
        print(f"Fetching FIRMS VIIRS 375m {product_type} [archive] ({start_date} to {end_date}, {len(REGION_BBOXES)} regions)...")
    else:
        today = date.today() - timedelta(days=DATA_LAG_DAYS)
        start_date = today - timedelta(days=LOOKBACK_DAYS - 1)
        end_date = today
        viirs_sources = VIIRS_SOURCES
        product_type = "SP (archive)" if DATA_LAG_DAYS > 10 else "NRT"
        lag_note = f"  (lag={DATA_LAG_DAYS}d, real date {date.today()})" if DATA_LAG_DAYS else ""
        print(f"Fetching FIRMS VIIRS 375m {product_type} ({LOOKBACK_DAYS}-day, {len(REGION_BBOXES)} regions, ending {end_date}{lag_note})...")

    tasks = [
        (src, bbox, label)
        for label, bbox in REGION_BBOXES
        for src in viirs_sources
    ]
    raw_rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(fetch_source, src, start_date, end_date, bbox): (src, label) for src, bbox, label in tasks}
        for fut in as_completed(futures):
            src, label = futures[fut]
            try:
                raw_rows.extend(fut.result())
            except (requests.HTTPError, ValueError) as exc:
                print(f"  WARNING: {src} [{label}] failed - {exc}", file=sys.stderr)

    print(f"Total raw rows: {len(raw_rows):,}")

    # Filter: low confidence, country bbox, parse errors
    parsed: list[tuple] = []
    low_conf = 0
    outside_zone = 0
    parse_err = 0
    for raw in raw_rows:
        if raw.get("confidence", "").lower() in ("l", "low"):
            low_conf += 1
            continue
        t = parse_row(raw)
        if t is None:
            parse_err += 1
            continue
        if not _in_conflict_zone(t[1], t[2]):  # t[1]=lat, t[2]=lon
            outside_zone += 1
            continue
        parsed.append(t)

    print(
        f"After filters: {len(parsed):,}"
        f"  ({low_conf:,} low-conf, {outside_zone:,} outside conflict zone, {parse_err} parse errors)"
    )

    # Exact dedup within the batch (same lat/lon/time/satellite)
    seen: set[tuple] = set()
    deduped: list[tuple] = []
    for row in parsed:
        key = (row[0], row[1], row[2], row[8])  # acq_dt, lat, lon, satellite
        if key not in seen:
            seen.add(key)
            deduped.append(row)

    exact_dups = len(parsed) - len(deduped)
    print(f"After exact dedup: {len(deduped):,}  ({exact_dups:,} removed)")

    if not deduped:
        print("Nothing to upload.")
        return

    df = _build_dataframe(deduped)
    print(f"\nUploading Parquet to UC Volume...")
    _upload(df, "firms_detections")
    print("Done.")


if __name__ == "__main__":
    main()
