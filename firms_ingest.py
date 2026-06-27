#!/usr/bin/env python3
"""
FIRMS VIIRS I-Band 375m NRT -> PostGIS ingestion.
Fetch the rolling window (RU/UA + Middle East), filter low-confidence, dedup, insert.
Design notes (source, filter, dedup) in CLAUDE.md.

Usage:
    python firms_ingest.py
Requires FIRMS_MAP_KEY in .env (get one free at https://firms.modaps.eosdis.nasa.gov/api/)
"""
import csv
import os
import pathlib
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

FIRMS_MAP_KEY: str = os.environ["FIRMS_MAP_KEY"]
DB_DSN: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/satellite_tracking",
)
FIRMS_BASE = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
# Shift the rolling window back to stay in sync with ACLED's data lag.
# Set DATA_LAG_DAYS in .env. 0 for paid/real-time access (default).
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

SCHEMA_SQL = (pathlib.Path(__file__).parent / "schema.sql").read_text()

CREATE_STAGE_SQL = """
CREATE TEMP TABLE _stage (
    acq_datetime TIMESTAMPTZ      NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    bright_ti4   REAL,
    bright_ti5   REAL,
    frp          REAL,
    scan         REAL,
    track        REAL,
    satellite    VARCHAR(10),
    confidence   VARCHAR(10),
    daynight     CHAR(1),
    type         SMALLINT,
    version      VARCHAR(10)
) ON COMMIT DROP;
"""

# Eliminate within-batch near-duplicates before inserting into firms_detections.
# The NOT EXISTS check in INSERT_FROM_STAGE_SQL only compares against already-committed
# rows, so it can't dedup within the batch itself (critical on a fresh/truncated table).
# Keep the row with the smallest _rid in each 1km/6h cluster; same-fire cross-satellite
# detections within the window collapse to one representative.
_STAGE_SELF_DEDUP_SQL = """
DELETE FROM _stage s2
USING _stage s1
WHERE s1._rid < s2._rid
  AND ST_DWithin(s1.geom, s2.geom, 1000)
  AND ABS(EXTRACT(EPOCH FROM (s1.acq_datetime - s2.acq_datetime))) <= 21600
"""

INSERT_FROM_STAGE_SQL = """
INSERT INTO firms_detections
    (acq_datetime, geom, latitude, longitude,
     bright_ti4, bright_ti5, frp, scan, track,
     satellite, confidence, daynight, type, version)
SELECT
    s.acq_datetime,
    s.geom,
    s.latitude, s.longitude,
    s.bright_ti4, s.bright_ti5, s.frp, s.scan, s.track,
    s.satellite, s.confidence, s.daynight, s.type, s.version
FROM _stage s
WHERE NOT EXISTS (
    SELECT 1
    FROM firms_detections d
    WHERE ST_DWithin(
              d.geom,
              s.geom,
              1000
          )
      AND ABS(EXTRACT(EPOCH FROM (d.acq_datetime - s.acq_datetime))) <= 21600
)
"""


# -- Fetch ---------------------------------------------------------------------

def _date_chunks(today: date, days: int, max_per: int) -> list[tuple[date, int]]:
    """Split [today-days+1 .. today] into (start_date, count) chunks."""
    start = today - timedelta(days=days - 1)
    chunks: list[tuple[date, int]] = []
    current = start
    remaining = days
    while remaining > 0:
        count = min(remaining, max_per)
        chunks.append((current, count))
        current += timedelta(days=count)
        remaining -= count
    return chunks


def fetch_source(source: str, today: date, bbox: str) -> list[dict]:
    chunks = _date_chunks(today, LOOKBACK_DAYS, MAX_DAYS_PER_REQUEST)
    all_rows: list[dict] = []
    print(f"  {source}", end="", flush=True)
    for start, count in chunks:
        url = f"{FIRMS_BASE}/{FIRMS_MAP_KEY}/{source}/{bbox}/{count}/{start}/"
        with requests.get(url, timeout=(10, 300), stream=True) as resp:
            resp.raise_for_status()
            if "text/html" in resp.headers.get("Content-Type", ""):
                raise ValueError(f"API returned HTML — check MAP key / product name")
            rows = list(csv.DictReader(resp.iter_lines(decode_unicode=True)))
            all_rows.extend(rows)
        print(f" {start}+{count}d({len(rows):,})", end="", flush=True)
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


# -- Verification --------------------------------------------------------------

def verify(conn: "psycopg2.connection") -> None:
    print("\n-- Verification -------------------------------------------------------")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM firms_detections")
        total: int = cur.fetchone()[0]

        cur.execute("SELECT ST_Extent(geom::geometry) FROM firms_detections")
        bbox = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*)
            FROM firms_detections a
            JOIN firms_detections b ON a.id < b.id
            WHERE a.satellite = b.satellite
              AND ST_DWithin(a.geom, b.geom, 1000)
              AND ABS(EXTRACT(EPOCH FROM (a.acq_datetime - b.acq_datetime))) <= 21600
        """)
        dup_pairs: int = cur.fetchone()[0]

        cur.execute("""
            SELECT id, acq_datetime, latitude, longitude, satellite, confidence, frp
            FROM firms_detections
            ORDER BY ingested_at DESC, id DESC
            LIMIT 3
        """)
        samples = cur.fetchall()

    print(f"1. Row count:    {total:,}")
    print(f"2. Bounding box: {bbox}")
    dup_flag = "  <-- WARNING: dedup bug?" if dup_pairs > 0 else ""
    print(f"3. Dup pairs (same sat, <=1km, <=6h): {dup_pairs}{dup_flag}")
    print()
    print("Sample rows (3 most recently ingested):")
    print(f"  {'id':>8}  {'acq_datetime':<28}  {'lat':>9}  {'lon':>10}  {'sat':<3}  {'conf':<4}  frp")
    for row in samples:
        print(
            f"  {row[0]:>8}  {str(row[1]):<28}  {row[2]:>9.4f}  {row[3]:>10.4f}"
            f"  {str(row[4] or ''):<3}  {str(row[5] or ''):<4}  {row[6]}"
        )


# -- Main ----------------------------------------------------------------------

def main() -> None:
    today = date.today() - timedelta(days=DATA_LAG_DAYS)
    product_type = "SP (archive)" if DATA_LAG_DAYS > 10 else "NRT"
    lag_note = f"  (lag={DATA_LAG_DAYS}d, real date {date.today()})" if DATA_LAG_DAYS else ""
    print(f"Fetching FIRMS VIIRS 375m {product_type} ({LOOKBACK_DAYS}-day, {len(REGION_BBOXES)} regions, ending {today}{lag_note})...")

    tasks = [
        (src, bbox, label)
        for label, bbox in REGION_BBOXES
        for src in VIIRS_SOURCES
    ]
    raw_rows: list[dict] = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        futures = {pool.submit(fetch_source, src, today, bbox): (src, label) for src, bbox, label in tasks}
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
    print(f"After exact dedup:           {len(deduped):,}  ({exact_dups:,} removed)")

    conn = psycopg2.connect(DB_DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)

        if not deduped:
            print("Nothing to insert.")
            verify(conn)
            return

        n_candidates = len(deduped)
        print(f"\nStaging {n_candidates:,} candidates, applying 1km/6h spatial dedup...")
        with conn:
            with conn.cursor() as cur:
                cur.execute(CREATE_STAGE_SQL)
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO _stage
                        (acq_datetime, latitude, longitude,
                         bright_ti4, bright_ti5, frp, scan, track,
                         satellite, confidence, daynight, type, version)
                    VALUES %s
                    """,
                    deduped,
                    page_size=2000,
                )

                # Build spatial index on staging table, then self-dedup within batch.
                cur.execute("ALTER TABLE _stage ADD COLUMN geom geography")
                cur.execute("UPDATE _stage SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography")
                cur.execute("CREATE INDEX ON _stage USING GIST(geom)")
                cur.execute("ALTER TABLE _stage ADD COLUMN _rid SERIAL")
                cur.execute(_STAGE_SELF_DEDUP_SQL)
                within_batch_dups = cur.rowcount

                cur.execute(INSERT_FROM_STAGE_SQL)
                inserted = cur.rowcount

        incremental_skipped = n_candidates - within_batch_dups - inserted
        print(
            f"Within-batch dedup: {within_batch_dups:,} removed  |  "
            f"Inserted: {inserted:,}  (incremental spatial dedup skipped {incremental_skipped:,})"
        )

        verify(conn)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
