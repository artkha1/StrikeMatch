#!/usr/bin/env python3
"""
GDELT 2.0 Events ingestion -> gdelt_events.
Phase 2 per SPEC.md: 7-day window, CAMEO 190x/193x/195x, ActionGeo_Type 3/4 only.

GDELT 2.0 publishes one export.CSV.zip every 15 minutes (~96 files/day).
We download them in parallel (16 workers) and filter aggressively before inserting.
"""
import csv
import io
import os
import pathlib
import sys
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

load_dotenv()

DB_DSN: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/satellite_tracking",
)
GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"
LOOKBACK_DAYS = 7
SCHEMA_SQL = (pathlib.Path(__file__).parent / "schema.sql").read_text()

CAMEO_PREFIXES = ("190", "195")
GEO_TYPES_OK = {"3", "4"}
DOWNLOAD_WORKERS = 16

# FIPS 10-4 country codes for regions with active armed conflict.
# Events geolocated outside this set are discarded at ingest — they are
# almost always domestic law-enforcement or protest actions mislabelled as
# CAMEO 19x, producing high-scoring false positives against wildfires.
CONFLICT_COUNTRIES = {
    # Russia-Ukraine war
    "UP",  # Ukraine
    "RS",  # Russia
    # Middle East / Levant
    "IS",  # Israel
    "GZ",  # Gaza Strip
    "WE",  # West Bank
    "SY",  # Syria
    "IZ",  # Iraq
    "YM",  # Yemen
    "LE",  # Lebanon
    "IR",  # Iran
    "TU",  # Turkey (Kurdish / Syria border operations)
    "QA",  # Qatar
    "KU",  # Kuwait
    "SA",  # Saudi Arabia
    "BA",  # Bahrain
    "MU",  # Oman
    "JO",  # Jordan
    "AE",  # UAE
    # North Africa
    "LY",  # Libya
    "SU",  # Sudan
    "EG",  # Egypt (Sinai insurgency)
    # East Africa
    "ET",  # Ethiopia (Tigray + Amhara)
    "SO",  # Somalia
    "KE",  # Kenya (Al-Shabaab cross-border attacks)
    "DJ",  # Djibouti (Gulf of Aden / Houthi proximity)
    "RW",  # Rwanda (DRC spillover)
    "UG",  # Uganda (ADF insurgency)
    # Central / Southern Africa
    "OD",  # South Sudan
    "CG",  # DR Congo
    "CT",  # Central African Republic
    "CD",  # Chad
    "MZ",  # Mozambique (Cabo Delgado / ISCAP)
    # West Africa / Sahel
    "ML",  # Mali
    "NG",  # Niger
    "NI",  # Nigeria (ISWAP / Boko Haram)
    "CM",  # Cameroon (Anglophone crisis + Boko Haram)
    # South / Central Asia
    "AF",  # Afghanistan
    "PK",  # Pakistan
    # Southeast Asia
    "BM",  # Myanmar
    # Caucasus
    #"AJ",  # Azerbaijan
    #"AM",  # Armenia
    
    # Americas
    "HA",  # Haiti (state collapse / gang warfare)
    "CO",  # Colombia (FARC / ELN insurgency)
}

# GDELT 2.0 column positions (0-indexed, tab-delimited, no header row)
_C = {
    "global_event_id":      0,
    "sqldate":              1,
    "actor1_name":          6,
    "actor2_name":         16,
    "cameo_code":          26,
    "cameo_root":          28,
    "goldstein_scale":     30,
    "num_mentions":        31,
    "num_sources":         32,
    "avg_tone":            34,
    "action_geo_type":     51,
    "action_geo_fullname": 52,
    "action_geo_country":  53,
    "action_geo_lat":      56,
    "action_geo_long":     57,
    "date_added":          59,
    "source_url":          60,
}


# -- helpers -------------------------------------------------------------------

def _f(cols: list[str], idx: int) -> float | None:
    v = cols[idx].strip()
    return float(v) if v else None


def _i(cols: list[str], idx: int) -> int | None:
    v = cols[idx].strip()
    return int(v) if v else None


def _file_urls(today: date, days: int) -> list[str]:
    """All 15-min export.CSV.zip URLs from (today - days + 1) 00:00 UTC to now."""
    start = datetime(today.year, today.month, today.day, tzinfo=timezone.utc) - timedelta(days=days - 1)
    now = datetime.now(timezone.utc)
    urls: list[str] = []
    cur = start
    while cur <= now:
        urls.append(f"{GDELT_BASE}/{cur.strftime('%Y%m%d%H%M%S')}.export.CSV.zip")
        cur += timedelta(minutes=15)
    return urls


# -- fetch + parse -------------------------------------------------------------

def _fetch_one(url: str) -> tuple[list[tuple], int]:
    """
    Download one GDELT 15-min zip, return (filtered_rows, raw_row_count).
    Returns ([], 0) silently on 404 (file not yet published).
    """
    resp = requests.get(url, timeout=(5, 60))
    if resp.status_code == 404:
        return [], 0
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_name = next(n for n in zf.namelist() if n.endswith(".CSV"))
        text = zf.open(csv_name).read().decode("latin-1")

    rows: list[tuple] = []
    raw = 0
    for cols in csv.reader(io.StringIO(text), delimiter="\t"):
        raw += 1
        if len(cols) < 61:
            continue

        # CAMEO filter
        cameo = cols[_C["cameo_code"]]
        if not any(cameo.startswith(p) for p in CAMEO_PREFIXES):
            continue

        # Geo precision filter
        geo_type = cols[_C["action_geo_type"]].strip()
        if geo_type not in GEO_TYPES_OK:
            continue

        # Country allowlist — exclude non-conflict geographies
        if cols[_C["action_geo_country"]].strip() not in CONFLICT_COUNTRIES:
            continue

        # Must have coordinates
        lat_s = cols[_C["action_geo_lat"]].strip()
        lon_s = cols[_C["action_geo_long"]].strip()
        if not lat_s or not lon_s:
            continue

        try:
            lat = float(lat_s)
            lon = float(lon_s)
            geid = int(cols[_C["global_event_id"]])
            ev_date = datetime.strptime(cols[_C["sqldate"]], "%Y%m%d").date()
            ev_dt = datetime.strptime(cols[_C["date_added"]], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
            root_s = cols[_C["cameo_root"]].strip()
            cameo_root = int(root_s) if root_s.isdigit() else None
        except ValueError:
            continue

        rows.append((
            geid,
            ev_date,
            ev_dt,
            cameo[:10],
            cameo_root,
            _f(cols, _C["goldstein_scale"]),
            _i(cols, _C["num_mentions"]),
            _i(cols, _C["num_sources"]),
            _f(cols, _C["avg_tone"]),
            cols[_C["actor1_name"]].strip() or None,
            cols[_C["actor2_name"]].strip() or None,
            int(geo_type),
            cols[_C["action_geo_fullname"]].strip() or None,
            cols[_C["action_geo_country"]].strip()[:5] or None,
            lat,
            lon,
            cols[_C["source_url"]].strip() or None,
        ))

    return rows, raw


# -- main ----------------------------------------------------------------------

def main() -> None:
    today = date.today()
    urls = _file_urls(today, LOOKBACK_DAYS)
    print(
        f"GDELT 2.0 ingest: {len(urls)} 15-min files to check "
        f"({LOOKBACK_DAYS}-day window ending {today})"
    )

    conn = psycopg2.connect(DB_DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)

        # Parallel download + parse
        print(f"Downloading with {DOWNLOAD_WORKERS} workers...", flush=True)
        all_rows: list[tuple] = []
        total_raw = 0
        errors = 0

        with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as pool:
            futures = {pool.submit(_fetch_one, url): url for url in urls}
            done = 0
            for future in as_completed(futures):
                done += 1
                try:
                    rows, raw = future.result()
                    all_rows.extend(rows)
                    total_raw += raw
                except Exception as exc:
                    errors += 1
                    print(f"\n  WARNING: {futures[future]} - {exc}", file=sys.stderr)
                if done % 96 == 0:  # progress every ~1 day of files
                    print(f"  {done}/{len(urls)} files  {len(all_rows):,} rows so far", flush=True)

        print(
            f"{len(urls)} files checked  |  {total_raw:,} raw rows  |  "
            f"{len(all_rows):,} passed filter  |  {errors} errors"
        )

        # Dedup within batch by GLOBALEVENTID (GDELT can re-emit events)
        seen: set[int] = set()
        deduped: list[tuple] = []
        for r in all_rows:
            if r[0] not in seen:
                seen.add(r[0])
                deduped.append(r)
        print(f"After exact dedup: {len(deduped):,}  ({len(all_rows) - len(deduped):,} removed)")

        if not deduped:
            print("Nothing to insert.")
        else:
            # Append lon, lat for ST_MakePoint(longitude, latitude)
            extended = [(*r, r[15], r[14]) for r in deduped]
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO gdelt_events
                            (global_event_id, event_date, event_datetime,
                             cameo_code, cameo_root,
                             goldstein_scale, num_mentions, num_sources, avg_tone,
                             actor1_name, actor2_name,
                             action_geo_type, action_geo_fullname, action_geo_country,
                             latitude, longitude, source_url, geom)
                        VALUES %s
                        ON CONFLICT (global_event_id) DO NOTHING
                        """,
                        extended,
                        template=(
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            "ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)"
                        ),
                        page_size=500,
                    )
                    inserted = cur.rowcount
            print(f"Inserted: {inserted:,}  (ON CONFLICT skipped {len(deduped) - inserted:,})")

        # Verification (SPEC Phase 2 checks 1-3)
        print("\n-- Verification -------------------------------------------------------")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gdelt_events")
            total: int = cur.fetchone()[0]

            cur.execute("""
                SELECT cameo_root, COUNT(*) AS n
                FROM gdelt_events
                GROUP BY cameo_root ORDER BY cameo_root
            """)
            cameo_dist = cur.fetchall()

            cur.execute("""
                SELECT global_event_id, event_datetime, actor1_name, actor2_name,
                       action_geo_fullname, goldstein_scale
                FROM gdelt_events ORDER BY ingested_at DESC, id DESC LIMIT 3
            """)
            samples = cur.fetchall()

        print(f"1. gdelt_events total: {total:,}")
        print(f"2. CAMEO root distribution: {cameo_dist}")
        print("3. Sample rows (3 most recently ingested):")
        for r in samples:
            print(f"   {r[0]}  {r[1]}  [{r[2] or '?'} -> {r[3] or '?'}]  {r[4]}  gs={r[5]}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
