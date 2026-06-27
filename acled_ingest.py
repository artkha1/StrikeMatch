#!/usr/bin/env python3
"""
ACLED conflict-event ingestion -> acled_events.
RU/UA + Middle East, strike sub_event_types only, 14-day rolling window.

filter -> parse -> dedup -> insert -> verify  (mirrors gdelt_ingest.py structure)

Usage:
    python acled_ingest.py                              # rolling 14-day window
    python acled_ingest.py --start 2025-01-14 --end 2025-01-15  # archive range

Requires ACLED_USERNAME / ACLED_PASSWORD in .env (Research-tier OAuth credentials).
"""
import argparse
import os
import pathlib
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
SCHEMA_SQL = (pathlib.Path(__file__).parent / "schema.sql").read_text()

LOOKBACK_DAYS = 14
PAGE_SIZE = 5000

# Free Research-tier accounts trail real-time by ~52 weeks.
# Set DATA_LAG_DAYS in .env to shift the rolling window back accordingly.
# Paid / real-time access: leave at 0.
DATA_LAG_DAYS = int(os.environ.get("DATA_LAG_DAYS", "0"))

ACLED_TOKEN_URL = "https://acleddata.com/oauth/token"
ACLED_READ_URL = "https://acleddata.com/api/acled/read"

# ACLED sub_event_types to keep (Explosions/Remote violence family)
STRIKE_SUBTYPES = {
    "Air/drone strike",
    "Shelling/artillery/missile attack"
}

# geo_precision 1 = exact coordinates, 2 = nearest admin center (<25 km typical)
GEO_PRECISION_OK = {1, 2}

# Countries in scope: Russia/Ukraine theater + Middle East theater
ACLED_COUNTRIES = [
    "Ukraine", "Russia",
    "Israel", "Palestine", "Syria", "Iraq", "Yemen", "Lebanon",
    "Iran", "Turkey", "Saudi Arabia", "Jordan", "Kuwait", "Bahrain",
    "Qatar", "Oman", "United Arab Emirates",
]


# -- Auth ----------------------------------------------------------------------

def _get_token() -> str:
    resp = requests.post(
        ACLED_TOKEN_URL,
        data={
            "grant_type": "password",
            "client_id": "acled",
            "scope": "authenticated",
            "username": os.environ["ACLED_USERNAME"],
            "password": os.environ["ACLED_PASSWORD"],
        },
        timeout=(10, 30),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# -- Fetch ---------------------------------------------------------------------

def _fetch_page(token: str, country: str, since: str, until: str, page: int) -> list[dict]:
    resp = requests.get(
        ACLED_READ_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "_format": "json",
            "country": country,
            "event_type": "Explosions/Remote violence",
            "event_date": f"{since}|{until}",
            "event_date_where": "BETWEEN",
            "limit": PAGE_SIZE,
            "page": page,
        },
        timeout=(10, 60),
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def _fetch_all(token: str, since: str, until: str) -> list[dict]:
    all_rows: list[dict] = []
    for country in ACLED_COUNTRIES:
        page = 1
        country_total = 0
        while True:
            rows = _fetch_page(token, country, since, until, page)
            all_rows.extend(rows)
            country_total += len(rows)
            if len(rows) < PAGE_SIZE:
                break
            page += 1
        print(f"  {country}: {country_total:,}  (cumulative: {len(all_rows):,})", flush=True)
    return all_rows


# -- Parse + filter -----------------------------------------------------------

def _parse_row(r: dict) -> tuple | None:
    try:
        lat = float(r["latitude"])
        lon = float(r["longitude"])
        ev_date = datetime.strptime(r["event_date"], "%Y-%m-%d").date()
        ev_dt = datetime(ev_date.year, ev_date.month, ev_date.day, tzinfo=timezone.utc)
        geo_prec = int(r.get("geo_precision", 0))
    except (KeyError, ValueError, TypeError):
        return None

    if geo_prec not in GEO_PRECISION_OK:
        return None

    sub_type = (r.get("sub_event_type") or "").strip()
    if sub_type not in STRIKE_SUBTYPES:
        return None

    sources_raw = (r.get("source") or "").strip()
    source_parts = [s.strip() for s in sources_raw.split(";") if s.strip()]
    num_sources = max(len(source_parts), 1)

    return (
        str(r["event_id_cnty"]),                                           # [0]  global_event_id TEXT
        ev_date,                                                            # [1]  event_date
        ev_dt,                                                              # [2]  event_datetime
        (r.get("event_type") or "").strip() or None,                       # [3]  event_type
        sub_type or None,                                                   # [4]  sub_event_type
        (r.get("notes") or "").strip() or None,                            # [5]  description
        num_sources,                                                        # [6]  num_sources
        (r.get("actor1") or "").strip() or None,                           # [7]  actor1_name
        (r.get("actor2") or "").strip() or None,                           # [8]  actor2_name
        (r.get("location") or "").strip() or None,                         # [9]  action_geo_fullname
        (r.get("country") or "").strip() or None,                          # [10] action_geo_country
        int(r["fatalities"]) if r.get("fatalities") is not None else None, # [11] fatalities
        lat,                                                                # [12] latitude
        lon,                                                                # [13] longitude
        sources_raw or None,                                                # [14] source_url
    )


# -- Main ----------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest ACLED strike events into Postgres.")
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
        since = args.start.strftime("%Y-%m-%d")
        until = args.end.strftime("%Y-%m-%d")
        print(f"ACLED ingest: [archive] {since} to {until}")
    else:
        effective_today = date.today() - timedelta(days=DATA_LAG_DAYS)
        since = (effective_today - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")
        until = effective_today.strftime("%Y-%m-%d")
        print(
            f"ACLED ingest: {LOOKBACK_DAYS}-day window {since} to {until}"
            + (f"  (lag={DATA_LAG_DAYS}d, real date {date.today()})" if DATA_LAG_DAYS else "")
        )

    token = _get_token()
    print("OAuth token acquired. Fetching events by country...")

    raw_rows = _fetch_all(token, since, until)
    print(f"\nTotal raw rows fetched: {len(raw_rows):,}")

    # Parse + filter (geo_precision, sub_event_type)
    parsed: list[tuple] = []
    skipped = 0
    for r in raw_rows:
        t = _parse_row(r)
        if t is None:
            skipped += 1
        else:
            parsed.append(t)
    print(
        f"After parse/filter: {len(parsed):,}  "
        f"({skipped:,} skipped — bad coords, geo_precision=3, or non-strike sub_event_type)"
    )

    # Dedup within batch by event_id_cnty (ACLED can return the same event_id_cnty for
    # events appearing in multiple country queries if geo overlaps)
    seen: set[str] = set()
    deduped: list[tuple] = []
    for r in parsed:
        if r[0] not in seen:
            seen.add(r[0])
            deduped.append(r)
    print(f"After dedup: {len(deduped):,}  ({len(parsed) - len(deduped):,} removed)")

    conn = psycopg2.connect(DB_DSN)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL)

        if not deduped:
            print("Nothing to insert.")
        else:
            # Append (lon, lat) for ST_MakePoint(longitude, latitude) — index [15], [16]
            extended = [(*r, r[13], r[12]) for r in deduped]
            with conn:
                with conn.cursor() as cur:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO acled_events
                            (global_event_id, event_date, event_datetime,
                             event_type, sub_event_type, description, num_sources,
                             actor1_name, actor2_name,
                             action_geo_fullname, action_geo_country,
                             fatalities, latitude, longitude, source_url, geom)
                        VALUES %s
                        ON CONFLICT (global_event_id) DO NOTHING
                        """,
                        extended,
                        template=(
                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                            "ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)"
                        ),
                        page_size=500,
                    )
                    inserted = cur.rowcount
            print(f"Inserted: {inserted:,}  (ON CONFLICT skipped {len(deduped) - inserted:,})")

        # Verification
        print("\n-- Verification -------------------------------------------------------")
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM acled_events")
            total: int = cur.fetchone()[0]

            cur.execute("""
                SELECT sub_event_type, COUNT(*) AS n
                FROM acled_events
                GROUP BY sub_event_type ORDER BY sub_event_type
            """)
            subtype_dist = cur.fetchall()

            cur.execute("""
                SELECT global_event_id, event_datetime, actor1_name, actor2_name,
                       action_geo_fullname, sub_event_type, fatalities
                FROM acled_events ORDER BY ingested_at DESC, id DESC LIMIT 3
            """)
            samples = cur.fetchall()

        print(f"1. acled_events total: {total:,}")
        print(f"2. sub_event_type distribution: {subtype_dist}")
        print("3. Sample rows (3 most recently ingested):")
        for r in samples:
            print(
                f"   {r[0]}  {r[1]}  [{r[2] or '?'} -> {r[3] or '?'}]"
                f"  {r[4]}  {r[5]}  fatalities={r[6]}"
            )

    finally:
        conn.close()


if __name__ == "__main__":
    main()
