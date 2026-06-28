#!/usr/bin/env python3
"""
Phase 4: PySpark bronze->silver transform + FIRMS<->ACLED candidate join.

Reads firms_detections and acled_events from Postgres, applies:
  1. Confidence filter  — drop 'l'/'low' rows (mirrors firms_ingest.py)
  2. Satellite-pass dedup — 1 km / ±6 h window via grid-bin equi-join
  3. FIRMS × ACLED join  — 25 km / event_midnight to +48 h, score computation

Writes:
  firms_silver              <- 7-day deduplicated FIRMS snapshot (overwritten each run)
  fire_event_correlations   <- candidate pairs (staging-table upsert, idempotent)

Usage:
    python spark_pipeline.py

Requires DATABASE_URL in .env (never hardcode credentials here).
The PostgreSQL JDBC driver is downloaded from Maven Central on first run via
spark.jars.packages — needs internet access once, then cached in ~/.ivy2.
"""

import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import psycopg2
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

load_dotenv()

# PySpark 4.x requires Java 17+; Spark on Windows requires HADOOP_HOME/bin/winutils.exe.
# Both env vars are set permanently by the setup steps, but may not be visible yet in
# a shell opened before installation — fall back to known paths so the script works
# without needing a terminal restart.
if not os.environ.get("JAVA_HOME"):
    _java = r"C:\Program Files\Microsoft\jdk-17.0.19.10-hotspot"
    if os.path.isdir(_java):
        os.environ["JAVA_HOME"] = _java
    else:
        raise SystemExit(
            "JAVA_HOME is not set. Install Java 17+ (e.g. winget install "
            "Microsoft.OpenJDK.17) and set JAVA_HOME in your shell or .env."
        )
if not os.environ.get("HADOOP_HOME"):
    _hadoop = r"C:\hadoop"
    if os.path.isfile(os.path.join(_hadoop, "bin", "winutils.exe")):
        os.environ["HADOOP_HOME"] = _hadoop
    # If missing, Spark still works for JDBC-only workloads but prints a WARN.

# Windows App Execution Aliases intercept bare `python` and redirect it to the
# Microsoft Store instead of the real interpreter.  Tell Spark's JVM worker
# launcher exactly which binary to use so UDFs can start Python subprocesses.
if not os.environ.get("PYSPARK_PYTHON"):
    os.environ["PYSPARK_PYTHON"] = sys.executable
if not os.environ.get("PYSPARK_DRIVER_PYTHON"):
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

DB_DSN: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/satellite_tracking",
)
LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "14"))
DATA_LAG_DAYS = int(os.environ.get("DATA_LAG_DAYS", "0"))
SCHEMA_SQL = (pathlib.Path(__file__).parent / "schema.sql").read_text()

# Staging table names (Spark writes here; psycopg2 copies to targets)
_STAGE_SILVER = "_firms_silver_stage"
_STAGE_CORR   = "_fire_event_correlations_stage"

# Dedup constants
_6H_S  = 21_600   # 6 hours in seconds
_1KM_M = 1_006.0  # 1 km + 0.6% for Haversine/WGS-84 meridional-radius divergence

# Join constants
_10KM_M    = 10_000.0    # proximity gate AND scoring denominator (replaces _25KM_M)
_MIN_FRP_MW = 1.0        # minimum FRP (MW) for correlation — filters sub-thermal noise at join time
_48H_S     = 48 * 3600   # fire must be observed within 48 h of event midnight (same/next day)
_6H_BUFFER = 6 * 3600    # small timezone buffer (ACLED event_date is local; FIRMS is UTC)
_SCORE_H   = 54.0        # temporal-decay denominator = 48 + 6


# ── Spark session ──────────────────────────────────────────────────────────────

def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("fire-event-pipeline-phase4")
        # Downloads the PostgreSQL JDBC driver from Maven Central on first run.
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memory", "4g")
        # Low shuffle partition count for local mode; default 200 is wasteful.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _jdbc_conf() -> tuple[str, dict]:
    """Parse DATABASE_URL and return (jdbc_url, properties) for spark.read.jdbc."""
    parsed = urlparse(DB_DSN)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    db   = (parsed.path or "/satellite_tracking").lstrip("/")
    return (
        f"jdbc:postgresql://{host}:{port}/{db}",
        {
            "user":     parsed.username or "postgres",
            "password": parsed.password or "postgres",
            "driver":   "org.postgresql.Driver",
        },
    )


# ── Haversine (native Spark columns — no Python UDF subprocess) ────────────────

def haversine_col(lat1, lon1, lat2, lon2):
    """
    Spherical great-circle distance in metres as a native Spark Column expression.
    Runs entirely in the JVM; no Python worker process is spawned.
    ~0.3 % error vs WGS-84 ellipsoid — negligible for 1 km and 25 km thresholds.
    """
    R = 6_371_000.0
    phi1 = F.radians(lat1)
    phi2 = F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dlam = F.radians(lon2 - lon1)
    a = (
        F.pow(F.sin(dphi / 2.0), 2)
        + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(dlam / 2.0), 2)
    )
    return F.lit(2.0 * R) * F.asin(F.sqrt(F.least(a, F.lit(1.0))))


# ── Bronze reads ───────────────────────────────────────────────────────────────

def read_firms(spark: SparkSession, url: str, props: dict, cutoff: datetime) -> DataFrame:
    # +00 suffix tells Postgres to parse the literal as UTC.
    ts = cutoff.strftime("%Y-%m-%d %H:%M:%S+00")
    return spark.read.jdbc(
        url=url,
        table="firms_detections",
        predicates=[f"acq_datetime >= '{ts}'"],
        properties=props,
    )


def read_acled(spark: SparkSession, url: str, props: dict, cutoff: datetime) -> DataFrame:
    ts = cutoff.strftime("%Y-%m-%d %H:%M:%S+00")
    return spark.read.jdbc(
        url=url,
        table="acled_events",
        predicates=[f"event_datetime >= '{ts}'"],
        properties=props,
    )


# ── Silver transform ───────────────────────────────────────────────────────────

def confidence_filter(df: DataFrame) -> DataFrame:
    """Drop low-confidence FIRMS rows (mirrors the filter in firms_ingest.py)."""
    return df.filter(~F.lower(F.col("confidence")).isin("l", "low"))


def satellite_pass_dedup(df: DataFrame) -> DataFrame:
    """
    Remove near-duplicate detections so no two output rows are within 1 km AND 6 h.

    Algorithm: grid-bin explode + equi-join + Haversine exact check + anti-join.

    Each row is assigned to a (lat_bin, lon_bin, time_bin) cell.  Side b is
    expanded to 9 virtual join keys (one per neighbouring cell) so the join on
    (a.lat_bin == b.join_lat, a.lon_bin == b.join_lon) is an equi-join that
    Spark can hash-partition — avoids an O(n²) cartesian product.

    Within each pair where a.id < b.id and Haversine ≤ 1 km and |Δt| ≤ 6 h,
    b is "dominated" by a and removed.  The surviving row with the lowest id in
    each cluster mirrors the SQL NOT EXISTS first-inserted-row semantics.

    Semantic difference vs. SQL: this processes the full 7-day snapshot at once,
    so transitively dominated rows are also removed (C dominated by B which is
    dominated by A → C is dropped even if C > 1 km from A).  This is more
    aggressive than the incremental SQL dedup but correct for satellite-pass
    clustering — all three detections are the same fire in consecutive passes.
    """
    # Use degree-based bins (0.009° ≈ 1 km in latitude everywhere).
    #
    # Latitude bins are exact: 1° lat = 111 km at any latitude.
    # Longitude bins are NOT uniform — 1° lon = cos(lat)×111 km — so we choose
    # a conservative lon expansion large enough to cover the highest-latitude fires
    # in the dataset (max observed: 72°N).
    #
    # Required lon bin expansion at latitude φ:
    #   k_lon = ceil( 1 km / (0.009° × cos(φ) × 111 km/°) ) + 1  (boundary margin)
    # At 72°N: k_lon = ceil(1000 / (0.009 × 0.309 × 111000)) + 1 = ceil(3.24) + 1 = 5
    #
    # Lat expansion: ±2 covers any lat-only pair within 1 km (same boundary argument
    # as for lon at the equator — two points in bins 2 apart differ by ≥ 0.009° ≈ 999m).
    #
    # All bins are long integers; equi-join is exact.
    _CELL_DEG = 0.009     # ≈1 km in latitude
    _LON_EXP  = 5         # lon neighbour radius; covers fires to ~76°N
    _LAT_EXP  = 2         # lat neighbour radius
    binned = (
        df
        .withColumn("lat_bin",  F.floor(F.col("latitude")  / _CELL_DEG).cast("long"))
        .withColumn("lon_bin",  F.floor(F.col("longitude") / _CELL_DEG).cast("long"))
        .withColumn("time_bin", F.floor(F.col("acq_datetime").cast("long") / _6H_S).cast("long"))
    )

    # Expand b to (2·_LAT_EXP+1) × (2·_LON_EXP+1) = 5×11 = 55 virtual join keys.
    # Haversine below eliminates false-positive candidate pairs from the expansion.
    neighbor_offsets = F.array(*[
        F.struct(F.lit(dlat).alias("dlat"), F.lit(dlon).alias("dlon"))
        for dlat in range(-_LAT_EXP, _LAT_EXP + 1)
        for dlon in range(-_LON_EXP, _LON_EXP + 1)
    ])
    b_expanded = (
        binned
        .withColumn("_off", F.explode(neighbor_offsets))
        .withColumn("join_lat", F.col("lat_bin") + F.col("_off.dlat"))
        .withColumn("join_lon", F.col("lon_bin") + F.col("_off.dlon"))
        .drop("_off")
    )

    # Equi-join: a's own bin == b's virtual join key, adjacent time bins, a.id < b.id.
    # time_bin diff <= 1 means the bins are at most one 6-hour slot apart.  The exact
    # time check below handles the edges where two detections fall in adjacent bins
    # but are actually > 6 h apart (e.g. 0:01 in bin 0 and 11:59 in bin 1 = 11.96 h).
    candidate_pairs = (
        binned.alias("a")
        .join(
            b_expanded.alias("b"),
            (F.col("a.lat_bin") == F.col("b.join_lat")) &
            (F.col("a.lon_bin") == F.col("b.join_lon")) &
            (F.col("a.id") < F.col("b.id")) &
            (F.abs(F.col("a.time_bin") - F.col("b.time_bin")) <= 1),
        )
    )

    # Exact Haversine + exact time check — both must pass.
    dominated_ids = (
        candidate_pairs
        .withColumn(
            "dist_m",
            haversine_col(
                F.col("a.latitude"), F.col("a.longitude"),
                F.col("b.latitude"), F.col("b.longitude"),
            ),
        )
        .withColumn(
            "time_diff_s",
            F.abs(F.col("a.acq_datetime").cast("long") - F.col("b.acq_datetime").cast("long")),
        )
        .filter((F.col("dist_m") <= _1KM_M) & (F.col("time_diff_s") <= _6H_S))
        .select(F.col("a.id").alias("id"))   # drop the older (lower-id) row, keep latest
        .distinct()
    )

    # Anti-join: drop every row whose id has a higher-id (newer) neighbour — keeps latest.
    return df.join(dominated_ids, on="id", how="left_anti")


# ── FIRMS × ACLED candidate join ───────────────────────────────────────────────

def compute_candidates(firms_silver: DataFrame, acled: DataFrame) -> DataFrame:
    """
    Many-to-many spatial-temporal join: FIRMS × ACLED within 25 km and [-72 h, +12 h].

    ACLED is broadcast (small after ingest-time filtering to RU/UA+ME strike events
    for 14 days — typically ≪ FIRMS volume).
    If ACLED grows beyond ~200 MB, remove the broadcast() hint and let Spark choose
    a sort-merge join, or set spark.sql.autoBroadcastJoinThreshold.

    Two-phase spatial:
      1. Bounding-box equi-ish join (cheap, eliminates most pairs).
         Lat margin: 25 km ≈ 0.225°.  Lon margin: 0.225° / cos(lat), capped at 5°.
      2. Exact Haversine UDF on the reduced candidate set → keep ≤ 25 000 m.

    Score formula: 5-factor multiplicative product (see CLAUDE.md).

    Semantic difference vs. correlate.py: the 14-day FIRMS window is fixed at job
    start via a Python datetime rather than Postgres's per-statement NOW().  No
    practical difference for a daily batch job.
    """
    f = firms_silver.select(
        F.col("id").alias("firms_detection_id"),
        F.col("acq_datetime").alias("f_dt"),
        F.col("latitude").alias("f_lat"),
        F.col("longitude").alias("f_lon"),
        F.col("frp"),
        F.col("confidence"),
    ).filter(F.col("frp") >= F.lit(_MIN_FRP_MW))  # drop sub-thermal noise before correlation

    g = acled.select(
        F.col("id").alias("acled_event_id"),
        F.col("event_datetime").alias("g_dt"),
        F.col("latitude").alias("g_lat"),
        F.col("longitude").alias("g_lon"),
        F.col("num_sources"),
    )

    lat_margin = 0.090  # 10 km in degrees latitude
    lon_margin = F.least(
        F.lit(lat_margin) / F.cos(F.radians(F.col("f_lat"))),
        F.lit(5.0),  # cap at 5° for polar/high-latitude cases
    )

    # Event midnight must be ≤ fire_time + 6h (timezone buffer) and ≥ fire_time - 48h.
    # Positive time_delta_h (event after fire) is excluded beyond the 6h buffer —
    # ACLED records actual event date, not publication time, so future events are spurious.
    bbox_joined = f.join(
        F.broadcast(g),
        (F.abs(F.col("f_lat") - F.col("g_lat")) <= lat_margin) &
        (F.abs(F.col("f_lon") - F.col("g_lon")) <= lon_margin) &
        (F.col("g_dt").cast("long") >= F.col("f_dt").cast("long") - _48H_S) &
        (F.col("g_dt").cast("long") <= F.col("f_dt").cast("long") + _6H_BUFFER),
    )

    # Exact Haversine — filter to ≤ 10 km
    with_dist = (
        bbox_joined
        .withColumn(
            "distance_m",
            haversine_col(
                F.col("f_lat"), F.col("f_lon"),
                F.col("g_lat"), F.col("g_lon"),
            ).cast(FloatType()),
        )
        .filter(F.col("distance_m") <= _10KM_M)
    )

    # Time delta in hours; negative means event was published before the fire was detected.
    with_time = with_dist.withColumn(
        "time_delta_h",
        ((F.col("g_dt").cast("long") - F.col("f_dt").cast("long")) / 3600.0).cast(FloatType()),
    )

    # Score: 5-factor multiplicative product (see CLAUDE.md).
    scored = with_time.withColumn(
        "score",
        (
            F.least(F.coalesce(F.col("frp").cast("double"), F.lit(0.0)) / 300.0, F.lit(1.0))
            * F.when(F.col("confidence") == "h", F.lit(1.0)).otherwise(F.lit(0.8))
            * F.least(F.coalesce(F.col("num_sources"), F.lit(1)).cast("double") / 3.0, F.lit(1.0))
            * F.sqrt(F.lit(1.0) - F.col("distance_m").cast("double") / _10KM_M)
            * (F.lit(1.0) - F.abs(F.col("time_delta_h").cast("double")) / _SCORE_H)
        ).cast(FloatType()),
    )

    return scored.select(
        "firms_detection_id", "acled_event_id", "distance_m", "time_delta_h", "score",
    )


# ── Postgres write helpers ─────────────────────────────────────────────────────

_FIRMS_SILVER_COLS = [
    "id", "acq_datetime", "latitude", "longitude",
    "bright_ti4", "bright_ti5", "frp", "scan", "track",
    "satellite", "confidence", "daynight", "type", "version", "ingested_at",
]


def write_firms_silver(firms_silver: DataFrame, url: str, props: dict) -> int:
    """
    Write deduplicated FIRMS rows to firms_silver (overwrite each run).

    Spark writes to a staging table (no PostGIS — Spark knows nothing about
    GEOGRAPHY).  psycopg2 then TRUNCATEs the target, inserts from staging with
    explicit TIMESTAMPTZ and ST_MakePoint casts, and drops the staging table.
    """
    (
        firms_silver.select(_FIRMS_SILVER_COLS)
        .write.mode("overwrite")
        .jdbc(url=url, table=_STAGE_SILVER, properties=props)
    )

    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute("TRUNCATE firms_silver")
            cur.execute(f"""
                INSERT INTO firms_silver
                    (id, acq_datetime, geom, latitude, longitude,
                     bright_ti4, bright_ti5, frp, scan, track,
                     satellite, confidence, daynight, type, version, ingested_at)
                SELECT
                    id,
                    acq_datetime::timestamptz,
                    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography,
                    latitude, longitude,
                    bright_ti4, bright_ti5, frp, scan, track,
                    satellite, confidence, daynight,
                    type, version,
                    ingested_at::timestamptz
                FROM {_STAGE_SILVER}
            """)
            count = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {_STAGE_SILVER}")
        conn.commit()
    return count


def write_correlations(candidates: DataFrame, url: str, props: dict) -> int:
    """
    Upsert candidate pairs to fire_event_correlations.

    Spark can only do plain INSERT via JDBC; ON CONFLICT is not supported.
    Workaround: write to a staging table, then psycopg2 applies
    ON CONFLICT (firms_detection_id, acled_event_id) DO NOTHING — idempotent upsert.
    Staging table is dropped afterward.
    """
    (
        candidates
        .write.mode("overwrite")
        .jdbc(url=url, table=_STAGE_CORR, properties=props)
    )

    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO fire_event_correlations
                    (firms_detection_id, acled_event_id, distance_m, time_delta_h, score)
                SELECT
                    firms_detection_id::bigint,
                    acled_event_id::bigint,
                    distance_m::real,
                    time_delta_h::real,
                    score::real
                FROM {_STAGE_CORR}
                ON CONFLICT (firms_detection_id, acled_event_id) DO NOTHING
            """)
            inserted = cur.rowcount
            cur.execute(f"DROP TABLE IF EXISTS {_STAGE_CORR}")
        conn.commit()
    return inserted


# ── Verification ───────────────────────────────────────────────────────────────

def verify() -> None:
    print("\n-- Verification -------------------------------------------------------")
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM firms_silver")
            n_silver = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM fire_event_correlations")
            n_corr = cur.fetchone()[0]

            cur.execute("""
                SELECT MIN(score), AVG(score)::real, MAX(score)
                FROM fire_event_correlations
            """)
            score_row = cur.fetchone()

            # Self-proximity check: should be 0 (matching firms_ingest.py lines 159-166)
            cur.execute("""
                SELECT COUNT(*)
                FROM firms_silver a
                JOIN firms_silver b ON a.id < b.id
                WHERE ST_DWithin(a.geom, b.geom, 1000)
                  AND ABS(EXTRACT(EPOCH FROM (a.acq_datetime - b.acq_datetime))) <= 21600
            """)
            dup_pairs = cur.fetchone()[0]

            cur.execute("""
                SELECT c.score, c.distance_m, c.time_delta_h,
                       f.latitude, f.longitude, f.frp, f.confidence,
                       g.action_geo_fullname, g.sub_event_type
                FROM fire_event_correlations c
                JOIN firms_silver f ON f.id = c.firms_detection_id
                JOIN acled_events g ON g.id = c.acled_event_id
                ORDER BY c.score DESC
                LIMIT 3
            """)
            top3 = cur.fetchall()

    print(f"1. firms_silver rows:          {n_silver:,}")
    dup_flag = "  <-- DEDUP BUG" if dup_pairs > 0 else ""
    print(f"2. firms_silver dup pairs:     {dup_pairs}{dup_flag}")
    print(f"3. fire_event_correlations:    {n_corr:,}")
    if score_row and score_row[0] is not None:
        print(f"4. Score: min={score_row[0]:.4f}  avg={score_row[1]:.4f}  max={score_row[2]:.4f}")
    else:
        print("4. Score: no rows")
    if top3:
        print("\nTop 3 by score:")
        for r in top3:
            sc, dm, dh, lat, lon, frp, conf, loc, sub_type = r
            print(
                f"  score={sc:.4f}  dist={dm:.0f}m  dt={dh:+.1f}h  "
                f"({lat:.3f},{lon:.3f})  frp={frp or 0:.1f}  conf={conf}  "
                f"loc={loc}  sub_type={sub_type}"
            )


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    effective_now = datetime.now(timezone.utc) - timedelta(days=DATA_LAG_DAYS)
    cutoff = effective_now - timedelta(days=LOOKBACK_DAYS)
    lag_note = f"  (lag={DATA_LAG_DAYS}d)" if DATA_LAG_DAYS else ""
    print(f"Phase 4 PySpark pipeline  |  window [{cutoff.date()} to {effective_now.date()}]{lag_note}")

    # Ensure schema (creates firms_silver if it doesn't exist yet)
    with psycopg2.connect(DB_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.commit()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    url, props = _jdbc_conf()

    # 1. Read bronze tables
    print("\nReading bronze tables from Postgres...")
    firms_raw = read_firms(spark, url, props, cutoff).cache()
    acled_raw = read_acled(spark, url, props, cutoff).cache()
    n_firms_raw = firms_raw.count()
    n_acled_raw = acled_raw.count()
    print(f"  firms_detections : {n_firms_raw:,} rows")
    print(f"  acled_events     : {n_acled_raw:,} rows")

    # 2. Silver transform (confidence filter + satellite-pass dedup)
    print("\nApplying silver transform...")
    firms_silver = satellite_pass_dedup(confidence_filter(firms_raw)).cache()
    n_silver = firms_silver.count()
    print(
        f"  {n_firms_raw:,} bronze  ->  {n_silver:,} silver"
        f"  ({n_firms_raw - n_silver:,} removed by confidence filter + dedup)"
    )

    print("\nWriting firms_silver...")
    written_silver = write_firms_silver(firms_silver, url, props)
    print(f"  {written_silver:,} rows written")

    # 3. FIRMS × ACLED candidate join + write
    print("\nComputing FIRMS x ACLED candidates (25 km / event_midnight ±48 h / +6 h buffer)...")
    candidates = compute_candidates(firms_silver, acled_raw)
    n_candidates = candidates.count()
    print(f"  {n_candidates:,} candidate pairs")

    print("Writing to fire_event_correlations (upsert via staging table)...")
    inserted = write_correlations(candidates, url, props)
    print(f"  {inserted:,} new pairs inserted  (ON CONFLICT skipped existing)")

    # 4. Verify
    verify()

    # Suppress the harmless "Failed to delete temp jar" IOException that the JVM
    # shutdown hook throws on Windows when the file is still locked by the JVM.
    try:
        spark.stop()
    except Exception:
        pass
    print("\nDone.")


if __name__ == "__main__":
    main()
