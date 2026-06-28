#!/usr/bin/env python3
"""
Phase 4 (Databricks Free Edition): PySpark bronze->silver->gold on Delta tables.

This is the Databricks-serverless port of spark_pipeline.py. It runs as a Databricks
Job task (or notebook). The spatial/temporal transform is identical — the same native
Spark-column Haversine logic — but all Postgres/PostGIS I/O is replaced with Delta:

  bronze   workspace.fire_pipeline.firms_detections   (loaded from Parquet on a UC Volume)
           workspace.fire_pipeline.acled_events        (loaded from Parquet on a UC Volume)
  silver   workspace.fire_pipeline.firms_silver        (overwritten each run)
  gold     workspace.fire_pipeline.fire_event_correlations (MERGE upsert)
           workspace.fire_pipeline.gold_fire_event_map (serving view for Power BI)

Why this differs from spark_pipeline.py (and why it is serverless-safe):
  * No SparkSession.builder.config(...) — serverless provides `spark` and rejects
    cluster config (driver memory, jars.packages, shuffle.partitions, master).
  * No psycopg2 / PostGIS / GEOGRAPHY. Power BI maps from plain lat/lon, so geom is
    dropped. Delta MERGE replaces the staging-table + ON CONFLICT dance.
  * No JDBC read of local Postgres — Databricks cannot reach it. Bronze arrives as
    Parquet on a Unity Catalog Volume, pushed up by the ingest scripts.

Config via environment / Databricks job parameters (all have defaults):
  FP_CATALOG        default "workspace"
  FP_SCHEMA         default "fire_pipeline"
  FP_VOLUME_INBOUND default "/Volumes/workspace/fire_pipeline/bronze_inbound"
"""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

# On Databricks `spark` is pre-provisioned in the notebook/job globals. When this file
# is run as a job task we fetch the active session — serverless returns it without any
# cluster config, which is exactly what we want.
spark = SparkSession.builder.getOrCreate()

# ── Identifiers ─────────────────────────────────────────────────────────────────
CATALOG = os.environ.get("FP_CATALOG", "workspace")
SCHEMA = os.environ.get("FP_SCHEMA", "fire_pipeline")
VOLUME_INBOUND = os.environ.get(
    "FP_VOLUME_INBOUND", f"/Volumes/{CATALOG}/{SCHEMA}/bronze_inbound"
)

_NS = f"`{CATALOG}`.`{SCHEMA}`"
T_FIRMS_BRONZE = f"{_NS}.firms_detections"
T_ACLED_BRONZE = f"{_NS}.acled_events"
T_FIRMS_SILVER = f"{_NS}.firms_silver"
T_CORR_GOLD = f"{_NS}.fire_event_correlations"
V_GOLD_MAP = f"{_NS}.gold_fire_event_map"

# Parquet drop locations on the Volume (written by firms_ingest.py / acled_ingest.py).
P_FIRMS = f"{VOLUME_INBOUND}/firms_detections"
P_ACLED = f"{VOLUME_INBOUND}/acled_events"

# No LOOKBACK_DAYS or DATA_LAG_DAYS here. The Databricks job processes exactly what
# export_bronze.py uploaded to the Volume — date scoping is done there (rolling window
# or --all for archive runs). Reading back from Delta with a time filter would anchor
# on the Delta table's max timestamp and silently exclude archive data.

# Dedup constants (identical to spark_pipeline.py)
_6H_S = 21_600     # 6 hours in seconds
_1KM_M = 1_006.0   # 1 km + 0.6% for Haversine/WGS-84 meridional-radius divergence

# Join constants (identical to spark_pipeline.py)
_10KM_M    = 10_000.0    # proximity gate AND scoring denominator (replaces _25KM_M)
_MIN_FRP_MW = 1.0        # minimum FRP (MW) for correlation — filters sub-thermal noise at join time
_48H_S     = 48 * 3600   # fire must be observed within 48 h of event midnight (same/next day)
_6H_BUFFER = 6 * 3600    # small timezone buffer (ACLED event_date is local; FIRMS is UTC)
_SCORE_H   = 54.0        # temporal-decay denominator = 48 + 6


# ── Haversine (native Spark columns — no Python UDF subprocess) ────────────────
# Verbatim from spark_pipeline.py — runs entirely in the JVM, serverless-safe.

def haversine_col(lat1, lon1, lat2, lon2):
    """Spherical great-circle distance in metres as a native Spark Column."""
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


# ── Silver transform (verbatim logic from spark_pipeline.py) ────────────────────

def confidence_filter(df: DataFrame) -> DataFrame:
    """Drop low-confidence FIRMS rows (mirrors the filter in firms_ingest.py)."""
    return df.filter(~F.lower(F.col("confidence")).isin("l", "low"))


def _dominated_ids(df: DataFrame) -> DataFrame:
    """
    Grid-bin explode + equi-join + exact Haversine: return the ids that are
    'dominated' (within 1 km AND 6 h of a lower-id detection). Shared by
    satellite_pass_dedup and the post-run verification check.
    """
    _CELL_DEG = 0.009  # ≈1 km in latitude
    _LON_EXP = 5       # lon neighbour radius; covers fires to ~76°N
    _LAT_EXP = 2       # lat neighbour radius
    binned = (
        df
        .withColumn("lat_bin", F.floor(F.col("latitude") / _CELL_DEG).cast("long"))
        .withColumn("lon_bin", F.floor(F.col("longitude") / _CELL_DEG).cast("long"))
        .withColumn("time_bin", F.floor(F.col("acq_datetime").cast("long") / _6H_S).cast("long"))
    )

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

    return (
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


def satellite_pass_dedup(df: DataFrame) -> DataFrame:
    """
    Remove near-duplicate detections so no two output rows are within 1 km AND 6 h.
    Grid-bin explode + equi-join + Haversine + anti-join (see spark_pipeline.py for
    the full derivation of the bin-expansion radii).
    """
    return df.join(_dominated_ids(df), on="id", how="left_anti")


# ── FIRMS × ACLED candidate join ───────────────────────────────────────────────

def compute_candidates(firms_silver: DataFrame, acled: DataFrame) -> DataFrame:
    """
    Many-to-many spatial-temporal join: FIRMS × ACLED within 25 km and [-72 h, +12 h].
    Score formula is the calibrated 5-factor multiplicative product (see CLAUDE.md).
    Denormalized fire/event coordinates are included so gold rows are self-contained.
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
        F.col("sub_event_type").alias("event_sub_event_type"),
        F.col("description").alias("event_description"),
        F.col("action_geo_fullname").alias("event_location_full_name"),
        F.col("source").alias("event_source"),
    )

    lat_margin = 0.090  # 10 km in degrees latitude
    lon_margin = F.least(
        F.lit(lat_margin) / F.cos(F.radians(F.col("f_lat"))),
        F.lit(5.0),  # cap at 5° for polar/high-latitude cases
    )

    # Event midnight must be ≤ fire_time + 6h (timezone buffer) and ≥ fire_time - 48h.
    # Positive time_delta_h beyond the buffer is excluded — ACLED records actual event
    # date, not publication time, so events recorded well after the fire are spurious.
    bbox_joined = f.join(
        F.broadcast(g),
        (F.abs(F.col("f_lat") - F.col("g_lat")) <= lat_margin) &
        (F.abs(F.col("f_lon") - F.col("g_lon")) <= lon_margin) &
        (F.col("g_dt").cast("long") >= F.col("f_dt").cast("long") - _48H_S) &
        (F.col("g_dt").cast("long") <= F.col("f_dt").cast("long") + _6H_BUFFER),
    )

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

    with_time = with_dist.withColumn(
        "time_delta_h",
        ((F.col("g_dt").cast("long") - F.col("f_dt").cast("long")) / 3600.0).cast(FloatType()),
    )

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
        "firms_detection_id",
        "acled_event_id",
        F.col("f_lat").alias("fire_lat"),
        F.col("f_lon").alias("fire_lon"),
        F.col("f_dt").alias("fire_acq_datetime"),
        F.col("frp").alias("fire_frp"),
        F.col("confidence").alias("fire_confidence"),
        F.col("g_lat").alias("event_lat"),
        F.col("g_lon").alias("event_lon"),
        F.col("g_dt").alias("event_datetime"),
        "event_sub_event_type",
        "event_description",
        "event_location_full_name",
        "event_source",
        F.col("num_sources").alias("event_num_sources"),
        "distance_m",
        "time_delta_h",
        "score",
    )


# ── Delta DDL ───────────────────────────────────────────────────────────────────

def ensure_namespace() -> None:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
    # Volume that export_bronze.py uploads Parquet into.
    spark.sql(
        f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.bronze_inbound"
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_FIRMS_BRONZE} (
            id BIGINT, acq_datetime TIMESTAMP,
            latitude DOUBLE, longitude DOUBLE,
            bright_ti4 FLOAT, bright_ti5 FLOAT, frp FLOAT, scan FLOAT, track FLOAT,
            satellite STRING, confidence STRING, daynight STRING,
            type SMALLINT, version STRING, ingested_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_ACLED_BRONZE} (
            id BIGINT, global_event_id STRING,
            event_date DATE, event_datetime TIMESTAMP,
            event_type STRING, sub_event_type STRING, description STRING,
            num_sources INT,
            actor1_name STRING, actor2_name STRING,
            action_geo_fullname STRING, action_geo_country STRING,
            fatalities INT,
            latitude DOUBLE, longitude DOUBLE, source STRING, ingested_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_FIRMS_SILVER} (
            id BIGINT, acq_datetime TIMESTAMP,
            latitude DOUBLE, longitude DOUBLE,
            bright_ti4 FLOAT, bright_ti5 FLOAT, frp FLOAT, scan FLOAT, track FLOAT,
            satellite STRING, confidence STRING, daynight STRING,
            type SMALLINT, version STRING, ingested_at TIMESTAMP
        ) USING DELTA
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {T_CORR_GOLD} (
            firms_detection_id BIGINT, acled_event_id BIGINT,
            fire_lat DOUBLE, fire_lon DOUBLE, fire_acq_datetime TIMESTAMP,
            fire_frp FLOAT, fire_confidence STRING,
            event_lat DOUBLE, event_lon DOUBLE, event_datetime TIMESTAMP,
            event_sub_event_type STRING, event_description STRING,
            event_location_full_name STRING, event_source STRING, event_num_sources INT,
            distance_m FLOAT, time_delta_h FLOAT, score FLOAT,
            created_at TIMESTAMP
        ) USING DELTA
    """)


# ── Bronze load (Parquet on Volume → Delta, idempotent MERGE) ───────────────────

def merge_bronze(src: DataFrame, target: str, key: str) -> int:
    """
    MERGE Parquet rows into a bronze Delta table on a natural key, inserting only
    new rows. Mirrors the append-only / NOT-EXISTS bronze semantics of the Postgres
    ingest, and makes re-running the same 14-day window idempotent.
    """
    view = f"_src_{key}"
    src.createOrReplaceTempView(view)
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON t.{key} = s.{key}
        WHEN NOT MATCHED THEN INSERT *
    """)
    return spark.table(view).count()


def load_bronze() -> tuple[DataFrame, DataFrame]:
    firms_in = spark.read.parquet(P_FIRMS)
    acled_in = spark.read.parquet(P_ACLED)

    merge_bronze(firms_in, T_FIRMS_BRONZE, "id")
    merge_bronze(acled_in, T_ACLED_BRONZE, "global_event_id")

    # Return the Parquet data directly — export_bronze.py already handled date scoping
    # (rolling window or --all for archive runs). Reading back from the Delta table with
    # a time-window filter would anchor on the Delta's max timestamp and silently exclude
    # archive rows that predate the rolling window.
    return firms_in, acled_in


# ── Silver / gold writes ────────────────────────────────────────────────────────

_FIRMS_SILVER_COLS = [
    "id", "acq_datetime", "latitude", "longitude",
    "bright_ti4", "bright_ti5", "frp", "scan", "track",
    "satellite", "confidence", "daynight", "type", "version", "ingested_at",
]
# Pandas exports these as float64 (DoubleType); existing Delta table schema is FLOAT (FloatType).
# Delta saveAsTable rejects the mismatch even in overwrite mode — cast explicitly.
_FIRMS_FLOAT_COLS = ["bright_ti4", "bright_ti5", "frp", "scan", "track"]


def write_silver(firms_silver: DataFrame) -> int:
    df = firms_silver.select(_FIRMS_SILVER_COLS)
    for c in _FIRMS_FLOAT_COLS:
        df = df.withColumn(c, F.col(c).cast(FloatType()))
    df = df.withColumn("type", F.col("type").cast("smallint"))
    df.write.format("delta").mode("overwrite").saveAsTable(T_FIRMS_SILVER)
    return spark.table(T_FIRMS_SILVER).count()


def write_gold(candidates: DataFrame) -> None:
    """
    Historical-archive upsert: MERGE on (firms_detection_id, acled_event_id).
    Coordinates are denormalized into each gold row at insert time so the serving
    view never needs to join back to silver/bronze. This makes the archive immune
    to Postgres ID reuse: if bronze IDs are reassigned after a truncate, old gold
    rows still carry the correct coordinates from when they were scored.
    """
    staged = candidates.withColumn("created_at", F.current_timestamp())
    staged.createOrReplaceTempView("_corr_stage")
    spark.sql(f"""
        MERGE INTO {T_CORR_GOLD} AS t
        USING _corr_stage AS s
        ON  t.firms_detection_id = s.firms_detection_id
        AND t.acled_event_id     = s.acled_event_id
        WHEN NOT MATCHED THEN INSERT *
    """)


def build_serving_view() -> None:
    """
    Gold serving view for Power BI: confirmed correlations only (score_display ≥ 2),
    one row per ACLED event (best-scoring fire). Jitter spreads co-located events
    on a 10×10 grid at 0.001° steps (±0.0045°, ≈ ±500 m) — handles dense areas
    like Gaza with 40 events at the same lat/lon. Up to 100 events per coordinate
    are guaranteed unique (no birthday-paradox collisions vs. hash jitter).
    """
    spark.sql(f"""
        CREATE OR REPLACE VIEW {V_GOLD_MAP} AS
        WITH matched_ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY acled_event_id ORDER BY score DESC) AS _rn
            FROM {T_CORR_GOLD}
        ),
        best_matches AS (
            SELECT * FROM matched_ranked WHERE _rn = 1 AND score >= 0.002
        ),
        jittered AS (
            -- ROW_NUMBER jitter within each base coordinate (guaranteed unique; handles
            -- dense areas like Gaza with 40 events at the same lat/lon)
            SELECT *, CAST(ROW_NUMBER() OVER (PARTITION BY event_lat, event_lon ORDER BY acled_event_id) - 1 AS BIGINT) AS _rank
            FROM best_matches
        )
        SELECT
            firms_detection_id, fire_acq_datetime, fire_frp, fire_confidence, fire_lat, fire_lon,
            acled_event_id, event_datetime, event_sub_event_type,
            event_description, event_location_full_name, event_source, event_num_sources,
            event_lat, event_lon, distance_m, time_delta_h, score,
            score * 1000 AS score_display,
            event_lat + CAST(_rank % 10 AS DOUBLE) * 0.001 - 0.0045 AS map_lat,
            event_lon + CAST(_rank / 10 AS DOUBLE) * 0.001 - 0.0045 AS map_lon
        FROM jittered
    """)


# ── Verification (Spark-native; no PostGIS) ─────────────────────────────────────

def verify() -> None:
    print("\n-- Verification -------------------------------------------------------")
    silver = spark.table(T_FIRMS_SILVER)
    n_silver = silver.count()

    corr = spark.table(T_CORR_GOLD)
    n_corr = corr.count()

    # Self-proximity check: replaces the PostGIS ST_DWithin audit in spark_pipeline.py.
    # Reuses the exact dedup candidate logic — must be 0 if dedup is correct.
    dup_pairs = _dominated_ids(silver.select("id", "latitude", "longitude", "acq_datetime")).count()

    stats = corr.agg(
        F.min("score").alias("mn"),
        F.avg("score").alias("av"),
        F.max("score").alias("mx"),
    ).first()

    print(f"1. firms_silver rows:          {n_silver:,}")
    dup_flag = "  <-- DEDUP BUG" if dup_pairs > 0 else ""
    print(f"2. firms_silver dup pairs:     {dup_pairs}{dup_flag}")
    print(f"3. fire_event_correlations:    {n_corr:,}")
    if stats and stats["mn"] is not None:
        print(
            f"4. Score (raw): min={stats['mn']:.4f}  avg={stats['av']:.4f}  max={stats['mx']:.4f}"
            f"  |  display (×1000): min={stats['mn']*1000:.1f}  avg={stats['av']*1000:.1f}  max={stats['mx']*1000:.1f}"
        )
    else:
        print("4. Score: no rows")

    top3 = (
        corr
        .orderBy(F.col("score").desc())
        .select(
            "score", "distance_m", "time_delta_h",
            "fire_lat", "fire_lon", "fire_frp", "fire_confidence",
            "event_location_full_name", "event_sub_event_type",
        )
        .limit(3)
        .collect()
    )
    if top3:
        print("\nTop 3 by score:")
        for r in top3:
            print(
                f"  score={r['score']:.4f}  dist={r['distance_m']:.0f}m  dt={r['time_delta_h']:+.1f}h  "
                f"({r['fire_lat']:.3f},{r['fire_lon']:.3f})  frp={r['fire_frp'] or 0:.1f}  conf={r['fire_confidence']}  "
                f"loc={r['event_location_full_name']}  sub_type={r['event_sub_event_type']}"
            )

    # Contract assertions — fail the job task on violation (mirrors _validate_pipeline).
    if n_silver == 0:
        raise SystemExit("Validation failed: firms_silver is empty")
    if dup_pairs > 0:
        raise SystemExit(f"Validation failed: {dup_pairs} residual dup pairs in firms_silver")
    if stats and stats["mn"] is not None and (stats["mn"] < 0 or stats["mx"] > 1):
        raise SystemExit("Validation failed: score outside [0, 1]")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Phase 4 Databricks pipeline  |  catalog={CATALOG} schema={SCHEMA}")
    print(f"  source: Volume Parquet at {VOLUME_INBOUND} (written by firms_ingest.py / acled_ingest.py)")

    ensure_namespace()

    print("\nLoading bronze from Volume Parquet (MERGE into Delta)...")
    firms_raw, acled_raw = load_bronze()
    n_firms_raw = firms_raw.count()
    n_acled_raw = acled_raw.count()
    print(f"  firms_detections : {n_firms_raw:,} rows (14-day window)")
    print(f"  acled_events     : {n_acled_raw:,} rows (14-day window)")

    print("\nApplying silver transform (confidence filter + satellite-pass dedup)...")
    firms_silver = satellite_pass_dedup(confidence_filter(firms_raw))
    n_silver = firms_silver.count()
    print(f"  {n_firms_raw:,} bronze  ->  {n_silver:,} silver  ({n_firms_raw - n_silver:,} removed)")

    print("\nWriting firms_silver (overwrite)...")
    print(f"  {write_silver(firms_silver):,} rows written")

    # Read the persisted silver back so the candidate join reuses the materialized
    # dedup result instead of recomputing the grid-bin explode + anti-join. Serverless
    # has no .cache() (it triggers PERSIST TABLE), so the silver Delta table is the
    # materialization point — the analogue of spark_pipeline.py's firms_silver.cache().
    firms_silver = spark.table(T_FIRMS_SILVER)

    print("\nComputing FIRMS x ACLED candidates (10 km / event_midnight ±48 h / +6 h buffer)...")
    candidates = compute_candidates(firms_silver, acled_raw)
    n_candidates = candidates.count()
    print(f"  {n_candidates:,} candidate pairs")

    print("Upserting fire_event_correlations (Delta MERGE)...")
    write_gold(candidates)

    print("Building gold_fire_event_map serving view...")
    build_serving_view()

    verify()
    print("\nDone.")


if __name__ == "__main__":
    main()
