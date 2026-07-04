"""
Spark-transform tests for pipeline/spark_pipeline_databricks.py.

Runs the real dedup and correlation logic on a local SparkSession with tiny
synthetic frames. Marked `spark` — requires pyspark + Java (runs in CI; skipped
automatically when a local session cannot start).
"""

import math
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import types as T  # noqa: E402

pytestmark = pytest.mark.spark  # marker so that "no spark" skips these tests

T0 = datetime(2024, 8, 18, 10, 0, 0)


def haversine_ref(lat1, lon1, lat2, lon2):
    """Reference great-circle distance (same sphere radius as the pipeline)."""
    r = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return 2 * r * math.asin(math.sqrt(min(a, 1.0)))


@pytest.fixture(scope="module")
def spark():
    import os
    import sys

    from pyspark.sql import SparkSession

    # Point Spark's Python workers at the interpreter running the tests
    # (avoids "Python worker failed to connect back" when python isn't on PATH).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("strikematch-tests")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.host", "127.0.0.1")
            .getOrCreate()
        )
        # Smoke check: local-mode Python workers are broken on some Windows
        # setups (firewall/loopback). Skip the module rather than fail.
        session.createDataFrame([(1,)], "x int").collect()
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"local Spark session unavailable: {exc}")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def sp(spark):
    # Import after the local session exists: the module-level getOrCreate()
    # then attaches to it instead of trying to build a Databricks session.
    from pipeline import spark_pipeline_databricks as sp

    return sp


_FIRMS_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("acq_datetime", T.TimestampType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("frp", T.DoubleType()),
        T.StructField("confidence", T.StringType()),
    ]
)

_ACLED_SCHEMA = T.StructType(
    [
        T.StructField("id", T.LongType()),
        T.StructField("event_datetime", T.TimestampType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("num_sources", T.LongType()),
        T.StructField("sub_event_type", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("action_geo_fullname", T.StringType()),
        T.StructField("source", T.StringType()),
    ]
)


def _firms_df(spark, rows):
    return spark.createDataFrame(rows, schema=_FIRMS_SCHEMA)


def _acled_df(spark, rows):
    return spark.createDataFrame(rows, schema=_ACLED_SCHEMA)


def _acled_row(id_, dt, lat, lon, num_sources=3):
    return (id_, dt, lat, lon, num_sources, "Air/drone strike", "desc", "Testville", "A; B; C")


# ── haversine_col ───────────────────────────────────────────────────────────────


def test_haversine_one_degree_latitude(spark, sp):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(0.0, 0.0, 1.0, 0.0)], "lat1 double, lon1 double, lat2 double, lon2 double")
    dist = df.select(sp.haversine_col(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")).alias("d")).first()[
        "d"
    ]
    assert dist == pytest.approx(111_194.93, rel=1e-4)  # 2πR/360


def test_haversine_same_point_is_zero(spark, sp):
    from pyspark.sql import functions as F

    df = spark.createDataFrame([(46.7, 41.7, 46.7, 41.7)], "lat1 double, lon1 double, lat2 double, lon2 double")
    dist = df.select(sp.haversine_col(F.col("lat1"), F.col("lon1"), F.col("lat2"), F.col("lon2")).alias("d")).first()[
        "d"
    ]
    assert dist == pytest.approx(0.0, abs=1e-6)


# ── confidence_filter ───────────────────────────────────────────────────────────


def test_confidence_filter_drops_low(spark, sp):
    df = _firms_df(
        spark,
        [
            (1, T0, 50.0, 30.0, 10.0, "h"),
            (2, T0, 50.0, 30.0, 10.0, "n"),
            (3, T0, 50.0, 30.0, 10.0, "l"),
            (4, T0, 50.0, 30.0, 10.0, "low"),
        ],
    )
    kept = {r["id"] for r in sp.confidence_filter(df).collect()}
    assert kept == {1, 2}


# ── satellite_pass_dedup ────────────────────────────────────────────────────────


def test_dedup_transitive_chain_keeps_one(spark, sp):
    # A–B and B–C are each within 1 km / 6 h, but A–C is ~1.2 km apart.
    # Transitive dedup must collapse the whole chain to the highest id (C).
    df = _firms_df(
        spark,
        [
            (1, T0, 50.0000, 30.0, 10.0, "h"),  # A
            (2, T0 + timedelta(hours=1), 50.0054, 30.0, 10.0, "h"),  # B, ~600 m from A
            (3, T0 + timedelta(hours=2), 50.0108, 30.0, 10.0, "h"),  # C, ~600 m from B
            (4, T0, 55.0, 40.0, 10.0, "h"),  # D, far away
        ],
    ).select("id", "latitude", "longitude", "acq_datetime", "frp", "confidence")
    kept = {r["id"] for r in sp.satellite_pass_dedup(df).collect()}
    assert kept == {3, 4}


def test_dedup_keeps_same_spot_outside_time_window(spark, sp):
    # Same coordinates but 7 h apart - distinct thermal events, both kept.
    df = _firms_df(
        spark,
        [
            (1, T0, 50.0, 30.0, 10.0, "h"),
            (2, T0 + timedelta(hours=7), 50.0, 30.0, 10.0, "h"),
        ],
    ).select("id", "latitude", "longitude", "acq_datetime", "frp", "confidence")
    kept = {r["id"] for r in sp.satellite_pass_dedup(df).collect()}
    assert kept == {1, 2}


# ── compute_candidates ──────────────────────────────────────────────────────────


def test_candidate_score_matches_formula(spark, sp):
    fire_lat, fire_lon = 46.70, 41.70
    event_lat, event_lon = 46.74, 41.70  # ~4.4 km north
    event_midnight = datetime(2024, 8, 18, 0, 0, 0)  # 10 h before the fire

    firms = _firms_df(spark, [(1, T0, fire_lat, fire_lon, 43.4, "h")])
    acled = _acled_df(spark, [_acled_row(100, event_midnight, event_lat, event_lon, num_sources=3)])

    rows = sp.compute_candidates(firms, acled).collect()
    assert len(rows) == 1
    row = rows[0]

    dist = haversine_ref(fire_lat, fire_lon, event_lat, event_lon)
    delta_h = -10.0  # event midnight - fire time
    expected = (
        min(43.4 / 300.0, 1.0)  # frp_score
        * 1.0  # conf_factor (high)
        * min(3 / 3.0, 1.0)  # source_credibility
        * math.sqrt(1.0 - dist / 10_000.0)  # proximity_decay
        * (1.0 - abs(delta_h) / 54.0)  # temporal_decay
    )
    assert row["distance_m"] == pytest.approx(dist, rel=1e-4)
    assert row["time_delta_h"] == pytest.approx(delta_h, abs=1e-6)
    assert row["score"] == pytest.approx(expected, rel=1e-4)


def test_candidate_nominal_confidence_factor(spark, sp):
    firms_h = _firms_df(spark, [(1, T0, 46.70, 41.70, 30.0, "h")])
    firms_n = _firms_df(spark, [(1, T0, 46.70, 41.70, 30.0, "n")])
    acled = _acled_df(spark, [_acled_row(100, datetime(2024, 8, 18), 46.71, 41.70)])

    score_h = sp.compute_candidates(firms_h, acled).first()["score"]
    score_n = sp.compute_candidates(firms_n, acled).first()["score"]
    assert score_n == pytest.approx(0.8 * score_h, rel=1e-4)


def test_candidate_excludes_low_frp(spark, sp):
    firms = _firms_df(spark, [(1, T0, 46.70, 41.70, 0.5, "h")])  # below 1.0 MW floor
    acled = _acled_df(spark, [_acled_row(100, datetime(2024, 8, 18), 46.70, 41.70)])
    assert sp.compute_candidates(firms, acled).count() == 0


def test_candidate_excludes_beyond_10km(spark, sp):
    firms = _firms_df(spark, [(1, T0, 46.70, 41.70, 30.0, "h")])
    acled = _acled_df(spark, [_acled_row(100, datetime(2024, 8, 18), 46.81, 41.70)])  # ~12 km
    assert sp.compute_candidates(firms, acled).count() == 0


def test_candidate_temporal_window_edges(spark, sp):
    firms = _firms_df(spark, [(1, T0, 46.70, 41.70, 30.0, "h")])

    inside_buffer = _acled_df(
        spark, [_acled_row(100, T0 + timedelta(hours=5), 46.70, 41.70)]
    )  # event 5 hours after fire
    beyond_buffer = _acled_df(
        spark, [_acled_row(101, T0 + timedelta(hours=7), 46.70, 41.70)]
    )  # event 7 hours after fire
    beyond_lookback = _acled_df(
        spark, [_acled_row(102, T0 - timedelta(hours=49), 46.70, 41.70)]
    )  # event 49 hours before fire

    assert sp.compute_candidates(firms, inside_buffer).count() == 1
    assert sp.compute_candidates(firms, beyond_buffer).count() == 0
    assert sp.compute_candidates(firms, beyond_lookback).count() == 0
