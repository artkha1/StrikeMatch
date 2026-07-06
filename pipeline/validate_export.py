"""
Data-quality gate for the exported dashboard dataset (dashboard/data/*.json).

Runs a Great Expectations suite over events.json plus project-specific pandas
checks (theater bounding boxes, ground-truth benchmark events, metadata
consistency). Exits non-zero on any failure so the Airflow `validate_export`
task blocks `push_data` - bad data never reaches GitHub Pages.

Runs in three places with the same checks:
  * Airflow task between export_data and push_data
  * CI (data-quality.yml) on any push that touches dashboard/data/**
  * locally / pytest via tests/test_data_quality.py

If great_expectations is not importable (e.g. it cannot be installed under the
Airflow constraint file), the same column checks run as plain pandas asserts.
"""

import json
import os
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

# firms_ingest requires FIRMS_MAP_KEY at import; validation only needs its bboxes.
os.environ.setdefault("FIRMS_MAP_KEY", "unused-for-validation")
from pipeline.acled_ingest import STRIKE_SUBTYPES  # noqa: E402
from pipeline.firms_ingest import COUNTRY_BBOXES  # noqa: E402

EVENTS_PATH = _ROOT / "dashboard" / "data" / "events.json"
METADATA_PATH = _ROOT / "dashboard" / "data" / "metadata.json"

REQUIRED_NON_NULL = [
    "map_lat",
    "map_lon",
    "score_display",
    "fire_frp",
    "fire_confidence",
    "event_datetime",
    "event_location_full_name",
]
# (column, min, max) - match definition + serving thresholds from CLAUDE.md
BOUNDS = [
    ("score_display", 2.0, 1000.0),  # serving view keeps score_display >= 2 only
    ("distance_m", 0.0, 10_000.0),  # 10 km proximity gate
    ("time_delta_h", -48.0, 6.0),  # event_midnight in [fire - 48 h, fire + 6 h]
    ("fire_frp", 1.0, None),  # sub-thermal fires excluded at join time
]
# Jitter spreads points up to ±0.0045°; allow a little slack past a country bbox edge.
BBOX_TOLERANCE_DEG = 0.01

# Benchmark events that any scoring/threshold change must preserve
# (see "Validation benchmarks" in CLAUDE.md). Thresholds sit below the
# current scores to leave headroom for minor recalibration.
GROUND_TRUTH = [
    # (label, event date, location substring, min score_display)
    ("Proletarsk oil depot 2024-08-18", "2024-08-18", "Proletarsk", 40.0),
    ("Dyagilevo airfield (Ryazan) 2025-06-01", "2025-06-01", "Ryazan", 2.5),
    ("Lyudinovo oil terminal 2025-01-17", "2025-01-17", "Lyudinovo", 2.5),
]


def load_events(path: Path = EVENTS_PATH) -> pd.DataFrame:
    """Load events.json in either format: array of objects, or columnar
    {"columns": [...], "rows": [[...], ...]}."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "columns" in data:
        return pd.DataFrame(data["rows"], columns=data["columns"])
    return pd.DataFrame(data)


# ── Column expectations (Great Expectations, pandas fallback) ───────────────────


def _column_checks_ge(df: pd.DataFrame, has_ids: bool) -> list[str]:
    import great_expectations as gx
    import great_expectations.expectations as gxe

    # The context must exist before the suite is built - in GE 1.x,
    # ExpectationSuite.add_expectation() requires an active project context.
    context = gx.get_context(
        mode="ephemeral"
    )  # TODO: change to file to generate test docs, properly set up to get full benefit of GE
    batch = (
        context.data_sources.add_pandas("events")
        .add_dataframe_asset("events")
        .add_batch_definition_whole_dataframe("events")
        .get_batch(batch_parameters={"dataframe": df})
    )

    suite = gx.ExpectationSuite(name="gold_export")
    if has_ids:
        suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="global_event_id"))  # provided by ACLED
        # TODO: not really a reason to do this, this just verifying ACLED isn't lying
        # In a future release, the above check could be dropped anf global_event_id doesn't have to be exported altogether, remove the BRONZE joining in view serving in spark job
        suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="acled_event_id"))  # hashed global_event_id
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="global_event_id"))
    for col in REQUIRED_NON_NULL:
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column=col))
    for col, lo, hi in BOUNDS:
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column=col, min_value=lo, max_value=hi))
    suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="fire_confidence", value_set=["h", "n"]))
    suite.add_expectation(
        gxe.ExpectColumnValuesToBeInSet(column="event_sub_event_type", value_set=list(STRIKE_SUBTYPES))
    )

    results = batch.validate(suite)

    failures = []
    for r in results.results:
        if not r.success:
            cfg = r.expectation_config
            unexpected = r.result.get("unexpected_count", "?")
            failures.append(f"{cfg.type}({cfg.kwargs.get('column')}): {unexpected} unexpected values")
            # Example: expect_column_values_to_be_in_set(fire_confidence): 1 unexpected values
    return failures


def _column_checks_pandas(df: pd.DataFrame, has_ids: bool) -> list[str]:
    failures = []
    if has_ids:
        for col in ("global_event_id", "acled_event_id"):
            dupes = df[col].duplicated().sum()
            if dupes:
                failures.append(f"unique({col}): {dupes} duplicate values")
        if df["global_event_id"].isna().any():
            failures.append(f"not_null(global_event_id): {df['global_event_id'].isna().sum()} nulls")
    for col in REQUIRED_NON_NULL:
        n = df[col].isna().sum()
        if n:
            failures.append(f"not_null({col}): {n} nulls")
    for col, lo, hi in BOUNDS:
        bad = ~df[col].between(lo if lo is not None else -float("inf"), hi if hi is not None else float("inf"))
        if bad.any():
            failures.append(f"between({col}, {lo}, {hi}): {int(bad.sum())} out-of-range values")
    for col, allowed in (("fire_confidence", {"h", "n"}), ("event_sub_event_type", STRIKE_SUBTYPES)):
        bad = ~df[col].isin(allowed)
        if bad.any():
            failures.append(f"in_set({col}): {int(bad.sum())} unexpected values")
    return failures


# ── Project-specific checks (plain pandas) ──────────────────────────────────────


def _check_theater_bboxes(df: pd.DataFrame) -> list[str]:
    """Every map point must fall inside at least one conflict-country bbox."""
    inside = pd.Series(False, index=df.index)
    t = BBOX_TOLERANCE_DEG
    for mn_lat, mx_lat, mn_lon, mx_lon in COUNTRY_BBOXES.values():
        inside |= (
            df["map_lat"].between(mn_lat - t, mx_lat + t)
            & df["map_lon"].between(
                mn_lon - t, mx_lon + t
            )  # at each iteration, rows corresponding to that bbox are marked True in `inside`
        )
    n_outside = int((~inside).sum())
    if n_outside:
        sample = df.loc[~inside, ["map_lat", "map_lon", "event_location_full_name"]].head(3)
        return [f"theater_bboxes: {n_outside} points outside all conflict-country bboxes, e.g.\n{sample}"]
    return []


def _check_ground_truth(df: pd.DataFrame) -> list[str]:
    failures = []
    for label, day, loc_substr, min_score in GROUND_TRUTH:
        rows = df[
            df["event_datetime"].astype(str).str.startswith(day)
            & df["event_location_full_name"].astype(str).str.contains(loc_substr, case=False)
        ]
        if rows.empty:
            failures.append(f"ground_truth({label}): event missing from export")
        elif rows["score_display"].max() < min_score:
            failures.append(
                f"ground_truth({label}): best score {rows['score_display'].max():.1f} "
                f"below expected minimum {min_score}"
            )
    return failures


def _check_metadata(df: pd.DataFrame, metadata_path: Path) -> list[str]:
    meta = json.loads(metadata_path.read_text(encoding="utf-8"))
    if meta.get("total_events") != len(df):
        return [f"metadata: total_events={meta.get('total_events')} but events.json has {len(df)} rows"]
    return []


# ── Entry points ────────────────────────────────────────────────────────────────


def run(events_path: Path = EVENTS_PATH, metadata_path: Path = METADATA_PATH) -> list[str]:
    """Run all checks; return a list of failure descriptions (empty = healthy)."""
    df = load_events(events_path)
    print(f"Validating {events_path.name}: {len(df):,} rows, {len(df.columns)} columns")

    has_ids = "global_event_id" in df.columns
    if not has_ids:
        print("NOTE: ID columns not present yet (pre-dates the view change), uniqueness checks skipped.")

    try:
        failures = _column_checks_ge(df, has_ids)
        print("Column checks: Great Expectations suite")
    except ImportError:
        failures = _column_checks_pandas(df, has_ids)
        print("Column checks: pandas fallback (great_expectations not installed)")

    failures += _check_theater_bboxes(df)
    failures += _check_ground_truth(df)
    failures += _check_metadata(df, metadata_path)
    return failures


def main() -> None:
    failures = run()
    if failures:
        print(f"\nFAILED - {len(failures)} check(s):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("All data-quality checks passed.")


if __name__ == "__main__":
    main()
