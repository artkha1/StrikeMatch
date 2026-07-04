"""Unit tests for the pure logic in pipeline/firms_ingest.py (no network, no Databricks)."""

from datetime import date, datetime, timezone

from pipeline.firms_ingest import (
    _DELTA_COL_ORDER,
    _build_dataframe,
    _date_chunks,
    _f,
    _i,
    _in_conflict_zone,
    parse_row,
)


def _raw_row(**overrides) -> dict:
    """A realistic FIRMS VIIRS CSV row (as returned by csv.DictReader)."""
    row = {
        "latitude": "50.4501",
        "longitude": "30.5234",
        "bright_ti4": "330.5",
        "bright_ti5": "290.1",
        "scan": "0.39",
        "track": "0.36",
        "acq_date": "2024-08-18",
        "acq_time": "0055",
        "satellite": "N20",
        "confidence": "h",
        "version": "2.0NRT",
        "bright_ti4_2": "",
        "frp": "43.4",
        "daynight": "N",
        "type": "0",
    }
    row.update(overrides)
    return row


# ── _in_conflict_zone ───────────────────────────────────────────────────────────


def test_in_conflict_zone_ukraine():
    assert _in_conflict_zone(50.45, 30.52)  # Kyiv


def test_in_conflict_zone_gaza():
    assert _in_conflict_zone(31.4, 34.4)  # Gaza Strip


def test_outside_conflict_zone_athens():
    # Greece sits inside the "Eastern Europe / Russia" regional bbox but is not
    # a conflict country, the per-country filter must reject it.
    assert not _in_conflict_zone(37.98, 23.72)


def test_outside_conflict_zone_ocean():
    assert not _in_conflict_zone(30.0, -40.0)  # middle of North Atlantic Ocean


# ── _date_chunks ────────────────────────────────────────────────────────────────


def test_date_chunks_single_day():
    assert _date_chunks(date(2024, 8, 18), date(2024, 8, 18), 5) == [(date(2024, 8, 18), 1)]


def test_date_chunks_splits_at_max():
    chunks = _date_chunks(date(2022, 2, 1), date(2022, 2, 12), 5)
    assert chunks == [
        (date(2022, 2, 1), 5),
        (date(2022, 2, 6), 5),
        (date(2022, 2, 11), 2),
    ]


def test_date_chunks_cover_range_contiguously():
    start, end = date(2022, 2, 24), date(2022, 3, 31)
    chunks = _date_chunks(start, end, 5)
    assert sum(n for _, n in chunks) == (end - start).days + 1
    assert chunks[0][0] == start
    for (d1, n1), (d2, _) in zip(chunks, chunks[1:]):
        assert (
            (d2 - d1).days == n1
        )  # difference in dates between consecutive chunks equals the number of days in the first chunk of each pair


# ── field parsing helpers ───────────────────────────────────────────────────────


def test_f_and_i_parse_values():
    assert _f({"frp": "43.4"}, "frp") == 43.4
    assert _i({"type": "0"}, "type") == 0


def test_f_and_i_empty_or_missing_return_none():
    assert _f({"frp": ""}, "frp") is None
    assert _f({}, "frp") is None
    assert _i({"type": "  "}, "type") is None


# ── parse_row ───────────────────────────────────────────────────────────────────


def test_parse_row_valid():
    t = parse_row(_raw_row())
    assert t is not None
    acq_dt, lat, lon = t[0], t[1], t[2]
    assert acq_dt == datetime(2024, 8, 18, 0, 55, tzinfo=timezone.utc)
    assert (lat, lon) == (50.4501, 30.5234)
    assert t[5] == 43.4  # frp
    assert t[8] == "N20"  # satellite
    assert t[9] == "h"  # confidence
    assert t[10] == "N"  # daynight
    assert t[11] == 0  # type


def test_parse_row_pads_short_acq_time():
    t = parse_row(_raw_row(acq_time="55"))
    assert t is not None
    assert t[0] == datetime(2024, 8, 18, 0, 55, tzinfo=timezone.utc)


def test_parse_row_missing_coordinate_returns_none():
    row = _raw_row()
    del row["latitude"]
    assert parse_row(row) is None


def test_parse_row_bad_date_returns_none():
    assert parse_row(_raw_row(acq_date="not-a-date")) is None


# ── _build_dataframe ────────────────────────────────────────────────────────────


def test_build_dataframe_schema_and_ids():
    rows = [parse_row(_raw_row()), parse_row(_raw_row(latitude="48.0", longitude="37.8"))]
    df = _build_dataframe(rows)
    assert list(df.columns) == _DELTA_COL_ORDER
    assert df["id"].dtype == "int64"
    assert df["id"].nunique() == 2


def test_build_dataframe_hash_ids_are_stable():
    rows = [parse_row(_raw_row())]
    ids_a = _build_dataframe(rows)["id"].tolist()
    ids_b = _build_dataframe(rows)["id"].tolist()
    assert ids_a == ids_b  # idempotent Delta MERGE depends on this
