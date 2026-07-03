"""Unit tests for the pure logic in pipeline/export_data.py."""

from datetime import datetime

from pipeline.export_data import _build_metadata, _round


def test_round_floats_to_five_digits():
    assert _round(46.70281234567) == 46.70281


def test_round_passes_through_non_floats():
    assert _round("Proletarsk") == "Proletarsk"
    assert _round(3) == 3
    assert _round(None) is None


def test_build_metadata_delta():
    meta = _build_metadata(total=24397, prev_total=24391)
    assert meta["total_events"] == 24397
    assert meta["events_added_last_run"] == 6


def test_build_metadata_first_run():
    meta = _build_metadata(total=100, prev_total=0)
    assert meta["events_added_last_run"] == 100


def test_build_metadata_timestamp_is_iso_utc():
    meta = _build_metadata(total=1, prev_total=1)
    parsed = datetime.fromisoformat(meta["last_run_utc"])
    assert parsed.tzinfo is not None
