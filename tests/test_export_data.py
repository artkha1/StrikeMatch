"""Unit tests for the pure logic in pipeline/export_data.py."""

from datetime import datetime

from pipeline.export_data import _build_metadata, _compact


def test_compact_rounds_coordinates_to_five_digits():
    assert _compact("map_lat", 46.70281234567) == 46.70281


def test_compact_rounds_display_values_to_one_digit():
    assert _compact("score_display", 72.5123) == 72.5
    assert _compact("fire_frp", 43.4567) == 43.5
    assert _compact("time_delta_h", -10.0001) == -10.0


def test_compact_rounds_distance_to_int():
    assert _compact("distance_m", 4437.8) == 4438


def test_compact_passes_through_non_floats():
    assert _compact("event_location_full_name", "Proletarsk") == "Proletarsk"
    assert _compact("event_num_sources", 3) == 3
    assert _compact("event_description", None) is None


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
