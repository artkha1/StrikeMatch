"""Unit tests for the pure logic in pipeline/acled_ingest.py (no network, no Databricks)."""

from datetime import datetime, timezone

from pipeline.acled_ingest import _DELTA_COL_ORDER, _build_dataframe, _parse_row


def _raw_event(**overrides) -> dict:
    """A realistic ACLED API row for a strike event."""
    row = {
        "event_id_cnty": "RUS12345",
        "event_date": "2024-08-18",
        "event_type": "Explosions/Remote violence",
        "sub_event_type": "Air/drone strike",
        "actor1": "Military Forces of Ukraine (2019-)",
        "actor2": "Military Forces of Russia (2000-)",
        "country": "Russia",
        "location": "Proletarsk",
        "latitude": "46.7028",
        "longitude": "41.7284",
        "geo_precision": "1",
        "source": "Astra; Interfax; Ministry of Defence of Russia",
        "notes": "On 18 August 2024, Ukrainian drones struck an oil depot in Proletarsk.",
        "fatalities": "0",
    }
    row.update(overrides)
    return row


# ── column mapping (per the ACLED contract in CLAUDE.md) ────────────────────────

def test_parse_row_maps_columns():
    t = _parse_row(_raw_event())
    assert t is not None
    assert t[0] == "RUS12345"                      # global_event_id ← event_id_cnty
    assert t[2] == datetime(2024, 8, 18, tzinfo=timezone.utc)  # event_datetime @ 00:00 UTC
    assert t[4] == "Air/drone strike"              # sub_event_type
    assert t[5].startswith("On 18 August 2024")    # description ← notes
    assert t[9] == "Proletarsk"                    # action_geo_fullname ← location
    assert t[10] == "Russia"                       # action_geo_country
    assert t[11] == 0                              # fatalities
    assert (t[12], t[13]) == (46.7028, 41.7284)    # latitude/longitude


def test_parse_row_counts_semicolon_split_sources():
    t = _parse_row(_raw_event(source="Astra; Interfax; Ministry of Defence of Russia"))
    assert t[6] == 3
    assert t[14] == "Astra; Interfax; Ministry of Defence of Russia"


def test_parse_row_empty_source_counts_as_one():
    t = _parse_row(_raw_event(source=""))
    assert t[6] == 1
    assert t[14] is None


# ── filters ─────────────────────────────────────────────────────────────────────

def test_parse_row_rejects_geo_precision_3():
    assert _parse_row(_raw_event(geo_precision="3")) is None


def test_parse_row_accepts_geo_precision_1_and_2():
    assert _parse_row(_raw_event(geo_precision="1")) is not None
    assert _parse_row(_raw_event(geo_precision="2")) is not None


def test_parse_row_rejects_non_strike_sub_event_type():
    assert _parse_row(_raw_event(sub_event_type="Grenade")) is None
    assert _parse_row(_raw_event(sub_event_type="Remote explosive/landmine/IED")) is None


def test_parse_row_accepts_both_strike_subtypes():
    assert _parse_row(_raw_event(sub_event_type="Air/drone strike")) is not None
    assert _parse_row(_raw_event(sub_event_type="Shelling/artillery/missile attack")) is not None


def test_parse_row_rejects_bad_coordinates():
    assert _parse_row(_raw_event(latitude="not-a-number")) is None
    row = _raw_event()
    del row["longitude"]
    assert _parse_row(row) is None


# ── _build_dataframe ────────────────────────────────────────────────────────────

def test_build_dataframe_schema_and_stable_ids():
    rows = [_parse_row(_raw_event()), _parse_row(_raw_event(event_id_cnty="UKR99999"))]
    df = _build_dataframe(rows)
    assert list(df.columns) == _DELTA_COL_ORDER
    assert df["id"].nunique() == 2

    df_again = _build_dataframe(rows)
    assert df["id"].tolist() == df_again["id"].tolist()  # hash of global_event_id is stable
