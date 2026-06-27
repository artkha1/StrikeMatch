# Phase 1 Specification: FIRMS Ingestion → PostGIS

## Data Source

**Product**: NASA FIRMS VIIRS I-Band 375m Near-Real-Time (NRT) only.
- MODIS and VIIRS 750m are excluded. Single schema, no cross-product union needed.
- NRT data lags real-time by roughly 3 hours. Preliminary accuracy; may be revised in standard product but we are not backfilling corrections.

## Schema

One table: `firms_detections`

Columns retained from the FIRMS VIIRS 375m NRT CSV:

| Column | Type | Notes |
|---|---|---|
| `id` | BIGSERIAL PK | |
| `acq_datetime` | TIMESTAMPTZ | Combined `acq_date` + `acq_time` (UTC) |
| `geom` | GEOGRAPHY(POINT, 4326) | Indexed, derived from lat/lon |
| `latitude` | DOUBLE PRECISION | Kept alongside geom for cheap export |
| `longitude` | DOUBLE PRECISION | |
| `bright_ti4` | REAL | Brightness temp, channel I-4 (fire pixel) |
| `bright_ti5` | REAL | Brightness temp, channel I-5 (background) |
| `frp` | REAL | Fire Radiative Power in MW |
| `scan` | REAL | Along-scan pixel size in km |
| `track` | REAL | Along-track pixel size in km |
| `satellite` | VARCHAR(10) | N=Suomi-NPP, 1=NOAA-20, 2=NOAA-21 |
| `confidence` | VARCHAR(10) | Only `nominal` or `high` post-filter |
| `daynight` | CHAR(1) | D or N |
| `type` | SMALLINT | 0=vegetation, 1=volcano, 2=other land, 3=offshore |
| `version` | VARCHAR(10) | FIRMS collection version |
| `ingested_at` | TIMESTAMPTZ | DEFAULT NOW(), set at ingest time |

## False Positive Filtering

**Strategy**: Drop `low` confidence detections at ingest — they are never written to the DB.

- Rows where `confidence = 'low'` are discarded before INSERT.
- `nominal` and `high` are kept as-is.
- No FRP threshold is applied at this stage; FRP is stored for future query-time filtering.

Rationale: agricultural burns and gas flares skew heavily toward low confidence. Dropping them permanently is acceptable because Phase 1 is correlation prep, not forensic archiving.

## Deduplication

**Strategy**: Spatial proximity + time window, enforced at ingest time.

- **Radius**: 1 km
- **Time window**: 6 hours
- Before inserting a new detection, check whether any existing row falls within 1 km AND has `acq_datetime` within ±6 hours of the candidate.
- If a match exists, skip the candidate (no upsert, no update — silent skip).
- This uses a PostGIS `ST_DWithin` query against a spatial index on `geom`.

This matches FIRMS's own NRT clustering guidance and catches the same fire appearing in consecutive orbital passes without merging genuinely distinct nearby fires in most real-world scenarios.

## Cadence & Backfill

**Cadence**: Manual trigger during Phase 1. No scheduler, no cron. Cadence to be decided before Phase 2.

**Initial backfill**: 7-day rolling window from the time of first run.

- Each manual run pulls the FIRMS NRT API endpoint for the past 7 days and applies the filter + dedup logic above.
- Subsequent runs on the same window are safe to re-run (dedup prevents double inserts).

## Verification

A successful ingestion run must pass all three checks:

1. **Row count**: Table has at least one row; print total count after run.
2. **Bounding box**: `ST_Extent(geom)` must fall within plausible global fire bounds (roughly ±70° latitude, ±180° longitude). Catches coordinate transform errors.
3. **Duplicate audit**: Query for detections within 1 km / 6 hours of each other that share the same `satellite` value. Count should be zero (or near-zero for edge cases at the spatial boundary). Print the count — nonzero is a signal the dedup logic has a bug.

Print all three results to stdout at the end of every ingest run.

## Out of Scope for Phase 1

- Airflow, Spark, Kafka, Databricks
- GDELT ingestion or spatial joins
- Confidence or FRP adjustments post-ingest
- Automated cadence / scheduling
- MODIS or VIIRS 750m data

---

# Phase 2 Specification: GDELT Ingestion → Correlation with FIRMS

## Decisions recorded (interviewed 2026-06-20)

| Topic | Decision |
|---|---|
| GDELT CAMEO scope | Strike-specific: codes 190x, 193x, 195x only |
| Minimum geo precision | City-level only — ActionGeo_Type 3 (US city) or 4 (world city) |
| Match time window | GDELT event published −72 h to +12 h relative to `firms.acq_datetime` |
| One-to-many handling | Many-to-many junction table; every valid pair stored |
| Infrastructure | FK column reserved in Phase 2 schema; populated in Phase 3 |
| Validation set | Deferred — must define before running on live production data |

---

## Data Source

**Feed**: GDELT 2.0 Events table (`export.CSV`). GKG and Mentions tables are excluded.

GDELT publishes a new 15-minute export every 15 minutes. For the 7-day backfill cadence
that mirrors Phase 1, ingest the daily merged files:

```
https://data.gdeltproject.org/gdeltv2/YYYYMMDD000000.export.CSV.zip
```

One file per calendar day; 7 requests per full backfill run.

GDELT 2.0 CSV columns used (field positions per the GDELT 2.0 codebook):

| GDELT field | Column stored | Notes |
|---|---|---|
| `GLOBALEVENTID` | `global_event_id` | GDELT's own unique event ID; used for dedup |
| `SQLDATE` | `event_date` | YYYYMMDD → DATE |
| `DateAdded` | `event_datetime` | YYYYMMDDHHmmSS (UTC) → TIMESTAMPTZ; this is the *publication* timestamp used in match window arithmetic |
| `EventCode` | `cameo_code` | Hierarchical CAMEO string, e.g. `1951` |
| `EventRootCode` | `cameo_root` | Numeric root, e.g. `19` |
| `GoldsteinScale` | `goldstein_scale` | −10 (most hostile) to +10 |
| `NumMentions` | `num_mentions` | Total article mentions across all sources |
| `NumSources` | `num_sources` | Distinct source count; used as credibility signal |
| `AvgTone` | `avg_tone` | Average sentiment of covering articles |
| `Actor1Name` | `actor1_name` | First actor free-text name |
| `Actor2Name` | `actor2_name` | Second actor free-text name |
| `ActionGeo_Type` | `action_geo_type` | Precision tier (only 3 or 4 stored) |
| `ActionGeo_FullName` | `action_geo_fullname` | Human-readable location string |
| `ActionGeo_CountryCode` | `action_geo_country` | FIPS country code |
| `ActionGeo_Lat` | `latitude` | Kept alongside geom for cheap export |
| `ActionGeo_Long` | `longitude` | |

---

## Schema

### Table: `gdelt_events`

```sql
CREATE TABLE gdelt_events (
    id                  BIGSERIAL PRIMARY KEY,
    global_event_id     BIGINT        NOT NULL UNIQUE,
    event_date          DATE          NOT NULL,
    event_datetime      TIMESTAMPTZ   NOT NULL,
    cameo_code          VARCHAR(10)   NOT NULL,
    cameo_root          SMALLINT      NOT NULL,
    goldstein_scale     REAL,
    num_mentions        INTEGER,
    num_sources         INTEGER,
    avg_tone            REAL,
    actor1_name         TEXT,
    actor2_name         TEXT,
    action_geo_type     SMALLINT      NOT NULL,
    action_geo_fullname TEXT,
    action_geo_country  VARCHAR(5),
    geom                GEOGRAPHY(POINT, 4326) NOT NULL,
    latitude            DOUBLE PRECISION NOT NULL,
    longitude           DOUBLE PRECISION NOT NULL,
    ingested_at         TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX gdelt_events_geom_idx        ON gdelt_events USING GIST (geom);
CREATE INDEX gdelt_events_datetime_idx    ON gdelt_events (event_datetime);
CREATE INDEX gdelt_events_cameo_idx       ON gdelt_events (cameo_code);
```

### Table: `fire_event_correlations`

```sql
CREATE TABLE fire_event_correlations (
    id                  BIGSERIAL PRIMARY KEY,
    firms_detection_id  BIGINT        NOT NULL REFERENCES firms_detections(id),
    gdelt_event_id      BIGINT        NOT NULL REFERENCES gdelt_events(id),
    distance_m          REAL          NOT NULL,   -- ST_Distance result in metres
    time_delta_h        REAL          NOT NULL,   -- (event_datetime − acq_datetime) in hours; negative = event before fire
    score               REAL          NOT NULL,
    infra_id            BIGINT,                   -- FK to infrastructure table (Phase 3); NULL in Phase 2
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    UNIQUE (firms_detection_id, gdelt_event_id)
);

CREATE INDEX fec_firms_idx  ON fire_event_correlations (firms_detection_id);
CREATE INDEX fec_gdelt_idx  ON fire_event_correlations (gdelt_event_id);
CREATE INDEX fec_score_idx  ON fire_event_correlations (score DESC);
```

---

## GDELT Filtering

### CAMEO code filter

Retain only events whose `EventCode` matches one of the three root families below.
The CAMEO hierarchy is prefix-based, so a 4-digit code `1951` is a sub-event of `195`.

| Root | Meaning | Sub-codes retained |
|---|---|---|
| `190` | Use conventional military force (generic) | 190, 1901, 1902, … |
| `193` | Fight with conventional arms | 193, 1931, 1932, … |
| `195` | Launch missile / rocket / artillery attack | 195, 1951, 1952, … |

Filter predicate: `cameo_code LIKE '190%' OR cameo_code LIKE '193%' OR cameo_code LIKE '195%'`

Rows with any other EventCode are discarded before INSERT and never written to the DB.

### Location precision filter

Retain only rows where `ActionGeo_Type IN (3, 4)` (US city, world city).

- Country-level (1), US-state-level (2), and world-state-level (5) geos are discarded.
- Rationale: GDELT's stated city-level precision is ~10–50 km. State/country geos are
  too imprecise to produce meaningful spatial joins against FIRMS 375m detections.

### Country allowlist filter

Retain only events where `ActionGeo_CountryCode` is in the active-conflict allowlist
(FIPS 10-4 codes, defined in `gdelt_ingest.py`). All other countries are discarded.

Motivation: CAMEO 190/193/195 codes in stable countries (US, Australia, UK, etc.) are
almost always domestic law-enforcement actions, gang violence, or military exercises that
GDELT's NLP mislabels. These produce high-scoring false positives when a nearby wildfire
happens to be high-FRP and low-distance. A California wildfire matched against a CAMEO 193
"fight" in Calimesa scored 0.1108 — higher than any confirmed conflict fire — before
this filter was added.

The allowlist covers 39 countries across the Russia-Ukraine war, Middle East, the Sahel,
Central/East Africa, South/Southeast Asia, the Caucasus, and Haiti/Colombia. See
`gdelt_ingest.py:CONFLICT_COUNTRIES` for the full list with per-country rationale.

---

## Deduplication

GDELT events carry a `GLOBALEVENTID` that is unique per event. This is the dedup key.

- Before inserting, check `WHERE global_event_id = ?` against `gdelt_events`.
- If a row with that ID already exists, skip (no update).
- No spatial dedup is applied to GDELT events — unlike FIRMS pixels, distinct GDELT
  events near the same location are separate news items, not sensor duplicates.

---

## Match Definition

A FIRMS detection and a GDELT event are a valid pair when **all three** conditions hold:

1. **Distance** ≤ 25 km: `ST_DWithin(firms.geom, gdelt.geom, 25000)`
   - 25 km matches GDELT's worst-case city-level geo accuracy.
   - Measured on `GEOGRAPHY` so units are metres.

2. **Time window** − 72 h ≤ `time_delta_h` ≤ +12 h, where:
   ```
   time_delta_h = EXTRACT(EPOCH FROM (gdelt.event_datetime − firms.acq_datetime)) / 3600
   ```
   - Negative = GDELT event published before the fire was detected (expected for causal matches).
   - +12 h buffer allows for overnight satellite passes where the fire is detected hours
     before the article is published (e.g. pass at 02:00 UTC, article at 09:00 UTC).

3. **CAMEO code**: `gdelt.cameo_code LIKE '190%' OR '193%' OR '195%'`
   (already enforced at ingest, so this is a free filter at correlation time).

---

## Scoring

The correlation score is stored in `fire_event_correlations.score` (range 0–1).
It combines four independent signals. All component scores are clamped to [0, 1].

```
score = frp_score × conf_factor × source_credibility × proximity_decay × temporal_decay
```

| Component | Formula | Rationale |
|---|---|---|
| `frp_score` | `LEAST(frp / 300.0, 1.0)` | FRP in MW; 300 MW is an extreme fire. Higher FRP → stronger signal. |
| `conf_factor` | `1.0` if confidence = `h`, `0.8` if `n` | FIRMS high-confidence detections weighted up. |
| `source_credibility` | `LEAST(num_sources / 3.0, 1.0)` | Events covered by ≥3 distinct sources score full credibility. Denominator calibrated to conflict-zone GDELT reporting (1–4 sources typical); the original /10 crushed single-source events to 0.10. |
| `proximity_decay` | `SQRT(1.0 − distance_m / 25000.0)` | Concave decay; gentler than linear at mid-range, still 0 at 25 km. Prevents city-centre geocodes from overwhelming all other signals. |
| `temporal_decay` | `1.0 − (ABS(time_delta_h) / 84.0)` | Linear decay to 0 at the edge of the 84 h window (72 + 12). |

**Calibration status**: Formula calibrated 2026-06-20 against validation set V1 (Moscow Kapotnya)
and V2 (Gukovo). Pre-calibration scores were ≤0.0052; post-calibration max true-positive score
is 0.0199 (Gukovo). The formula now reliably ranks Gukovo > Moscow as expected. An operational
score threshold has not yet been set — requires at least 5 confirmed non-events for comparison.

---

## One-to-Many Handling

Both sides of the relationship can be one-to-many:

- One GDELT event may match many FIRMS detections (a fire cluster spread over 25 km).
- One FIRMS detection may match many GDELT events (multiple reported strikes nearby).

**All valid pairs are stored** in `fire_event_correlations`. No pair is silently dropped.
Consumers query the junction table and apply their own aggregation (e.g. `MAX(score)`,
`COUNT(*) GROUP BY gdelt_event_id`).

---

## Cadence & Backfill

**Cadence**: Manual trigger, matching Phase 1. No scheduler.

**Backfill**: 7-day rolling window aligned with Phase 1's FIRMS window.

- Each run ingests 7 daily GDELT export files (one per day) then rebuilds correlations
  for any FIRMS detection whose `acq_datetime` falls within the same window.
- Re-runs are safe: `GLOBALEVENTID` dedup prevents duplicate GDELT rows;
  the `UNIQUE(firms_detection_id, gdelt_event_id)` constraint prevents duplicate pairs.

---

## Verification

After each run, print:

1. **GDELT row count**: total rows in `gdelt_events`.
2. **CAMEO distribution**: count per root code (190, 193, 195) to confirm filter applied.
3. **Correlation count**: total rows in `fire_event_correlations`; must be ≥ 0.
4. **Score distribution**: `MIN`, `AVG`, `MAX` of `score` — flags degenerate scoring bugs.
5. **Sample correlations**: 3 rows ordered by `score DESC`, showing both the FIRMS point
   and the matched GDELT event's `actor1_name`, `actor2_name`, `action_geo_fullname`,
   `event_datetime`, `distance_m`, `time_delta_h`.

---

## Validation Set

Validated 2026-06-20 against the live pipeline (754,743 FIRMS rows, 20,124 GDELT events,
122,509 correlation pairs). Coordinates marked "≈" were provided as approximate.

| # | Event | Date | Coords | Type | Expected | Result |
|---|---|---|---|---|---|---|
| V1 | Moscow Oil Refinery strike (Kapotnya) | 2026-06-18 | 55.66°N 37.84°E | Conflict TP | Match + score | Match found, scores low (see notes) |
| V2 | Gukovo oil depot strike | 2026-06-18 | 48.06°N 39.93°E | Conflict TP | Match + score | Match found, strongest true positive |
| V3 | Withington Wilderness fire | 2026-06-17/18 | ≈33.7°N 107.5°W | Wildfire TN | No GDELT match | No FIRMS detections found — coordinates unverified |
| V4 | Bear Fire, NM (SE of Quemado) | 2026-06-09 onward | ≈34.15°N 108.2°W | Wildfire TN | FIRMS yes, no GDELT | Correct true negative |

### Per-event detail

**V1 — Moscow refinery (Kapotnya)**
- FIRMS: multiple detections 12.5 km from target, FRP 0.63–2.98 MW, all nominal confidence.
  Low FRP suggests either a smouldering secondary fire or that the main blaze cooled before
  the next orbital pass (~09:47 UTC Jun 19).
- GDELT: events geolocated to "Kapotnya, Moskva, Russia" (3.8 km from target, 1 source each)
  and to "Moscow, Moskva, Russia" (city centre, 17.4 km from target, up to 7 sources).
- Correlations: both geolocations produce pairs. Post-calibration best score 0.0019 (Kapotnya
  events, 1 src, 9.4 km). Moscow city-centre events (7 src, 24 km) score lower — the SQRT
  proximity decay still penalises 24 km heavily enough that distance wins over source count,
  which is the correct rank ordering given GDELT's geocoding to city centres.
- Verdict: true positive caught. Score improved ~5× after calibration (0.0004 → 0.0019).

**V2 — Gukovo oil depot**
- FIRMS: 22.83 MW detection (high confidence, sat=N20) 6.3 km from target at Jun 18 00:20 UTC.
- GDELT: CAMEO 193 (fight with conventional arms) at 772 m from target, 1 source. Published
  Jun 18 08:45 UTC → time_delta = +8.4 h (fire detected first; news published 8.4 h later,
  within the +12 h lag buffer designed for exactly this scenario).
- Correlations: post-calibration score 0.0199 — highest true-positive score in the dataset
  (pre-calibration was 0.0052, a ~3.8× improvement). The high-confidence VIIRS pixel with
  22.83 MW FRP drives frp_score to 0.076 and conf_factor to 1.0.
- Verdict: true positive caught. Score gap vs. noise now more useful for thresholding.

**V3 — Withington Wilderness fire**
- FIRMS: 0 detections within 50 km of 33.7°N 107.5°W (Jun 14–20). Widening to 100 km finds
  detections at ~76 km — these are the Bear Fire (V4), not Withington.
- GDELT: 0 events (expected — a wildfire generates no CAMEO 190/193/195 events).
- Verdict: inconclusive. The given coordinates appear to be ~76 km off from the nearest
  FIRMS activity. Resolve before using as a negative control:
  1. Check NM Forestry Division or NIFC for the exact fire perimeter.
  2. Re-run `SELECT ... WHERE ST_DWithin(geom, ST_MakePoint(-107.5, 33.7)::geography, 100000)`
     with a broader time range to locate detections.

**V4 — Bear Fire NM**
- FIRMS: detections at 34.05–34.09°N 108.28°W (13–14 km from approximate coords), FRP 0.4–8 MW,
  nominal confidence, multiple passes Jun 14–16.
- GDELT: 0 events within 50 km (correct — wildfire, no military CAMEO).
- Correlations: 0 pairs. ✓
- Verdict: correct true negative. Fire is detectable by VIIRS; absence of GDELT match confirms
  the CAMEO filter is not leaking non-conflict events at this location.

### Scoring calibration finding (from validation run)

The multiplicative score formula performs the intended rank ordering (Gukovo > Moscow)
but produces absolute values well below 0.01 for all real-world true positives tested.
Root causes identified:

1. **`source_credibility = LEAST(num_sources / 10, 1.0)`** — normalising by 10 is too
   aggressive. In conflict-zone reporting, 1–4 sources is typical; Telegram-first events
   rarely exceed 3 sources by the time GDELT processes them. A denominator of 3 or a
   log transform (`log2(num_sources + 1) / log2(11)`) would keep the 10-source ceiling
   without crushing single-source events to 0.10.

2. **`proximity_decay` dominates at city-level precision** — for GDELT events geolocated
   to a city centre (e.g. Moscow, 24 km from the FIRMS pixel), proximity_decay ≈ 0.04,
   which multiplies against every other signal. A square-root or log decay would be less
   punishing while still rewarding proximity.

3. **Low FRP for refinery fires** — an oil-refinery strike may produce a very intense but
   short-lived fire that VIIRS misses between orbital passes. The FRP observed (2.85 MW)
   suggests a residual smouldering detection, not the peak event. The current formula
   cannot distinguish "fire cooled by the time of the pass" from "no fire was there."

**Status (2026-06-20)**: Calibration applied and country filter added.

- Score formula: `source_credibility` denominator 10→3, `proximity_decay` linear→`SQRT`.
- Country allowlist: 39 active-conflict FIPS codes; all others discarded at GDELT ingest.
- Post-filter pipeline: 7,591 GDELT events, 70,218 correlation pairs, score range 0.0–0.0944.
- True positive scores unchanged: V2 Gukovo 0.0199, V1 Moscow Kapotnya 0.0019.
- Highest remaining scorer: Angok, South Sudan 0.0944 (160 MW savanna/conflict-zone fire —
  ambiguous; South Sudan is an active conflict zone but June savanna burns are common).

**Threshold guidance** (preliminary — requires more confirmed non-events):

| Threshold | Pairs passing | Notes |
|---|---|---|
| ≥ 0.050 | 4 | Very high confidence only; misses Gukovo (0.0199) |
| ≥ 0.020 | 394 | Catches Gukovo; misses Kapotnya (0.0019) |
| ≥ 0.002 | 13,910 | Catches both TPs; review burden high |

Suggested operational entry point: **score ≥ 0.020** for alerting, **score ≥ 0.002** for
archival / analyst review queue. Kapotnya-class events (low FRP, VIIRS missed the peak)
will not surface above 0.020 without corroborating signals (higher FRP in a later pass,
multi-source GDELT event).

---

## Out of Scope for Phase 2

- Airflow, Spark, Kafka, Databricks
- Infrastructure table definition (FK column reserved but table not created)
- Automated cadence / scheduling
- GDELT GKG or Mentions tables
- CAMEO codes outside 190x / 193x / 195x
- ActionGeo_Type values other than 3 and 4
