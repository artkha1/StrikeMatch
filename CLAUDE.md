# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Project: Fire-Event Correlation Pipeline
Correlates NASA FIRMS satellite fire/thermal-anomaly detections with
ACLED-reported combat/strike events near critical infrastructure.
Final MVP: an interactive global map dashboard showing only confirmed correlated
strike events; hovering a point surfaces the matched military action with
score, event description, and source links. The map includes a timeline scrubber.

# Scope
Two theaters only: **Russia/Ukraine + the Middle East**. FIRMS fire data and ACLED conflict
events are both restricted to these regions — FIRMS via the regional/country bboxes in
`firms_ingest.py`, ACLED via the `country=` API filter in `acled_ingest.py`.

**Date range: 2022-02-01 to present** (start of Russia-Ukraine war). ACLED Research-tier
access has a ~1-year publication lag, so the practical ceiling for ACLED data is ~today − 1 year.
FIRMS SP archive products cover any date, with no embargo.

---

# Current Phase — ACTIVE TASKS (next session picks up here)

The ACLED migration is complete and the algorithm has been tuned. Three tasks remain
(Task 1 is complete):

## ~~Task 1 — Remove PostgreSQL entirely~~ ✓ DONE

Postgres/PostGIS removed. Ingest scripts now write Parquet directly to the UC Volume.
Deleted: `export_bronze.py`, `schema.sql`, `spark_pipeline.py`.
IDs are stable hash-based values: FIRMS from `hash(acq_datetime, lat, lon, satellite)`,
ACLED from `hash(global_event_id)` — both via `pd.util.hash_pandas_object`.

## Task 2 — Column renames

Two columns have misleading names; rename everywhere (Delta DDL, Python select aliases,
serving view SQL):

| Old name | New name | Location | Why |
|---|---|---|---|
| `source_url` | `source` | `acled_events` table, gold denorm, serving view | ACLED `source` field is a list of outlet names, not URLs |
| `event_fullname` | `event_location_full_name` | gold `fire_event_correlations`, serving view | Matches the ACLED field semantics (full location name, e.g. "Kherson, Kherson, Ukraine") |

In `spark_pipeline_databricks.py`:
- `compute_candidates()`: `F.col("source_url").alias("event_source_url")` →
  `F.col("source").alias("event_source")`  (note: acled_events column is `source` after rename)
- `compute_candidates()`: `F.col("action_geo_fullname").alias("event_fullname")` →
  `F.col("action_geo_fullname").alias("event_location_full_name")`
- Gold table DDL in `ensure_namespace()`: rename those two columns
- `build_serving_view()`: update column references

In `acled_ingest.py`: rename the insert column `source_url` → `source`.

**Note:** the underlying ACLED API field mapping is unchanged — `source` (";"-split string
from ACLED) → count into `num_sources`, names into `source` (was `source_url`).

## Task 3 — Serving view: matched records only

Drop `fire_only` and `event_only` from `gold_fire_event_map`. The map shows only confirmed
correlations (score_display ≥ 2). Unmatched fires and unmatched events are not surfaced.

In `build_serving_view()`, remove `displayable_events`, `jittered_events`,
`fire_only`, and `event_only` CTEs. The view becomes:

```sql
CREATE OR REPLACE VIEW gold_fire_event_map AS
WITH matched_ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY acled_event_id ORDER BY score DESC) AS _rn
    FROM fire_event_correlations
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
```

The jitter places up to 100 events per coordinate on a centered 10×10 grid at ±0.0045° (≈ ±500 m),
guaranteeing no duplicate `(map_lat, map_lon)` pairs in Power BI.

## Task 4 — Historical backfill: 2022-02-01 to present

The pipeline needs to ingest the full war period, not just a rolling 14-day window.

**FIRMS:** Archive (SP) products are available for any date via `--start`/`--end`. The
script already auto-selects SP products when date is > 10 days old, and chunks into 5-day
API requests internally. Run month-by-month to manage memory and upload size:
```bash
python firms_ingest.py --start 2022-02-23 --end 2022-02-28
python firms_ingest.py --start 2022-03-01 --end 2022-03-31
# ... continue month by month ...
```

**ACLED:** Research-tier lag means data is available up to ~today − 1 year. Same
`--start`/`--end` interface. Batch by month:
```bash
python acled_ingest.py --start 2022-02-24 --end 2022-02-28
# ...
```

**Volume size estimate:** ~40 months × 70K FIRMS rows/month ≈ 2.8 M fire detections;
~40 months × 7K ACLED events/month ≈ 280 K events. Both are manageable in Delta.

**Strategy:** Run all ingest batches first (they write Parquet locally then upload,
overwriting the same Volume path each run — or use dated subdirs if keeping individual
month files). Then trigger the Databricks job once with all data loaded. The Delta MERGE
is idempotent so re-running is safe.

**Important:** After removing Postgres (Task 1), the ingest scripts no longer have an
incremental dedup check. For backfill, run each month exactly once. If a month is re-run,
the Parquet will be overwritten and the Delta MERGE will skip already-existing rows
(FIRMS merges on `id`; ACLED merges on `global_event_id`).

---

# ACLED API contract (Research-tier — ~1-year lag)
- **Auth (OAuth):** `POST https://acleddata.com/oauth/token` with
  `grant_type=password, client_id=acled, scope=authenticated, username, password`
  → `access_token` (24 h; refresh 14 d). Send `Authorization: Bearer {token}` on reads.
- **Read:** `GET https://acleddata.com/api/acled/read?_format=json` with filters
  `country=Ukraine|Russia|Syria|...`, `event_type=Explosions/Remote violence`,
  `event_date={yyyy-mm-dd}&event_date_where=>`; paginate 5000 rows/call.
- **Strike filter:** `sub_event_type` ∈ {Air/drone strike, Shelling/artillery/missile attack}; `geo_precision` 1–2 only.
- **Credentials:** `ACLED_USERNAME` / `ACLED_PASSWORD` in `.env` — never hardcode or commit.

## Column mapping (ACLED API → acled_events Delta table)
`event_id_cnty`→`global_event_id` (STRING) · `event_date` @00:00 UTC→`event_datetime` ·
`latitude`/`longitude`→same · `sub_event_type`→`sub_event_type` · `notes`→`description` ·
`source` (";"-split count)→`num_sources`, names→`source` · `location`→`action_geo_fullname` ·
`country`/`iso`→`action_geo_country` · `actor1`/`actor2`→`actor1_name`/`actor2_name` ·
`fatalities`→`fatalities`.

---

# Stack
- Python 3.x, requests, pandas, databricks-sdk, pyarrow
- PySpark — Databricks serverless (`spark_pipeline_databricks.py`, primary)
- Apache Airflow 2.9.2 via Docker Compose — triggers the Databricks job on schedule
- Power BI serves the gold layer from a Databricks serverless SQL warehouse
- ACLED conflict-event source via OAuth API (~1-year research lag)
- NASA FIRMS MAP Key + ACLED OAuth + Databricks PAT read from `.env`

**Postgres/PostGIS removed** — ingest scripts write Parquet directly to the UC Volume.

---

## Commands

### Start services
```bash
docker compose up -d          # Airflow only (Postgres service removed)
docker compose logs -f airflow-scheduler
```

### Run pipeline scripts
```bash
python firms_ingest.py          # fetch FIRMS (rolling window), write Parquet → UC Volume
python acled_ingest.py          # fetch ACLED strikes, write Parquet → UC Volume
# then trigger spark_pipeline_databricks.py via Databricks Workflows UI or the DAG
```

Archive / backfill mode:
```bash
python firms_ingest.py --start 2022-02-01 --end 2022-02-28
python acled_ingest.py --start 2022-02-01 --end 2022-02-28
# repeat month by month; then trigger Databricks job once. Start with February 24, 2022 to match Russian invasion of Ukraine
```

Ground-truth calibration dates:
```bash
python firms_ingest.py --start 2024-08-18 --end 2024-08-19   # Proletarsk oil depot
python firms_ingest.py --start 2025-01-17 --end 2025-01-18   # Lyudinovo oil terminal
python firms_ingest.py --start 2025-06-01 --end 2025-06-03   # Spiderweb airbases
python acled_ingest.py --start 2024-08-18 --end 2024-08-18   # (repeat per event date)
```

### Databricks
Drop Delta tables before a clean rebuild:
```sql
DROP TABLE IF EXISTS workspace.fire_pipeline.firms_detections;
DROP TABLE IF EXISTS workspace.fire_pipeline.acled_events;
DROP TABLE IF EXISTS workspace.fire_pipeline.firms_silver;
DROP TABLE IF EXISTS workspace.fire_pipeline.fire_event_correlations;
DROP VIEW  IF EXISTS workspace.fire_pipeline.gold_fire_event_map;
```
Trigger job: Databricks Workflows UI → `fire_event_pipeline` → Run now.

### Airflow UI
- URL: http://localhost:8080 (credentials: admin/admin)
- DAG: `fire_event_pipeline` — daily 06:00 UTC; tasks: `run_databricks_job ──► validate_pipeline`

---

## Architecture

### Tables (all Delta, `workspace.fire_pipeline.*`)

| Table | Layer | Writer | Purpose |
|---|---|---|---|
| `firms_detections` | Bronze | job MERGE from Parquet | Raw FIRMS detections |
| `acled_events` | Bronze | job MERGE from Parquet | ACLED strike events (RU/UA+ME, Feb 2022–) |
| `firms_silver` | Silver | `satellite_pass_dedup` | Deduplicated snapshot; input to correlation |
| `fire_event_correlations` | Gold | `compute_candidates` + MERGE | Scored FIRMS×ACLED pairs |
| `gold_fire_event_map` | Gold view | `build_serving_view` | Power BI: matched records only (score_display ≥ 2) |

### Key design decisions

**Single-phase dedup (silver only)**
With Postgres removed, the incremental NOT EXISTS check is gone. `satellite_pass_dedup`
(grid-bin + Haversine anti-join, 1 km / 6 h) runs as a batch on every job execution,
which is more aggressive and correct (handles transitivity). ACLED dedup is handled by
the Delta MERGE on `global_event_id`.

**Correlation scoring (5-factor multiplicative)**
```
score = (frp/300) × conf_factor × (num_sources/3) × sqrt(1 − dist/10000) × (1 − |Δt_h|/T)
```
`num_sources` is derived from ACLED's ";"-split `source` field (1–4 typical).
Temporal denominator `T = 54` (48 h window + 6 h timezone buffer).
`Δt_h = event_midnight − fire_time`; negative in correct matches (fire after event).
Raw score ∈ [0, 1]; serving view exposes `score_display = score × 1000`.
Score targets (`score_display`): ≥ 20 alerting, ≥ 2 archival.
FRP minimum: fires with FRP < 1.0 MW are excluded from correlation at join time.

| Component | Formula | Rationale |
|---|---|---|
| `frp_score` | `LEAST(frp/300, 1)` | 300 MW ≈ an extreme fire |
| `conf_factor` | `1.0` high / `0.8` nominal | weight high-confidence detections |
| `source_credibility` | `LEAST(num_sources/3, 1)` | conflict reporting is 1–4 outlets |
| `proximity_decay` | `SQRT(1 − dist/10000)` | concave; 0 at 10 km boundary |
| `temporal_decay` | `1 − |Δt_h|/54` | linear; T=54 (48 h + 6 h timezone buffer) |

**Match definition (FIRMS × ACLED)**
A pair is valid when distance ≤ 10 km AND `event_midnight ∈ [fire_time − 48 h, fire_time + 6 h]`.
ACLED `geo_precision` 1–2 is site-precise (≤ 5 km error); 10 km gives ~4 km safety margin.
Many-to-many stored in gold; serving view selects best-scoring fire per ACLED event.

**Serving view: matched only**
`gold_fire_event_map` contains only confirmed correlations (score_display ≥ 2, one row per
ACLED event). Fire-only and event-only rows are not surfaced. Map coordinates use ACLED
event lat/lon with ROW_NUMBER-based jitter (10×10 grid at 0.001° steps, ±0.0045°) to
separate co-located events (e.g., 40 events at the same Gaza coordinate).

**FIRMS source & false-positive filter**
VIIRS I-Band 375m only — NRT products (lag ~3 h) for recent dates, SP archive products
for dates > 10 days old (auto-selected by `firms_ingest.py`). `confidence = 'low'` is
dropped at ingest. FRP stored without threshold at ingest; fires < 1.0 MW filtered at
correlation time. MODIS and VIIRS 750m excluded.

**Validation benchmarks (confirmed matches, score_display)**

| Event | Date | score_display | Dist | FRP | Notes |
|---|---|---|---|---|---|
| Proletarsk oil depot (Rostov) | 2024-08-18 | **~55** | 5 km | 43.7 MW | Strongest true positive; summer, clear sky |
| Dyagilevo airfield (Ryazan) | 2025-06-01 | ~3.3 | 6 km | 5.5 MW | Spiderweb campaign; confirmed fuel fire |
| Lyudinovo oil terminal (Kaluga) | 2025-01-17 | ~3.2 | 1.3 km | 3.4 MW | Winter; weak but confirmed |

VIIRS cloud-cover misses (expected, not pipeline failures): Tuapse Jan 2024, Kazan Jan 2025,
Kstovo Jan 2025.

**`.env`** (see `.env.example` for full template)
```
FIRMS_MAP_KEY=...              # get free key at firms.modaps.eosdis.nasa.gov/api/
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/satellite_tracking
ACLED_USERNAME=...
ACLED_PASSWORD=...
DATA_LAG_DAYS=364              # ACLED research-tier lag in days; 0 for real-time
DATABRICKS_HOST=https://...
DATABRICKS_TOKEN=...
DATABRICKS_VOLUME_PATH=/Volumes/workspace/fire_pipeline/bronze_inbound
DATABRICKS_JOB_ID=...
DATABRICKS_SQL_HTTP_PATH=/sql/1.0/warehouses/...
FP_CATALOG=workspace           # optional; defaults to "workspace"
FP_SCHEMA=fire_pipeline        # optional; defaults to "fire_pipeline"
AIRFLOW_CONN_DATABRICKS_DEFAULT={"conn_type":"databricks","host":"...","password":"..."}
```
After Task 1 (Postgres removal): `DATABASE_URL` is dropped. Never hardcode or commit secrets.
