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

**Date range: 2022-02-01 to today-1 year** (start of Russia-Ukraine war). ACLED Research-tier
access has a ~1-year publication lag, so the practical ceiling for all data is ~today − 1 year.
FIRMS SP archive products cover any date, with no embargo.

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
- Python 3.x, requests, pandas, databricks-sdk, pyarrow (`requirements.txt` — local ingest only)
- PySpark — Databricks serverless (`spark_pipeline_databricks.py`, primary); **not** in `requirements.txt`
- Apache Airflow 2.9.2 via Docker Compose — triggers the Databricks job on schedule
- Power BI serves the gold layer from a Databricks serverless SQL warehouse
- ACLED conflict-event source via OAuth API (~1-year research lag)
- NASA FIRMS MAP Key + ACLED OAuth + Databricks PAT read from `.env`

**Postgres/PostGIS removed** — ingest scripts write Parquet directly to the UC Volume.

**Docker Compose services:** `airflow-db` (Postgres 16 metadata store), `airflow-init` (one-shot schema migration + admin user creation), `airflow-scheduler` (LocalExecutor; loads `.env`; mounts `.:/opt/pipeline:ro` so ingest scripts run inside the container), `airflow-webserver` (port 8080). `Dockerfile.airflow` pins `apache-airflow-providers-databricks` under Airflow constraints.

---

## CRITICAL: Triggering the Databricks job

The Databricks job task runs `/Workspace/Users/timkhaiet@gmail.com/spark_pipeline_databricks.py`.
**Before triggering the job, upload the local script to that workspace path** — otherwise the
job executes the old version. A PreToolUse hook in `.claude/settings.local.json` detects
uncommitted local changes in `spark_pipeline_databricks.py` and blocks the job trigger if any exist.

**Hook logic** (`.claude/hooks/check_databricks_trigger.py`): fires when tool input contains
`jobs.run_now`, `run_now(job_id`, or `DATABRICKS_JOB_ID`; runs `git diff HEAD -- spark_pipeline_databricks.py`;
exits with code 2 (block) if changes are detected, 0 (allow) otherwise. Fails open on hook error.

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
- DAG: `fire_event_pipeline` — daily 06:00 UTC
- Task graph:
  ```
  ingest_firms ─┐
                 ├──► run_databricks_job ──► validate_pipeline
  ingest_acled ─┘
  ```
  `ingest_firms` and `ingest_acled` run in parallel (BashOperator). `run_databricks_job` uses `DatabricksRunNowOperator`. `validate_pipeline` queries bronze/silver/gold via Databricks SQL warehouse directly.

---

## Architecture

### Script function map

| Script | Key functions |
|---|---|
| `firms_ingest.py` | `fetch_source` — parallel fetch (one call per VIIRS product+region); `_in_conflict_zone` — spatial bbox filter; `parse_row` — field extraction + low-confidence drop; `_upload` — Parquet write to UC Volume |
| `acled_ingest.py` | `_get_token` — OAuth; `_fetch_page` — paginated API with retry; `_parse_row` — geo_precision + sub_event_type filter; `_upload` — Parquet write to UC Volume |
| `spark_pipeline_databricks.py` | `satellite_pass_dedup` — silver dedup (grid-bin + Haversine anti-join, 1 km/6 h); `compute_candidates` — 5-factor scoring spatial-temporal join; `merge_bronze`/`write_silver`/`write_gold`/`build_serving_view` — DDL + idempotent writes; `verify` — pipeline assertions |
| `dags/fire_event_pipeline.py` | Airflow DAG — see task graph above |

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
