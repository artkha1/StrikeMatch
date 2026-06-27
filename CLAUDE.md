# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Project: Fire-Event Correlation Pipeline
Correlates NASA FIRMS satellite fire/thermal-anomaly detections with
ACLED-reported combat/strike events near critical infrastructure, using PostGIS for
spatial joins. Final MVP: an interactive global map dashboard showing deduplicated
fire detections; hovering a point surfaces any correlated military strike/action with score and news headlines/source links.
Fires with no matched event and events with no matched fire should both be surfaced.
The map includes a timeline scrubber for rewinding.

# Scope
Two theaters only: **Russia/Ukraine + the Middle East**. FIRMS fire data and ACLED conflict
events are both restricted to these regions — FIRMS via the regional/country bboxes in
`firms_ingest.py`, ACLED via the `country=` API filter in `acled_ingest.py`.

# Current Phase — ACTIVE TASK: replace GDELT with ACLED + narrow to 2 theaters

GDELT is being removed as the conflict-event source. It geocodes to city centroids
(~24 km off the fire), mislabels events (CAMEO false positives — a California wildfire once
matched a "fight"), and carries no event description — only a place name + URL. **ACLED**
replaces it: human-coded strike events with precise coordinates, a real `notes` description,
and a clean strike taxonomy. Scope narrows from 39 countries to **Russia/Ukraine + the
Middle East**.

**Guiding constraint: minimal changes** — keep the correlation join, the scoring formula, and
the Power BI serving view; swap only the *ingest layer* and remap columns. The one structural
change: the conflict-event table is **properly renamed `gdelt_events` → `acled_events`** (and
its FK `gdelt_event_id` → `acled_event_id`), not repurposed under the old GDELT name. The
Databricks Delta medallion + Airflow + Power BI runtime (described below) is already in place
and stays as-is — only the conflict-event source feeding it changes.

## ACLED API contract (Research-tier access — obtainable via institutional email)
- **Auth (OAuth):** `POST https://acleddata.com/oauth/token` with
  `grant_type=password, client_id=acled, scope=authenticated, username, password`
  → `access_token` (24 h; refresh 14 d). Send `Authorization: Bearer {token}` on reads.
- **Read:** `GET https://acleddata.com/api/acled/read?_format=json` with filters
  `country=Ukraine|Russia|Syria|...`, `event_type=Explosions/Remote violence`,
  `event_date={yyyy-mm-dd}&event_date_where=>`; paginate 5000 rows/call.
- **Strike filter:** `sub_event_type` ∈ {Air/drone strike, Shelling/artillery/missile attack,
  Remote explosive/landmine/IED}; `geo_precision` 1–2 only.
- **Credentials:** `ACLED_USERNAME` / `ACLED_PASSWORD` in `.env` **and** `.airflow.env` —
  never hardcode or commit (same rule as the NASA Earthdata + Databricks tokens).

## Concrete changes — files to change (highlighted)
- **NEW `acled_ingest.py`** — replaces `gdelt_ingest.py` in the DAG. OAuth + paginated read;
  RU/UA+ME `country` filter; strike `sub_event_type` filter; remap to `acled_events` (mapping
  below); derive `num_sources` from the ";"-split `source` field; dedup on `event_id_cnty`.
  Mirror the structure of `gdelt_ingest.py` (filter → parse → dedup → insert → verify).
- **`schema.sql`** — **rename `gdelt_events` → `acled_events`**; `global_event_id` → **TEXT**
  (holds ACLED `event_id_cnty`); add `description` (ACLED `notes`), `event_type`, `sub_event_type`,
  `fatalities`; drop the unused CAMEO columns. In `fire_event_correlations` rename the FK
  `gdelt_event_id` → `acled_event_id` (still references the serial `id`).
- **`firms_ingest.py`** — trim `REGION_BBOXES` to "Eastern Europe / Russia" + "Middle East";
  trim `COUNTRY_BBOXES` / `_in_conflict_zone` to the RU/UA+ME set; set `LOOKBACK_DAYS = 14`.
- **`spark_pipeline_databricks.py`** — rename the bronze table to `acled_events` (identifiers
  `T_GDELT_BRONZE` → `T_ACLED_BRONZE`, `gdelt_event_id` → `acled_event_id`); MERGE key
  `global_event_id` becomes STRING in `ensure_namespace` / `merge_bronze` / `load_bronze`; map
  the denormalized event fields in `compute_candidates` to ACLED (`sub_event_type`, `location`,
  `source`, derived `num_sources`, **+ new `description`** for the tooltip); update
  `build_serving_view` event columns; recenter the time-window constants (see Scoring).
- **`spark_pipeline.py`** (local fallback) — same correlation / window changes for parity.
- **`export_bronze.py`** — rename the `GDELT_SQL` query to `ACLED_SQL` and update its column list to the new `acled_events` schema.
- **`dags/fire_event_pipeline.py`** — rename the `ingest_gdelt` task to run `acled_ingest.py`.
- **Retired (keep in-repo, drop from DAG):** `gdelt_ingest.py` — reference/fallback only.

## Column mapping (ACLED → acled_events)
`event_id_cnty`→`global_event_id` (TEXT) · `event_date` @00:00 UTC→`event_datetime` ·
`latitude`/`longitude`→same · `sub_event_type`→`sub_event_type` (+display) · `notes`→`description` ·
`source` (";"-split count)→`num_sources`, names→`source_url` · `location`→`action_geo_fullname` ·
`country`/`iso`→`action_geo_country` · `actor1`/`actor2`→`actor1_name`/`actor2_name` ·
`fatalities`→`fatalities`.

## Startup — clean slate + 14-day backfill (do this first)
1. **Wipe all existing data** (the current Bamako/Moscow rows are from the stale-ID bug; start
   fresh). Postgres bronze:
   ```bash
   docker exec satellite_tracking-db-1 psql -U postgres -d satellite_tracking \
     -c "TRUNCATE firms_detections, acled_events, fire_event_correlations, firms_silver CASCADE"
   ```
   Also drop the Databricks Delta tables (`workspace.fire_pipeline.*`) so the new
   `acled_events` / gold schema is rebuilt fresh.
2. **Ingest 14 days of both:** `python firms_ingest.py` (LOOKBACK_DAYS=14) and
   `python acled_ingest.py` (event_date ≥ today−14).
3. **Run the transform** (local `spark_pipeline.py` or the Databricks job) and verify.

Out of scope: AWS S3/Lambda/Glue (that's a separate, decoupled task), Kafka,
dbt, Great Expectations.

# Stack
- Python 3.x, psycopg2, requests
- Postgres + PostGIS via Docker Compose (local bronze source)
- PySpark — local mode (`spark_pipeline.py`, fallback) **and** Databricks serverless
  (`spark_pipeline_databricks.py`, primary) on Delta bronze/silver/gold
- Apache Airflow 2.9.2 via Docker Compose — orchestrates the Databricks job remotely
- Power BI serves the gold layer from a Databricks serverless SQL warehouse
- ACLED conflict-event source via OAuth API (replaces GDELT) — see Current Phase
- NASA Earthdata token + ACLED OAuth credentials + Databricks PAT read from `.env` — never hardcode or commit them

---

## Commands

### Start services
```bash
docker compose up -d          # all services (PostGIS + Airflow)
docker compose up db -d       # PostGIS only (for local script runs)
docker compose logs -f airflow-scheduler   # tail scheduler logs
```

### Initialize / reset schema
```bash
psql postgresql://postgres:postgres@localhost:5432/satellite_tracking -f schema.sql
# Wipe data only (keep schema). On Windows (no local psql) run it via the container:
docker exec satellite_tracking-db-1 psql -U postgres -d satellite_tracking \
  -c "TRUNCATE firms_detections, acled_events, fire_event_correlations, firms_silver CASCADE"
```

### Run pipeline scripts locally (requires PostGIS running)
```bash
python firms_ingest.py          # fetch FIRMS (14-day, RU/UA+ME), write firms_detections (bronze)
python acled_ingest.py          # fetch ACLED strike events (14-day, RU/UA+ME), write acled_events (bronze)
python spark_pipeline.py        # local fallback: dedup → firms_silver; correlate → fire_event_correlations
```

Archive mode — fetch a specific date range for calibration/backfill (auto-selects SP products):
```bash
python firms_ingest.py --start 2024-01-25 --end 2024-01-26   # Jan 25 2024 Tuapse oil refinery
python firms_ingest.py --start 2024-08-18 --end 2024-08-19   # Aug 18 2024 Proletarsk oil depot
python firms_ingest.py --start 2025-01-14 --end 2025-01-15   # Jan 14 2025 Kazan Orgsintez
python firms_ingest.py --start 2025-01-17 --end 2025-01-18   # Jan 17 2025 Lyudinovo oil depot
python firms_ingest.py --start 2025-01-29 --end 2025-01-30   # Jan 29 2025 Kstovo refinery
python firms_ingest.py --start 2025-06-01 --end 2025-06-03   # Jun 1 2025 Spiderweb airbases
python acled_ingest.py --start 2024-01-25 --end 2024-01-25   # (repeat per event)
python export_bronze.py --all                                 # export all rows after multi-range ingest
```

### Databricks path (primary — Delta medallion)
```bash
python export_bronze.py   # export 14-day bronze windows → Parquet → UC Volume (needs Databricks env)
# then run the Databricks job (spark_pipeline_databricks.py) via Workflows UI or the DAG
```

**Air gap:** Databricks Free Edition is serverless-only and cannot reach the local Postgres. Bronze
crosses the gap as files: `export_bronze.py` writes Parquet (dropping PostGIS geom) and uploads to
a Unity Catalog Volume over HTTPS; the Databricks job MERGEs that Parquet into Delta. Nothing
connects back to the laptop.

```
Postgres ──export_bronze.py──► UC Volume Parquet ──Databricks job──► Delta medallion ──► SQL warehouse ──► Power BI
```

| File | Runs on | Role |
|---|---|---|
| `export_bronze.py` | laptop | Postgres 14-day windows → Parquet → UC Volume |
| `spark_pipeline_databricks.py` | Databricks job | Parquet → bronze Delta → silver → gold + serving view |
| `dags/fire_event_pipeline.py` | local Airflow | orchestrates ingest → export → trigger job → validate |

### Airflow UI
- URL: http://localhost:8080 (credentials: admin/admin)
- DAG: `fire_event_pipeline` — daily 06:00 UTC, max 1 active run

---

## Architecture

### Data flow
```
NASA FIRMS API (2 VIIRS sources: SNPP + NOAA-20)
    │
    ▼ firms_ingest.py   (14-day, RU/UA + Middle East bboxes)
firms_detections          ← Bronze, append-only, 14-day rolling
    │
    ▼ spark_pipeline.py (satellite_pass_dedup)
firms_silver              ← Silver, overwritten daily, deduplicated snapshot
    │
    ├──────────────────────────────────────┐
    ▼                                      ▼
ACLED API (weekly, OAuth)            firms_silver
    │ acled_ingest.py                      │
    ▼  (strike sub-event types)            │
acled_events              ← Bronze         │   (renamed from gdelt_events)
    │                                      │
    └──────── spark_pipeline.py ───────────┘
              (compute_candidates)
                    │
                    ▼
        fire_event_correlations   ← scored pairs, upsert-safe
```

### DAG topology
```
ingest_firms ─┐
               ├──► export_bronze ──► run_databricks_job ──► validate_pipeline
ingest_acled ─┘
```
`export_bronze` ships the 14-day bronze windows to a UC Volume; `run_databricks_job`
triggers `spark_pipeline_databricks.py` (Delta medallion); `validate_pipeline` queries
the Databricks SQL warehouse.

### Tables
Local Postgres holds bronze only; silver/gold are Delta in Databricks
(`workspace.fire_pipeline.*`). The `spark_pipeline_databricks.py` writers replace the
local `spark_pipeline.py` Postgres path.

| Table | Layer | Store | Writer | Purpose |
|---|---|---|---|---|
| `firms_detections` | Bronze | Postgres → Delta | `firms_ingest.py` → job MERGE | Raw FIRMS detections; incremental dedup at insert |
| `acled_events` | Bronze | Postgres → Delta | `acled_ingest.py` → job MERGE | ACLED strike events (RU/UA+ME); renamed from `gdelt_events` |
| `firms_silver` | Silver | Delta | `spark_pipeline_databricks.py` | Deduplicated 14-day snapshot; input to correlation |
| `fire_event_correlations` | Gold | Delta | `spark_pipeline_databricks.py` | Scored FIRMS×ACLED pairs |
| `gold_fire_event_map` | Gold | Delta view | `spark_pipeline_databricks.py` | Power BI serving view: matched + fire-only + event-only rows |

### Key design decisions

**Two-phase dedup (bronze + silver)**
`firms_ingest.py` performs a lightweight SQL NOT EXISTS check (1 km / ±6h) to prevent
re-ingesting exact duplicate passes across runs. `spark_pipeline.py` does a more
aggressive batch dedup with transitivity (grid-bin + Haversine): if B dominates A
and C, all three collapse to one, even if A and C are far apart. Correlation runs
against `firms_silver`, not `firms_detections`.

**Staging tables (local `spark_pipeline.py` only)**
Spark can't write GEOGRAPHY columns directly. The local job writes to `_firms_silver_stage`
(plain FLOAT lat/lon) and `_fire_event_correlations_stage`, then psycopg2 moves rows to the
real tables with geom cast and ON CONFLICT handling. The Databricks job has no PostGIS at all:
it stores plain lat/lon in Delta (Power BI maps from lat/lon) and uses Delta `MERGE` for the
idempotent silver-overwrite / gold-upsert instead of the staging dance.

**Correlation scoring (5-factor multiplicative)**
```
score = (frp/300) × conf_factor × (num_sources/3) × sqrt(1 − dist/25000) × (1 − |Δt_h|/T)
```
`num_sources` is derived from ACLED's ";"-split `source` field (1–4 typical, so the `/3`
denominator still holds). Temporal denominator `T = 54` (48 h window + 6 h timezone buffer).
`Δt_h = event_midnight − fire_time`; in correct matches this is negative (fire after event).
Score targets (established from ACLED-era ground-truth calibration): ≥0.020 alerting, ≥0.002 archival.

| Component | Formula | Rationale |
|---|---|---|
| `frp_score` | `LEAST(frp/300, 1)` | 300 MW ≈ an extreme fire |
| `conf_factor` | `1.0` high / `0.8` nominal | weight high-confidence detections up |
| `source_credibility` | `LEAST(num_sources/3, 1)` | conflict reporting is 1–4 sources |
| `proximity_decay` | `SQRT(1 − dist/25000)` | concave; gentler mid-range, 0 at 25 km |
| `temporal_decay` | `1 − \|Δt_h\|/54` | linear decay; T=54 (48 h + 6 h timezone buffer) |

**Match definition (FIRMS × ACLED)**
A pair is valid when distance ≤ 25 km AND `event_midnight ∈ [fire_time − 48 h, fire_time + 6 h]`.
In practice this means the fire must be detected on the same day or the next day after the ACLED
event date. The +6 h buffer is a timezone allowance only (ACLED event_date is local time; FIRMS
`acq_datetime` is UTC — a late-night local strike can appear as early-next-day UTC). `time_delta_h`
is defined as `event_midnight − fire_time`; valid pairs are ≤ 0 (event before fire), or slightly
positive only within the timezone buffer. GDELT-style large positive values (event published days
after fire) are excluded — ACLED records actual event dates, not publication times.
25 km can be narrowed during recalibration since ACLED `geo_precision` 1–2 is tighter than GDELT's
city-centroid error. Many-to-many: every valid pair is stored; consumers aggregate (`MAX(score)`).

**FIRMS source & false-positive filter**
VIIRS I-Band 375m only — NRT products (lag ~3 h), switching to the SP archive products when
`DATA_LAG_DAYS > 10`. MODIS and VIIRS 750m are excluded (single schema). `confidence = 'low'`
is dropped at ingest and never stored (agricultural burns and gas flares skew low-confidence);
`nominal`/`high` are kept; FRP is stored, not thresholded.

**Validation benchmarks (ACLED-era, confirmed matches)**
All four events below are in the local DB and produce correct correlations via `spark_pipeline.py`.

| Event | Date | Score | Dist | FRP | Notes |
|---|---|---|---|---|---|
| Proletarsk oil depot (Rostov) | 2024-08-18 | **0.069** | 5 km | 43.7 MW | Strongest true positive; summer, clear sky; fire burned 2+ days |
| Dyagilevo airfield (Ryazan) | 2025-06-01 | 0.0045 | 6 km | 5.5 MW | Part of Spiderweb campaign; confirmed air base fuel fire |
| Lyudinovo oil terminal (Kaluga) | 2025-01-17 | 0.0033 | 1.3 km | 3.4 MW | Winter detection; weak but confirmed |
| Engels refinery (Saratov) | 2025-01-14 | 0.0020 | 12.6 km | 1.1 MW | Borderline archival threshold; drone debris + fire |

VIIRS misses (cloud cover): Tuapse Jan 2024 (winter Black Sea), Kazan Jan 2025, Kstovo Jan 2025.
These are expected sensor gaps, not pipeline failures.

**`.env`**
Single secrets file shared by local scripts and the Airflow container. `DATABASE_URL` in `.env`
points to `localhost:5432` for local runs; the Airflow container overrides it to `db:5432` via
a hardcoded env var in `docker-compose.yml` (`x-airflow-common-env`).
