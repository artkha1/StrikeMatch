# Databricks Free Edition Migration (Phase 4)

The Phase 4 Spark job and its bronze/silver/gold data run on **Databricks Free Edition**
as **Delta** tables. Local Postgres is now only the bronze *source*; the medallion lives
in Delta and is served to **Power BI** from a serverless SQL warehouse.

## Why the air gap

Free Edition is serverless-only, has no private networking, and **cannot reach the local
Postgres**. So bronze crosses the gap as files: `export_bronze.py` (local) writes Parquet
and uploads it to a Unity Catalog Volume over outbound HTTPS; the Databricks job MERGEs
that Parquet into Delta. Nothing connects back to the laptop.

```
Postgres ──export_bronze.py──► UC Volume Parquet ──Databricks job──► Delta medallion ──► SQL warehouse ──► Power BI
```

## Components

| File | Runs on | Role |
|---|---|---|
| `export_bronze.py` | laptop | Postgres 7-day windows → Parquet → UC Volume (drops PostGIS geom) |
| `spark_pipeline_databricks.py` | Databricks job | Parquet → bronze Delta → silver → gold + serving view |
| `dags/fire_event_pipeline.py` | local Airflow | orchestrates ingest → export → trigger job → validate |

`spark_pipeline_databricks.py` is the serverless port of `spark_pipeline.py`: identical
Haversine/dedup/scoring logic, but no `SparkSession` cluster config, no JDBC, no
psycopg2/PostGIS. Delta `MERGE` replaces the staging-table + `ON CONFLICT` upsert; the
self-proximity audit is recomputed with the same grid-bin Haversine logic instead of
`ST_DWithin`. `spark_pipeline.py` is retained as the local-mode fallback.
