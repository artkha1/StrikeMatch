from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

PIPELINE_DIR = "/opt/pipeline"

# Airflow connection id pointing at the Databricks workspace (host + PAT). Created
# from DATABRICKS_HOST / DATABRICKS_TOKEN — see DATABRICKS.md.
DATABRICKS_CONN_ID = "databricks_default"
DATABRICKS_JOB_ID = int(os.environ.get("DATABRICKS_JOB_ID", "0"))

default_args = {
    "owner": "pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def _validate_pipeline() -> None:
    """
    Assert the Databricks Delta medallion is healthy by querying the serverless SQL
    warehouse: all bronze/silver/gold tables non-empty and scores within [0, 1].
    """
    from databricks import sql

    catalog = os.environ.get("FP_CATALOG", "workspace")
    schema = os.environ.get("FP_SCHEMA", "fire_pipeline")
    failures: list[str] = []

    with sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", "").rstrip("/"),
        http_path=os.environ["DATABRICKS_SQL_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn, conn.cursor() as cur:
        for table in ("firms_detections", "gdelt_events", "firms_silver", "fire_event_correlations"):
            cur.execute(f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.`{table}`")
            n = cur.fetchone()[0]
            if n == 0:
                failures.append(f"{table}: 0 rows")

        cur.execute(
            f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.fire_event_correlations"
            " WHERE score < 0 OR score > 1"
        )
        bad = cur.fetchone()[0]
        if bad:
            failures.append(f"fire_event_correlations: {bad} rows with score outside [0, 1]")

    if failures:
        from airflow.exceptions import AirflowException

        raise AirflowException(
            "Validation failed:\n" + "\n".join(f"  - {f}" for f in failures)
        )
    print("All validation checks passed.")


with DAG(
    dag_id="fire_event_pipeline",
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["satellite-tracking", "databricks"],
    doc_md="""
## Fire-Event Correlation Pipeline (Databricks)

Ingests NASA FIRMS detections and GDELT conflict events into local Postgres (bronze
source), exports the 7-day windows to Parquet and uploads them to a Databricks Unity
Catalog Volume, then triggers the Databricks Spark job that builds the Delta
bronze→silver→gold medallion. Gold is served to Power BI from a serverless SQL
warehouse; validation queries that warehouse.

**Task graph**
```
ingest_firms ─┐
               ├──► export_bronze ──► run_databricks_job ──► validate_pipeline
ingest_gdelt ─┘
```

All tasks are **idempotent** — ingest dedups, export overwrites the Volume Parquet,
the Databricks job MERGEs bronze/gold. Schedule: daily at 06:00 UTC.
    """,
) as dag:

    ingest_firms = BashOperator(
        task_id="ingest_firms",
        bash_command=f"cd {PIPELINE_DIR} && python firms_ingest.py",
    )

    ingest_gdelt = BashOperator(
        task_id="ingest_gdelt",
        bash_command=f"cd {PIPELINE_DIR} && python gdelt_ingest.py",
    )

    # Export the 7-day bronze windows to Parquet and push to the UC Volume.
    export_bronze = BashOperator(
        task_id="export_bronze",
        bash_command=f"cd {PIPELINE_DIR} && python export_bronze.py",
    )

    # Trigger the deployed Databricks job (bronze→silver→gold Delta) over HTTPS.
    run_databricks_job = DatabricksRunNowOperator(
        task_id="run_databricks_job",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
    )

    validate_pipeline = PythonOperator(
        task_id="validate_pipeline",
        python_callable=_validate_pipeline,
    )

    [ingest_firms, ingest_gdelt] >> export_bronze >> run_databricks_job >> validate_pipeline
