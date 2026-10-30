"""
d8a-de-workbench: the one DAG the template ships. It proves the scheduler
reaches the warehouse and runs dbt. Replace it with your own DAG in P4; keep
the pattern (a sensor or check first, then the pipeline, then the tests).
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def warehouse_reachable() -> str:
    import psycopg2  # provided by the image

    dsn = os.environ["WAREHOUSE_DSN"]
    with psycopg2.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute("select count(*) from information_schema.schemata where schema_name in ('raw','staging','marts')")
        n = cur.fetchone()[0]
    if n < 3:
        raise RuntimeError(f"warehouse schemas missing: {n} of 3")
    return f"warehouse ok: {n} schemas"


with DAG(
    dag_id="workbench_healthcheck",
    description="The workbench reaches the warehouse and dbt parses.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["d8a", "workbench"],
) as dag:
    check = PythonOperator(task_id="warehouse_reachable", python_callable=warehouse_reachable)
    dbt_parse = BashOperator(
        task_id="dbt_parse",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir . && dbt parse --profiles-dir .",
        env={**os.environ, "POSTGRES_HOST": "postgres"},
    )
    check >> dbt_parse
