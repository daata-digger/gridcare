from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "gridcare", "retries": 1, "retry_delay": timedelta(minutes=3)}
with DAG(
    "grid_ingestion_hourly",
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    schedule="15 * * * *",
    catchup=False,
    tags=["bronze", "grid"],
) as dag:

    ingest_grid_load = BashOperator(
        task_id="ingest_grid_load",
        bash_command="python /project/ingestion/fetch_grid_data.py",
        env={},  # keep empty unless you need overrides
    )
