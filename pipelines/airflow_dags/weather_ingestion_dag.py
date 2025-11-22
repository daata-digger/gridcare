from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {"owner": "gridcare", "retries": 1, "retry_delay": timedelta(minutes=3)}
with DAG(
    "weather_ingestion_hourly",
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    schedule="5 * * * *",
    catchup=False,
    tags=["bronze", "weather"],
) as dag:

    ingest_weather = BashOperator(
        task_id="ingest_weather",
        bash_command="python /project/ingestion/fetch_weather_data.py",
    )
