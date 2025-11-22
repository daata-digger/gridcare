from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = dict(owner="gridcare", retries=1, retry_delay=timedelta(minutes=2))

with DAG(
    dag_id="silver_gold_hourly",
    start_date=datetime(2025, 1, 1),
    schedule="20 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["silver", "gold", "postgres", "spark"],
) as dag:

    common_env = {
        "PG_HOST": "db",
        "PG_PORT": "5432",
        "PG_DB":   "postgres",
        "PG_USER": "postgres",
        "PG_PASS": "postgres",
    }

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command="docker exec gridcare_spark python3 /project/pipelines/transform/bronze_to_silver.py",
        env=common_env,
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command="docker exec gridcare_spark python3 /project/pipelines/transform/silver_to_gold.py",
        env=common_env,
    )

    bronze_to_silver >> silver_to_gold
