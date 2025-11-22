from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "gridcare",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="ml_training_daily",
    default_args=default_args,
    description="Train load forecasting model on gold iso_hourly_enriched",
    start_date=datetime(2025, 11, 20),
    schedule_interval="0 3 * * *",  # daily at 03:00 UTC
    catchup=False,
    tags=["ml", "gold"],
) as dag:

    train_model = BashOperator(
        task_id="train_rf_model",
        bash_command=(
            "docker exec gridcare_spark "
            "python3 /project/ml/model_train.py"
        ),
    )