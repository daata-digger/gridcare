"""
GridCARE Weather Data Ingestion DAG 
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "gridcare",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="weather_ingestion_hourly",
    default_args=default_args,
    description="Fetch weather data (simplified version)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="5 * * * *",
    catchup=False,
    tags=["bronze", "weather", "ingestion"],
) as dag:

    # Task 1: Check database
    check_db = BashOperator(
        task_id="check_database_connection",
        bash_command="""
        echo "Checking database connection..."
        docker exec gridcare_db pg_isready -U postgres && echo "Database OK" || echo "Database not ready"
        exit 0
        """,
    )

    # Task 2: Simulate weather ingestion
    ingest_weather_data = BashOperator(
        task_id="ingest_weather_data",
        bash_command="""
        echo "Weather data ingestion started at $(date)"
        echo "Note: fetch_weather_data.py script needs to be created"
        echo "This is a placeholder task that simulates successful ingestion"
        sleep 2
        echo "Weather data ingestion completed"
        exit 0
        """,
    )

    # Task 3: Log completion
    log_completion = BashOperator(
        task_id="log_completion",
        bash_command='echo "Weather ingestion pipeline completed at $(date)"',
    )

    check_db >> ingest_weather_data >> log_completion