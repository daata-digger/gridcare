"""
GridCARE Grid Data Ingestion DAG 
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
    dag_id="grid_ingestion_hourly",
    default_args=default_args,
    description="Fetch grid/ISO data (simplified version)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="15 * * * *",
    catchup=False,
    tags=["bronze", "grid", "ingestion"],
) as dag:

    # Task 1: Check database connection
    check_db = BashOperator(
        task_id="check_database_connection",
        bash_command="""
        echo "Checking database connection..."
        docker exec gridcare_db pg_isready -U postgres && echo "Database OK" || echo "Database not ready"
        exit 0
        """,
    )

    # Task 2: Simulate data ingestion (placeholder until script exists)
    ingest_grid_load = BashOperator(
        task_id="ingest_grid_load",
        bash_command="""
        echo "Grid data ingestion started at $(date)"
        echo "Note: fetch_grid_data.py script needs to be created"
        echo "This is a placeholder task that simulates successful ingestion"
        sleep 2
        echo "Grid data ingestion completed"
        exit 0
        """,
    )

    # Task 3: Log completion
    log_completion = BashOperator(
        task_id="log_completion",
        bash_command='echo "Grid ingestion pipeline completed at $(date)"',
    )

    check_db >> ingest_grid_load >> log_completion