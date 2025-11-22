@echo off
setlocal
call %CONDA_PREFIX%\Scripts\activate.bat gridcare-etl
python ingestion\fetch_grid_data.py
python ingestion\fetch_weather_data.py
docker compose exec spark bash -lc "cd /project && python3 pipelines/transform/bronze_to_silver.py"
docker compose exec spark bash -lc "export PG_HOST=db PG_USER=postgres PG_PASS=postgres PG_DB=postgres && cd /project && python3 pipelines/transform/silver_to_gold.py"
endlocal
