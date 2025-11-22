# pipelines/transform/silver_to_gold.py
import os
from pathlib import Path
import pandas as pd

def stringify_ts(df, cols):
    """Return a new DF with given timestamp/date cols converted to ISO strings."""
    out = df
    for c in cols:
        if c in out.columns:
            out = out.withColumn(c, F.date_format(F.col(c), "yyyy-MM-dd HH:mm:ss"))
    return out


from pyspark.sql import SparkSession, functions as F

SILVER_GRID = "storage/silver/grid_clean"
SILVER_WEATHER = "storage/silver/weather_clean"
SILVER_ENRICHED = "storage/silver/grid_weather_enriched"

GOLD_ROOT = "storage/gold"
GOLD_HOURLY = f"{GOLD_ROOT}/iso_hourly_demand"
GOLD_DAILY = f"{GOLD_ROOT}/iso_daily_demand"
GOLD_ENRICH = f"{GOLD_ROOT}/iso_hourly_enriched"

# Postgres config (matches docker-compose)
PG_HOST = os.getenv("PG_HOST", "gridcare_db")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASS", "postgres")

def spark_session():
    return (
        SparkSession.builder.appName("silver_to_gold")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

def write_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def df_to_postgres(pdf: pd.DataFrame, table: str):
    from sqlalchemy import create_engine
    url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    engine = create_engine(url)
    # small safety: cast pandas datetime to naive UTC strings for portability
    for c in pdf.columns:
        if pd.api.types.is_datetime64_any_dtype(pdf[c]):
            pdf[c] = pd.to_datetime(pdf[c], utc=True).dt.tz_convert("UTC").dt.tz_localize(None)
    pdf.to_sql(table, engine, if_exists="replace", index=False)

def main():
    spark = spark_session()

    # Prefer enriched if available, else join grid+weather on hour
    if Path(SILVER_ENRICHED).exists():
        enriched = spark.read.parquet(SILVER_ENRICHED)
    else:
        grid = spark.read.parquet(SILVER_GRID)
        weather = spark.read.parquet(SILVER_WEATHER)
        g = grid.withColumn("hour", F.date_trunc("hour", F.col("timestamp_utc")))
        w = weather.withColumn("hour", F.date_trunc("hour", F.col("timestamp_utc")))
        enriched = (
            g.join(w, on=[g.iso_code == w.iso_code, g.hour == w.hour], how="left")
             .select(
                 g.iso_code.alias("iso_code"),
                 g.timestamp_utc.alias("timestamp_utc"),
                 g.load_mw.alias("load_mw"),
                 F.col("temperature").cast("double").alias("temperature"),
                 F.col("wind_speed").cast("double").alias("wind_speed"),
                 F.col("humidity").cast("double").alias("humidity"),
                 F.col("solar_irradiance_proxy").cast("double").alias("solar_irradiance_proxy"),
             )
        )

    # Gold 1: ISO hourly demand
    hourly = (
        enriched
        .withColumn("hour", F.date_trunc("hour", F.col("timestamp_utc")))
        .groupBy("iso_code", "hour")
        .agg(
            F.avg("load_mw").alias("avg_load_mw"),
            F.max("load_mw").alias("max_load_mw"),
            F.min("load_mw").alias("min_load_mw"),
            F.last("load_mw", ignorenulls=True).alias("last_load_mw"),
            F.avg("temperature").alias("avg_temp"),
            F.avg("wind_speed").alias("avg_wind"),
            F.avg("humidity").alias("avg_humidity")
        )
        .orderBy("iso_code", "hour")
    )

    # Gold 2: ISO daily demand
    daily = (
        enriched
        .withColumn("day", F.to_date(F.col("timestamp_utc")))
        .groupBy("iso_code", "day")
        .agg(
            F.sum("load_mw").alias("sum_load_mw"),
            F.avg("load_mw").alias("avg_load_mw"),
            F.max("load_mw").alias("max_load_mw"),
            F.min("load_mw").alias("min_load_mw")
        )
        .orderBy("iso_code", "day")
    )

    # Gold 3: Enriched hourly rows for direct charting
    enr = (
        enriched
        .withColumn("hour", F.date_trunc("hour", F.col("timestamp_utc")))
        .select(
            "iso_code", "timestamp_utc", "hour", "load_mw",
            "temperature", "wind_speed", "humidity", "solar_irradiance_proxy"
        )
        .orderBy("iso_code", "timestamp_utc")
    )

    # Write Gold parquet
    write_parquet(hourly, GOLD_HOURLY)
    write_parquet(daily, GOLD_DAILY)
    write_parquet(enr, GOLD_ENRICH)

    # Load to Postgres using pandas
    print("[POSTGRES] installing client libraries if missing...")
    try:
        import psycopg2  # noqa
        import sqlalchemy  # noqa
    except Exception:
        import subprocess, sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "psycopg2-binary", "SQLAlchemy"])

    print("[POSTGRES] writing tables...")
    hourly_str = stringify_ts(hourly, ["hour"])
    df_to_postgres(hourly_str.toPandas(), "grid_iso_hourly_demand")

    # daily has a date column; stringify to keep it simple
    daily_str = daily.withColumn("day", F.date_format(F.col("day"), "yyyy-MM-dd"))
    df_to_postgres(daily_str.toPandas(), "grid_iso_daily_demand")

    enr_str = stringify_ts(enr, ["timestamp_utc", "hour"])
    df_to_postgres(enr_str.toPandas(), "grid_iso_hourly_enriched")


    print(f"[GOLD] parquet → {GOLD_ROOT}")
    print("[POSTGRES] tables → grid_iso_hourly_demand, grid_iso_daily_demand, grid_iso_hourly_enriched")

    spark.stop()

if __name__ == "__main__":
    main()
