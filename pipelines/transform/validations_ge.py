# pipelines/transform/validations_ge.py

from pyspark.sql import SparkSession, functions as F

BASE = "/project/storage"
BRONZE = f"{BASE}/bronze"
SILVER = f"{BASE}/silver"
GOLD = f"{BASE}/gold"


def check_non_empty(df, name, problems):
    cnt = df.count()
    print(f"[CHECK] {name} row count = {cnt}")
    if cnt == 0:
        problems.append(f"{name} is empty")


def check_null_ratio(df, col, max_ratio, name, problems):
    total = df.count()
    if total == 0:
        problems.append(f"{name} has zero rows when checking null ratio for {col}")
        return
    nulls = df.filter(F.col(col).isNull()).count()
    ratio = nulls / total
    print(f"[CHECK] {name}.{col} null ratio = {ratio:.3f}")
    if ratio > max_ratio:
        problems.append(
            f"{name}.{col} null ratio {ratio:.3f} is above allowed {max_ratio:.3f}"
        )


def check_range(df, col, lo, hi, name, problems):
    bad = df.filter((F.col(col) < lo) | (F.col(col) > hi)).count()
    print(f"[CHECK] {name}.{col} out-of-range rows = {bad} (allowed range {lo} to {hi})")
    if bad > 0:
        problems.append(
            f"{name}.{col} has {bad} rows outside [{lo}, {hi}]"
        )


def run_validations():
    spark = SparkSession.builder.appName("gridcare_validations").getOrCreate()

    problems: list[str] = []

    # ---------- Silver layer ----------
    print("\n[VALIDATE] Silver grid_clean")
    silver_grid = spark.read.parquet(f"{SILVER}/grid_clean")
    silver_grid.printSchema()
    check_non_empty(silver_grid, "silver.grid_clean", problems)
    check_null_ratio(silver_grid, "load_mw", 0.01, "silver.grid_clean", problems)

    print("\n[VALIDATE] Silver weather_clean")
    silver_weather = spark.read.parquet(f"{SILVER}/weather_clean")
    silver_weather.printSchema()
    check_non_empty(silver_weather, "silver.weather_clean", problems)
    for c in ["temperature", "wind_speed", "humidity"]:
        check_null_ratio(silver_weather, c, 0.10, "silver.weather_clean", problems)

    check_range(silver_weather, "temperature", -40.0, 130.0,
                "silver.weather_clean", problems)
    check_range(silver_weather, "humidity", 0.0, 100.0,
                "silver.weather_clean", problems)

    print("\n[VALIDATE] Silver grid_weather_enriched")
    enriched = spark.read.parquet(f"{SILVER}/grid_weather_enriched")
    enriched.printSchema()
    check_non_empty(enriched, "silver.grid_weather_enriched", problems)
    check_null_ratio(enriched, "load_mw", 0.0, "silver.grid_weather_enriched", problems)

    # ---------- Gold layer ----------
    print("\n[VALIDATE] Gold iso_hourly_enriched")
    gold_hourly = spark.read.parquet(f"{GOLD}/iso_hourly_enriched")
    gold_hourly.printSchema()
    check_non_empty(gold_hourly, "gold.iso_hourly_enriched", problems)

    print("\n[VALIDATE] Gold iso_daily_demand")
    gold_daily = spark.read.parquet(f"{GOLD}/iso_daily_demand")
    gold_daily.printSchema()
    check_non_empty(gold_daily, "gold.iso_daily_demand", problems)

    # Sanity check that daily sums roughly match hourly sums
    daily_from_hourly = (
        gold_hourly.groupBy("iso_code", F.to_date("timestamp_utc").alias("day"))
        .agg(F.sum("load_mw").alias("sum_load_mw_hourly"))
    )

    joined = (
        gold_daily.alias("d")
        .join(
            daily_from_hourly.alias("h"),
            on=["iso_code", "day"],
            how="left",
        )
    )

    mismatch = joined.filter(
        F.abs(F.col("d.sum_load_mw") - F.col("h.sum_load_mw_hourly")) > 1e-6
    ).count()

    print(f"[CHECK] daily sums vs hourly aggregation mismatches = {mismatch}")
    if mismatch > 0:
        problems.append("Gold daily sums do not match aggregation from hourly data")

    if problems:
        print("\n[RESULT] VALIDATION FAILED")
        for p in problems:
            print(" -", p)
        raise SystemExit(1)

    print("\n[RESULT] VALIDATION PASSED")
    spark.stop()


if __name__ == "__main__":
    run_validations()
