from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


BRONZE_GRID_ROOT = "storage/bronze/grid/load"
BRONZE_WEATHER_ROOT = "storage/bronze/weather"
SILVER_GRID = "storage/silver/grid_clean"
SILVER_WEATHER = "storage/silver/weather_clean"
SILVER_ENRICHED = "storage/silver/grid_weather_enriched"


def spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_to_silver")
        # Parse files even if some are missing or slightly malformed
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.sql.files.ignoreMissingFiles", "true")
        # Keep legacy parser for lenient date handling
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Force the session timezone to UTC so timestamps are consistent
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _iso_from_path(col):
    # Works with file:/// URLs and both separators
    # Example: file:///project/storage/bronze/grid/load/NYISO/2025-11-21/NYISO_load.parquet
    p = F.input_file_name()
    # First split on '/grid/load/' or '\grid\load\' then take the next path token
    return F.coalesce(
        F.element_at(F.split(F.element_at(F.split(p, "/grid/load/"), 2), "/"), 1),
        F.element_at(F.split(F.element_at(F.split(p, r"\\grid\\load\\"),
                                         2), r"\\"), 1),
    )


def _weather_iso_from_path():
    p = F.input_file_name()
    return F.coalesce(
        F.element_at(F.split(F.element_at(F.split(p, "/weather/"), 2), "/"), 1),
        F.element_at(F.split(F.element_at(F.split(p, r"\\weather\\"),
                                         2), r"\\"), 1),
    )


def _parse_grid_timestamp(df):
    """
    Try several grid timestamp fields and numeric epochs.
    Assumes any local to UTC conversion was already done in ingestion for those ISOs that need it.
    """
    candidates = [
        "timestamp",          # preferred if present
        "time_utc",
        "Time",
        "Interval Start",
        "Interval End",
        "time",
        "begin",
        "interval",
        "x",
        "datetime",
        "date",
        "endtime",
        "starttime",
    ]
    # Coalesce available columns to a string
    ts_str = None
    for c in candidates:
        if c in df.columns:
            ts_str = F.coalesce(ts_str, F.col(c).cast("string")) if ts_str is not None else F.col(c).cast("string")

    if ts_str is None:
        return df.withColumn("timestamp_utc_tmp", F.lit(None).cast("timestamp"))

    # Try to parse as standard timestamp string
    ts_as_ts = F.to_timestamp(ts_str)

    # If numeric, decide between seconds and milliseconds based on magnitude
    only_numeric = F.when(ts_str.rlike(r"^\d+(\.\d+)?$"), ts_str).cast("double")
    ts_from_ms = F.to_timestamp(F.from_unixtime(only_numeric / F.lit(1000.0)))
    ts_from_sec = F.to_timestamp(F.from_unixtime(only_numeric))

    ts_epoch = F.when(only_numeric > F.lit(1e11), ts_from_ms).otherwise(ts_from_sec)

    return df.withColumn("timestamp_utc_tmp", F.coalesce(ts_as_ts, ts_epoch))


def _parse_grid_load(df):
    load_candidates = ["load", "Load", "value", "MW", "MWh", "y"]
    load_col = None
    for c in load_candidates:
        if c in df.columns:
            load_col = F.coalesce(load_col, F.col(c).cast("double")) if load_col is not None else F.col(c).cast("double")
    return df.withColumn("load_mw_tmp", load_col if load_col is not None else F.lit(None).cast("double"))


def load_grid_bronze(spark: SparkSession):
    print("Reading Bronze grid data...")

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(BRONZE_GRID_ROOT)
        .withColumn("input_file", F.input_file_name())
    )

    # Robust iso_code from path for both Linux and Windows style
    p = F.col("input_file")
    iso1 = F.regexp_extract(p, r"grid[\\/]+load[\\/]+([^\\/]+)[\\/]+", 1)
    # as a fallback, split by known anchor and pick next part
    iso2 = F.element_at(F.split(F.element_at(F.split(p, "/grid/load/"), 2), "/"), 1)
    iso3 = F.element_at(F.split(F.element_at(F.split(p, r"\\grid\\load\\"),
                                             2), r"\\"), 1)
    iso_code = F.coalesce(iso1, iso2, iso3)

    # Candidate timestamp and load columns
    ts_candidates = ["timestamp","Time","Interval End","Interval Start","time","time_utc","begin","interval","x","datetime","date","endtime","starttime"]
    load_candidates = ["load","Load","value","MW","MWh","y"]

    ts_cols = [F.col(c).cast("string") for c in ts_candidates if c in df.columns]
    ts_str = F.coalesce(*ts_cols) if ts_cols else None
    load_cols = [F.col(c).cast("double") for c in load_candidates if c in df.columns]
    load_num = F.coalesce(*load_cols) if load_cols else None

    if ts_str is None or load_num is None:
        raise SystemExit("[GRID] Missing timestamp or load columns in Bronze grid")

    # Two stage parse to catch strings like 'YYYY-MM-DD HH:MM:SS' first
    ts_fmt = F.to_timestamp(ts_str, "yyyy-MM-dd HH:mm:ss")
    # Then a generic attempt including numeric epoch seconds or ms
    ts_try = F.to_timestamp(ts_str)
    ts_num = F.when(ts_str.rlike(r"^\d+(\.\d+)?$"), ts_str).cast("double")
    ts_from_ms = F.to_timestamp(F.from_unixtime(ts_num / F.lit(1000.0)))
    ts_from_sec = F.to_timestamp(F.from_unixtime(ts_num))
    ts_num_parsed = F.when(ts_num > F.lit(1e11), ts_from_ms).otherwise(ts_from_sec)

    ts_utc = F.coalesce(ts_fmt, ts_try, ts_num_parsed)

    cleaned = (
        df.select(
            iso_code.alias("iso_code"),
            ts_utc.alias("timestamp_utc"),
            load_num.alias("load_mw"),
        )
        .where(F.col("iso_code").isNotNull() & (F.col("iso_code") != ""))
        .where(F.col("timestamp_utc").isNotNull() & F.col("load_mw").isNotNull())
        .dropDuplicates(["iso_code", "timestamp_utc"])
    )

    cleaned.write.mode("overwrite").parquet(SILVER_GRID)
    print(f"[GRID] Wrote Silver grid → {SILVER_GRID}")
    cleaned.groupBy("iso_code").count().orderBy("iso_code").show(200, truncate=False)
    return cleaned


def _parse_wind_speed_to_mph(col):
    """
    Handles values like "10 mph", "5 to 10 mph", "12", "20 km/h".
    We extract the first numeric and cast to double. Units other than mph are left as is
    since we cannot reliably infer without more context. Adjust if you want strict mph.
    """
    first_num = F.regexp_extract(col, r"(\d+(\.\d+)?)", 1)
    return F.when(first_num == "", None).otherwise(first_num.cast("double"))


def load_weather_bronze(spark):
    from pyspark.sql import functions as F

    print("Reading Bronze weather data...")

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(BRONZE_WEATHER_ROOT)  # storage/bronze/weather
        .withColumn("input_file", F.input_file_name())
    )

    # Derive iso_code from the file path, handling both Linux and Windows separators
    path = F.col("input_file")
    iso_from_fwd = F.element_at(F.split(F.element_at(F.split(path, "/weather/"), 2), "/"), 1)
    iso_from_bwd = F.element_at(F.split(F.element_at(F.split(path, r"\\weather\\"), 2), r"\\"), 1)
    iso_code = F.coalesce(iso_from_fwd, iso_from_bwd)

    # Our Bronze writer saved UTC-naive strings like "YYYY-MM-DD HH:MM:SS"
    ts_utc = F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")

    weather_clean = (
        df.select(
            iso_code.alias("iso_code"),
            ts_utc.alias("timestamp_utc"),
            F.col("temperature").cast("double").alias("temperature"),
            F.col("wind_speed").cast("double").alias("wind_speed"),
            F.col("humidity").cast("double").alias("humidity"),
        )
        .dropna(subset=["timestamp_utc"])
    )

    weather_clean.write.mode("overwrite").parquet(SILVER_WEATHER)
    print(f"[WEATHER] Wrote Silver weather → {SILVER_WEATHER}")
    weather_clean.groupBy("iso_code").count().orderBy("iso_code").show(200, truncate=False)
    return weather_clean


def enrich_grid_with_weather(grid_clean, weather_clean):
    if grid_clean is None or weather_clean is None:
        print("[ENRICH] Missing inputs, skipping")
        return

    print("Joining grid and weather using nearest timestamp within 60 minutes per iso_code")

    # Prepare
    gc = grid_clean.select(
        F.col("iso_code").alias("iso_g"),
        F.col("timestamp_utc").alias("ts_g"),
        F.col("load_mw")
    )
    wc = weather_clean.select(
        F.col("iso_code").alias("iso_w"),
        F.col("timestamp_utc").alias("ts_w"),
        F.col("temperature"),
        F.col("wind_speed"),
        F.col("humidity")
    )

    # Candidate pairs within 60 minutes
    cand = (
        gc.join(wc, gc.iso_g == wc.iso_w, "left")
          .withColumn("dt_sec", F.abs(F.unix_timestamp("ts_g") - F.unix_timestamp("ts_w")))
          .where(F.col("dt_sec") <= F.lit(3600))
    )

    # Pick closest weather per grid timestamp
    w = Window.partitionBy("iso_g", "ts_g").orderBy(F.col("dt_sec").asc())
    nearest = cand.withColumn("rn", F.row_number().over(w)).where(F.col("rn") == 1)

    enriched = (
        nearest.select(
            F.col("iso_g").alias("iso_code"),
            F.col("ts_g").alias("timestamp_utc"),
            F.date_trunc("hour", F.col("ts_g")).alias("hour"),
            F.col("load_mw"),
            F.col("temperature").cast("double"),
            F.col("wind_speed").cast("double"),
            F.col("humidity").cast("double"),
            F.lit(None).cast("double").alias("solar_irradiance_proxy"),
        )
    )

    enriched.write.mode("overwrite").parquet(SILVER_ENRICHED)
    print(f"[ENRICH] Wrote Silver enriched → {SILVER_ENRICHED}")
    enriched.groupBy("iso_code").count().orderBy("iso_code").show(200, truncate=False)


def main():
    spark = spark_session()
    try:
        grid_clean = load_grid_bronze(spark)
        weather_clean = load_weather_bronze(spark)
        enrich_grid_with_weather(grid_clean, weather_clean)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
