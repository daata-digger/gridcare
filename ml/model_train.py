
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator


GOLD = "/project/storage/gold"
MODEL_PATH = "/project/storage/ml/rf_load_model"


def main():
    spark = SparkSession.builder.appName("gridcare_model_train").getOrCreate()

    print("[ML] Reading gold iso_hourly_enriched")
    df = spark.read.parquet(f"{GOLD}/iso_hourly_enriched")

    # Only keep rows where we have weather. This is mostly CAISO right now.
    df = df.filter(
        (F.col("temperature").isNotNull())
        & (F.col("wind_speed").isNotNull())
        & (F.col("humidity").isNotNull())
    )

    print(f"[ML] Training rows after filter = {df.count()}")

    if df.count() < 100:
        raise SystemExit("[ML] Not enough rows with weather data to train a model")

    # Features
    df = df.withColumn("hour_of_day", F.hour("timestamp_utc"))
    df = df.withColumn("day_of_week", F.dayofweek("timestamp_utc"))

    feature_cols = [
        "hour_of_day",
        "day_of_week",
        "temperature",
        "wind_speed",
        "humidity",
        # one hot for iso_code
        "iso_vec",
    ]

    indexer = StringIndexer(
        inputCol="iso_code",
        outputCol="iso_index",
        handleInvalid="keep",
    )

    encoder = OneHotEncoder(
        inputCols=["iso_index"],
        outputCols=["iso_vec"],
    )

    assembler = VectorAssembler(
        inputCols=[
            "hour_of_day",
            "day_of_week",
            "temperature",
            "wind_speed",
            "humidity",
            "iso_vec",
        ],
        outputCol="features",
    )

    rf = RandomForestRegressor(
        labelCol="load_mw",
        featuresCol="features",
        numTrees=50,
        maxDepth=10,
        seed=42,
    )

    pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])

    train, test = df.randomSplit([0.8, 0.2], seed=42)

    print(f"[ML] Train rows = {train.count()}, Test rows = {test.count()}")

    model = pipeline.fit(train)

    preds = model.transform(test)

    evaluator_rmse = RegressionEvaluator(
        labelCol="load_mw",
        predictionCol="prediction",
        metricName="rmse",
    )
    evaluator_mae = RegressionEvaluator(
        labelCol="load_mw",
        predictionCol="prediction",
        metricName="mae",
    )

    rmse = evaluator_rmse.evaluate(preds)
    mae = evaluator_mae.evaluate(preds)

    print(f"[ML] Test RMSE = {rmse:.2f} MW")
    print(f"[ML] Test MAE  = {mae:.2f} MW")

    print(f"[ML] Saving model to {MODEL_PATH}")
    model.write().overwrite().save(MODEL_PATH)

    spark.stop()
    print("[ML] Training finished")


if __name__ == "__main__":
    main()
