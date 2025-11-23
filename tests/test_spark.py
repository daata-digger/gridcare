import os
from pathlib import Path

# Set JAVA_HOME if not set
java_home = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
if Path(java_home).exists():
    os.environ["JAVA_HOME"] = java_home
    print(f"Set JAVA_HOME to: {java_home}")

from pyspark.sql import SparkSession

print("Creating Spark session...")
spark = SparkSession.builder.appName("test").getOrCreate()
print("✅ Spark session created successfully!")

print("\nSpark version:", spark.version)
spark.stop()