from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, count, col

spark = (
    SparkSession.builder
    .appName("DiagnoseC360EventTimes")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

path = (
    "hdfs://localhost:9000/ecommerce_customer360/"
    "processed/streaming/cart_events_enriched"
)

df = spark.read.parquet(path)

print("===== ENRICHED EVENT TIMESTAMPS =====")
df.select(
    count("*").alias("rows"),
    min("event_timestamp").alias("earliest_event"),
    max("event_timestamp").alias("latest_event"),
).show(truncate=False)

print("===== EVENT TYPES =====")
df.groupBy("event_type").count().show(truncate=False)

print("===== RECENT SAMPLE =====")
df.select(
    "event_id", "event_type", "event_time",
    "event_timestamp", "is_synthetic"
).orderBy(col("event_timestamp").desc()).show(10, truncate=False)

spark.stop()
