from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, session_window, count
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType
)

KAFKA_SERVERS = "127.0.0.1:9092"
TOPIC = "cart_events"

BASE = "hdfs://localhost:9000/ecommerce_customer360"

WINDOWED_OUTPUT = (
    f"{BASE}/processed/streaming/windowed_events"
)
WINDOWED_CHECKPOINT = (
    f"{BASE}/checkpoints/windowed_events"
)

SESSION_OUTPUT = (
    f"{BASE}/processed/streaming/customer_sessions"
)
SESSION_CHECKPOINT = (
    f"{BASE}/checkpoints/customer_sessions"
)

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("event_source", StringType(), True),
    StructField("is_synthetic", BooleanType(), True),
])

spark = (
    SparkSession.builder
    .appName("EcommerceC360WindowAggregation")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        kafka.selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_timestamp", to_timestamp("event_time"))
        .filter(
            col("customer_id").isNotNull()
            & col("event_timestamp").isNotNull()
            & col("event_type").isin(
                "view_product", "add_to_cart", "remove_from_cart"
            )
        )
    )

    # Tumbling 10-minute windows, sliding every 1 minute.
    sliding_counts = (
        events
        .withWatermark("event_timestamp", "20 minutes")
        .groupBy(
            window(
                col("event_timestamp"),
                "10 minutes",
                "1 minute"
            ),
            col("event_type"),
            col("is_synthetic")
        )
        .agg(count("*").alias("event_count"))
    )

    sliding_query = (
        sliding_counts.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", WINDOWED_OUTPUT)
        .option("checkpointLocation", WINDOWED_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    # Session window: each customer's activity session uses a
    # 10-minute inactivity gap. This is a separate aggregation.
    session_counts = (
        events
        .withWatermark("event_timestamp", "20 minutes")
        .groupBy(
            session_window(col("event_timestamp"), "10 minutes"),
            col("customer_id"),
            col("is_synthetic")
        )
        .agg(count("*").alias("session_event_count"))
    )

    session_query = (
        session_counts.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SESSION_OUTPUT)
        .option("checkpointLocation", SESSION_CHECKPOINT)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("WINDOWED_STREAM_STATUS=STARTED")
    print("Sliding aggregation: 10-minute window / 1-minute slide")
    print("Session aggregation: 10-minute inactivity gap")
    print("Windowed output:", WINDOWED_OUTPUT)
    print("Session output:", SESSION_OUTPUT)

    spark.streams.awaitAnyTermination()

finally:
    spark.stop()
