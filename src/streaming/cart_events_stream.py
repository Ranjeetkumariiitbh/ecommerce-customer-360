from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, broadcast, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType
)

KAFKA_SERVERS = "127.0.0.1:9092"
TOPIC = "cart_events"

PRODUCTS_PATH = (
    "hdfs://localhost:9000/ecommerce_customer360/processed/products_clean"
)
OUTPUT_PATH = (
    "hdfs://localhost:9000/ecommerce_customer360/processed/streaming/cart_events_enriched"
)
CHECKPOINT_PATH = (
    "hdfs://localhost:9000/ecommerce_customer360/checkpoints/cart_events_enriched"
)

event_schema = StructType([
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
    .appName("EcommerceC360CartEvents")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    # Static product dimension from the already-cleaned HDFS dataset.
    products = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(PRODUCTS_PATH)
        .select(
            col("product_id"),
            col("product_category_name")
        )
        .dropDuplicates(["product_id"])
    )

    # Live Kafka stream.
    kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = (
        kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), event_schema).alias("event"))
        .select("event.*")
        .withColumn("event_timestamp", to_timestamp("event_time"))
        .filter(
            col("event_id").isNotNull()
            & col("customer_id").isNotNull()
            & col("event_type").isin(
                "view_product", "add_to_cart", "remove_from_cart"
            )
            & col("event_timestamp").isNotNull()
        )
    )

    # Broadcast the small product dimension to avoid a shuffle join.
    enriched = (
        parsed.join(
            broadcast(products),
            on="product_id",
            how="left"
        )
        .withColumn("processed_at", current_timestamp())
    )

    query = (
        enriched.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("STREAM_STATUS=STARTED")
    print("Kafka topic:", TOPIC)
    print("Output:", OUTPUT_PATH)
    print("Checkpoint:", CHECKPOINT_PATH)

    query.awaitTermination()

finally:
    spark.stop()
