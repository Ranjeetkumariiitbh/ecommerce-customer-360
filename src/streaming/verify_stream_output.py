from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("VerifyEcommerceC360Streaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    path = (
        "hdfs://localhost:9000/"
        "ecommerce_customer360/processed/streaming/cart_events_enriched"
    )

    df = spark.read.parquet(path)

    print("STREAMING_OUTPUT_COUNT =", df.count())
    print("OUTPUT_COLUMNS =", df.columns)

    df.groupBy("event_type", "is_synthetic").count().show(
        20, truncate=False
    )

    print("SAMPLE_ENRICHED_EVENTS:")
    df.select(
        "event_id",
        "customer_id",
        "event_type",
        "product_id",
        "product_category_name",
        "event_timestamp",
        "is_synthetic",
    ).show(10, truncate=False)

    print(
        "ROWS_WITH_PRODUCT_CATEGORY =",
        df.filter(col("product_category_name").isNotNull()).count()
    )
finally:
    spark.stop()
