from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("VerifyC360WindowOutputs")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

base = "hdfs://localhost:9000/ecommerce_customer360/processed/streaming"

for name in ["windowed_events", "customer_sessions"]:
    print(f"\n===== {name} =====", flush=True)

    try:
        df = spark.read.parquet(f"{base}/{name}")
        print("Columns:", df.columns, flush=True)

        count = df.count()
        print("Rows:", count, flush=True)

        if count:
            df.show(10, truncate=False)
        else:
            print("No finalized output rows yet.", flush=True)

    except Exception as exc:
        print("READ_STATUS=NOT_VERIFIED", flush=True)
        print(str(exc)[:1000], flush=True)

spark.stop()
