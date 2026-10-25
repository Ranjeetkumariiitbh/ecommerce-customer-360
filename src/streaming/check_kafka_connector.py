from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("EcommerceC360KafkaConnectorCheck")
    .getOrCreate()
)

try:
    print("Spark version:", spark.version)

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "127.0.0.1:9092")
        .option("subscribe", "cart_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    print("Kafka source schema:")
    stream.printSchema()

    print("CONNECTOR_CHECK=PASS")
finally:
    spark.stop()
