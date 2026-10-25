from datetime import datetime, timezone

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

HDFS = "hdfs://localhost:9000"
INPUT = f"{HDFS}/ecommerce_customer360/processed/customer_orders"
OUTPUT_BASE = f"{HDFS}/ecommerce_customer360/ml/customer_segmentation"

run_id = datetime.now(timezone.utc).strftime("run_%Y%m%d_%H%M%S")
OUTPUT = f"{OUTPUT_BASE}/{run_id}"

spark = (
    SparkSession.builder
    .appName("Customer360-RFM-KMeans")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

try:
    df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT)

    required = {
        "customer_unique_id",
        "order_id",
        "order_status",
        "order_purchase_timestamp",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    orders = (
        df.filter(F.col("customer_unique_id").isNotNull())
        .filter(F.col("order_id").isNotNull())
        .filter(F.to_timestamp("order_purchase_timestamp").isNotNull())
        .filter(F.lower(F.trim(F.col("order_status"))) == "delivered")
        .withColumn("order_ts", F.to_timestamp("order_purchase_timestamp"))
        .select("customer_unique_id", "order_id", "order_ts")
        .dropDuplicates(["customer_unique_id", "order_id"])
    )

    stats = orders.agg(
        F.count("*").alias("order_rows"),
        F.countDistinct("customer_unique_id").alias("customers"),
        F.max("order_ts").alias("latest_order"),
    ).first()

    if not stats["customers"] or stats["customers"] < 3:
        raise ValueError("Insufficient delivered-customer records for segmentation")

    # Use dataset-relative reference date, avoiding dependence on today's date.
    reference_date = F.date_add(F.to_date(F.lit(stats["latest_order"])), 1)

    rfm = orders.groupBy("customer_unique_id").agg(
        F.datediff(reference_date, F.max("order_ts")).cast("double").alias("recency_days"),
        F.countDistinct("order_id").cast("double").alias("frequency"),
    )

    # The supplied customer_orders schema has no price column.
    # Monetary value is intentionally excluded rather than fabricated.
    features = (
        rfm.withColumn("log_recency", F.log1p(F.col("recency_days")))
           .withColumn("log_frequency", F.log1p(F.col("frequency")))
           .select("customer_unique_id", "recency_days", "frequency",
                   "log_recency", "log_frequency")
           .cache()
    )

    assembler = VectorAssembler(
        inputCols=["log_recency", "log_frequency"],
        outputCol="raw_features",
        handleInvalid="error",
    )
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="features",
        withMean=True,
        withStd=True,
    )
    kmeans = KMeans(
        featuresCol="features",
        predictionCol="cluster",
        k=3,
        seed=42,
        maxIter=30,
    )
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(features)
    scored = model.transform(features)

    evaluator = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean",
    )
    silhouette = evaluator.evaluate(scored)

    labels = (
        scored.groupBy("cluster")
        .agg(
            F.count("*").alias("customer_count"),
            F.round(F.avg("recency_days"), 2).alias("avg_recency_days"),
            F.round(F.avg("frequency"), 2).alias("avg_frequency"),
        )
        .withColumn(
            "segment",
            F.when(
                (F.col("avg_frequency") >= 2) &
                (F.col("avg_recency_days") <=
                 F.lit(float(rfm.approxQuantile("recency_days", [0.5], 0.01)[0]))),
                "Frequent / Recent",
            ).when(
                F.col("avg_recency_days") >=
                F.lit(float(rfm.approxQuantile("recency_days", [0.5], 0.01)[0])),
                "Needs Reactivation",
            ).otherwise("Occasional Customers"),
        )
    )

    result = scored.join(
        labels.select("cluster", "segment"),
        on="cluster",
        how="left",
    ).select(
        "customer_unique_id", "recency_days", "frequency",
        "cluster", "segment",
    )

    # All outputs are under a unique run ID; existing datasets are untouched.
    result.write.mode("errorifexists").parquet(f"{OUTPUT}/customer_segments")
    labels.write.mode("errorifexists").option("header", True).csv(
        f"{OUTPUT}/segment_summary"
    )
    model.write().save(f"{OUTPUT}/model")

    print("\n===== CUSTOMER SEGMENTATION RESULTS =====")
    print(f"ML_RUN_ID={run_id}")
    print(f"ML_OUTPUT={OUTPUT}")
    print(f"DELIVERED_ORDERS={stats['order_rows']}")
    print(f"CUSTOMERS_SEGMENTED={features.count()}")
    print(f"KMEANS_K=3")
    print(f"SILHOUETTE={silhouette:.4f}")
    labels.orderBy("cluster").show(truncate=False)
    print("CUSTOMER_SEGMENTATION=PASS")

    features.unpersist()
finally:
    spark.stop()
