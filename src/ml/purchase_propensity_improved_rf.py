import os
import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

spark = (
    SparkSession.builder
    .appName("Customer360-Improved-RF")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

INPUT = "hdfs://localhost:9000/ecommerce_customer360/processed/customer_orders"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE = f"hdfs://localhost:9000/ecommerce_customer360/ml/purchase_propensity_improved_rf/run_{RUN_ID}"

HORIZON_DAYS = 90
SEED = 42

raw = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .csv(INPUT)
)

raw = raw.filter(
    F.col("customer_unique_id").isNotNull()
    & F.col("order_purchase_timestamp").isNotNull()
)

max_date = raw.agg(F.max("order_purchase_timestamp")).first()[0]
cutoff = max_date - timedelta(days=HORIZON_DAYS)

history = raw.filter(F.col("order_purchase_timestamp") <= F.lit(cutoff))
future = raw.filter(
    (F.col("order_purchase_timestamp") > F.lit(cutoff))
    & (F.col("order_purchase_timestamp") <= F.lit(max_date))
)

# One row per order is confirmed in the source.
# Aggregate status and delivery behavior using historical orders only.
aggs = [
    F.countDistinct("order_id").cast("double").alias("order_count"),
    F.datediff(
        F.lit(cutoff), F.max("order_purchase_timestamp")
    ).cast("double").alias("recency_days"),
    F.datediff(
        F.lit(cutoff), F.min("order_purchase_timestamp")
    ).cast("double").alias("tenure_days"),
    F.countDistinct(F.to_date("order_purchase_timestamp"))
    .cast("double").alias("active_purchase_days"),
    F.countDistinct("customer_city").cast("double").alias("city_count"),
]

statuses = [
    "delivered", "canceled", "shipped",
    "unavailable", "invoiced", "processing",
    "created", "approved"
]

for status in statuses:
    aggs.append(
        F.sum(
            F.when(F.col("order_status") == status, 1.0)
            .otherwise(0.0)
        ).alias(f"{status}_count")
    )

for col, name, divisor in [
    ("order_approved_at", "approval_delay_hours", 3600.0),
    ("order_delivered_customer_date", "delivery_duration_days", 86400.0),
    ("order_estimated_delivery_date", "estimated_duration_days", 86400.0),
]:
    if col in history.columns:
        aggs.append(
            F.avg(
                F.when(
                    F.col(col).isNotNull(),
                    (
                        F.unix_timestamp(col)
                        - F.unix_timestamp("order_purchase_timestamp")
                    ) / divisor,
                )
            ).alias(f"avg_{name}")
        )

if (
    "order_delivered_customer_date" in history.columns
    and "order_estimated_delivery_date" in history.columns
):
    aggs.append(
        F.sum(
            F.when(
                F.col("order_delivered_customer_date").isNotNull()
                & F.col("order_estimated_delivery_date").isNotNull()
                & (
                    F.col("order_delivered_customer_date")
                    > F.col("order_estimated_delivery_date")
                ),
                1.0,
            ).otherwise(0.0)
        ).alias("late_delivery_count")
    )

aggs.append(F.max("customer_state").alias("customer_state"))

features = history.groupBy("customer_unique_id").agg(*aggs)

features = (
    features
    .withColumn(
        "orders_per_tenure",
        F.col("order_count") / (F.col("tenure_days") + 1.0),
    )
    .withColumn(
        "delivered_rate",
        F.col("delivered_count")
        / F.greatest(F.col("order_count"), F.lit(1.0)),
    )
    .withColumn(
        "cancel_rate",
        F.col("canceled_count")
        / F.greatest(F.col("order_count"), F.lit(1.0)),
    )
    .withColumn(
        "late_delivery_rate",
        F.col("late_delivery_count")
        / F.greatest(F.col("delivered_count"), F.lit(1.0)),
    )
)

labels = (
    future.select("customer_unique_id")
    .distinct()
    .withColumn("label", F.lit(1.0))
)

data = features.join(labels, "customer_unique_id", "left")
data = data.fillna({"label": 0.0, "customer_state": "UNKNOWN"})

numeric_cols = [
    c for c in data.columns
    if c not in ("customer_unique_id", "customer_state", "label")
]

data = data.fillna({c: 0.0 for c in numeric_cols}).cache()

print("\n===== IMPROVED RF DATA =====")
print("RUN_ID:", RUN_ID)
print("CUTOFF:", cutoff)
print("CUSTOMERS:", data.count())
data.groupBy("label").count().show()

positive = data.filter("label = 1.0")
negative = data.filter("label = 0.0")

p_train, p_val, p_test = positive.randomSplit([0.70, 0.15, 0.15], SEED)
n_train, n_val, n_test = negative.randomSplit([0.70, 0.15, 0.15], SEED)

train = p_train.unionByName(n_train)
validation = p_val.unionByName(n_val)
test = p_test.unionByName(n_test)

counts = {
    int(r["label"]): int(r["count"])
    for r in train.groupBy("label").count().collect()
}

n0 = max(counts.get(0, 1), 1)
n1 = max(counts.get(1, 1), 1)

# Keep a capped positive weight for stability.
positive_weight = min(float(n0) / float(n1), 100.0)

train = train.withColumn(
    "class_weight",
    F.when(F.col("label") == 1.0, F.lit(positive_weight))
    .otherwise(F.lit(1.0)),
)

indexer = StringIndexer(
    inputCol="customer_state",
    outputCol="state_index",
    handleInvalid="keep",
)

encoder = OneHotEncoder(
    inputCols=["state_index"],
    outputCols=["state_vector"],
    handleInvalid="keep",
)

assembler = VectorAssembler(
    inputCols=numeric_cols + ["state_vector"],
    outputCol="features",
    handleInvalid="keep",
)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    weightCol="class_weight",
    numTrees=100,
    maxDepth=6,
    minInstancesPerNode=2,
    seed=SEED,
)

pipeline = Pipeline(stages=[indexer, encoder, assembler, rf])
model = pipeline.fit(train)

def score(frame):
    scored = model.transform(frame)
    prob_udf = F.udf(
        lambda v: float(v[1]) if v is not None and len(v) > 1 else 0.0,
        "double",
    )
    return scored.withColumn("p1", prob_udf("probability"))

val_scored = score(validation).cache()
test_scored = score(test).cache()

auc_eval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC",
)

pr_eval = BinaryClassificationEvaluator(
    labelCol="label",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderPR",
)

val_auc = auc_eval.evaluate(val_scored)
val_pr = pr_eval.evaluate(val_scored)
test_auc = auc_eval.evaluate(test_scored)
test_pr = pr_eval.evaluate(test_scored)

# Choose threshold from validation only; test is evaluated once afterward.
thresholds = [i / 1000.0 for i in range(1, 501)]

best = None
for threshold in thresholds:
    pred = val_scored.withColumn(
        "pred", (F.col("p1") >= F.lit(threshold)).cast("double")
    )

    row = pred.agg(
        F.sum(F.when((F.col("label") == 1) & (F.col("pred") == 1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("label") == 0) & (F.col("pred") == 1), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("label") == 1) & (F.col("pred") == 0), 1).otherwise(0)).alias("fn"),
    ).first()

    tp, fp, fn = int(row.tp or 0), int(row.fp or 0), int(row.fn or 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    if best is None or f1 > best["f1"]:
        best = {
            "threshold": threshold,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": precision,
            "recall": recall,
            "f1": f1,
        }

threshold = best["threshold"]

test_pred = test_scored.withColumn(
    "prediction_at_threshold",
    (F.col("p1") >= F.lit(threshold)).cast("double"),
)

test_row = test_pred.agg(
    F.sum(F.when((F.col("label") == 1) & (F.col("prediction_at_threshold") == 1), 1).otherwise(0)).alias("tp"),
    F.sum(F.when((F.col("label") == 0) & (F.col("prediction_at_threshold") == 1), 1).otherwise(0)).alias("fp"),
    F.sum(F.when((F.col("label") == 1) & (F.col("prediction_at_threshold") == 0), 1).otherwise(0)).alias("fn"),
).first()

tp, fp, fn = int(test_row.tp or 0), int(test_row.fp or 0), int(test_row.fn or 0)
precision = tp / (tp + fp) if tp + fp else 0.0
recall = tp / (tp + fn) if tp + fn else 0.0
f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

metrics = {
    "run_id": RUN_ID,
    "cutoff": str(cutoff),
    "horizon_days": HORIZON_DAYS,
    "train_positive_weight": positive_weight,
    "validation_roc_auc": val_auc,
    "validation_pr_auc": val_pr,
    "validation_threshold_metrics": best,
    "test_roc_auc": test_auc,
    "test_pr_auc": test_pr,
    "test_threshold": threshold,
    "test_precision": precision,
    "test_recall": recall,
    "test_f1": f1,
    "test_tp": tp,
    "test_fp": fp,
    "test_fn": fn,
    "numeric_features": numeric_cols,
}

model.write().overwrite().save(f"{BASE}/model")
test_pred.select(
    "customer_unique_id", "label", "p1", "prediction_at_threshold"
).write.mode("overwrite").parquet(f"{BASE}/test_predictions")

spark.sparkContext.parallelize([json.dumps(metrics, indent=2)]).saveAsTextFile(
    f"{BASE}/metrics"
)

print("\n===== IMPROVED RF RESULTS =====")
print(json.dumps(metrics, indent=2))
print("MODEL_PATH:", f"{BASE}/model")
print("PREDICTIONS_PATH:", f"{BASE}/test_predictions")
print("METRICS_PATH:", f"{BASE}/metrics")

spark.stop()
