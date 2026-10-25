from datetime import datetime
import json

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

spark = (
    SparkSession.builder
    .appName("Customer360-Purchase-Propensity-RandomForest")
    .master("local[2]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

INPUT = "hdfs://localhost:9000/ecommerce_customer360/processed/customer_orders"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT = f"hdfs://localhost:9000/ecommerce_customer360/ml/purchase_propensity/run_{RUN_ID}"

# Load order history and define a strictly future label window.
df = (
    spark.read.option("header", True).option("inferSchema", True).csv(INPUT)
    .withColumn("ts", F.to_timestamp("order_purchase_timestamp"))
    .filter(F.col("customer_unique_id").isNotNull() & F.col("ts").isNotNull())
    .select("customer_unique_id", "order_id", "ts")
    .dropDuplicates(["customer_unique_id", "order_id"])
)

max_ts = df.agg(F.max("ts")).first()[0]
cutoff = max_ts - __import__("datetime").timedelta(days=90)

history = df.filter(F.col("ts") < F.lit(cutoff))
future = df.filter(
    (F.col("ts") >= F.lit(cutoff)) &
    (F.col("ts") <= F.lit(max_ts))
)

features = history.groupBy("customer_unique_id").agg(
    F.countDistinct("order_id").cast("double").alias("order_count"),
    F.datediff(F.lit(cutoff.date()), F.max("ts").cast("date"))
        .cast("double").alias("recency_days"),
    F.datediff(F.lit(cutoff.date()), F.min("ts").cast("date"))
        .cast("double").alias("tenure_days")
)

labels = future.groupBy("customer_unique_id").agg(
    F.lit(1.0).alias("label")
)

data = (
    features.join(labels, "customer_unique_id", "left")
    .fillna({"label": 0.0})
    .select("customer_unique_id", "order_count", "recency_days", "tenure_days", "label")
    .cache()
)

counts = {int(r["label"]): r["count"] for r in data.groupBy("label").count().collect()}
positive = counts.get(1, 0)
negative = counts.get(0, 0)

print(f"RUN_ID={RUN_ID}")
print(f"FEATURE_CUTOFF={cutoff}")
print(f"CUSTOMERS={data.count()}")
print(f"POSITIVE_LABELS={positive}")
print(f"NEGATIVE_LABELS={negative}")

if positive < 20 or negative < 20:
    raise RuntimeError("Not enough examples in one class to train and evaluate reliably.")

train, test = data.randomSplit([0.8, 0.2], seed=42)
train = train.cache()
test = test.cache()

train_counts = {int(r["label"]): r["count"] for r in train.groupBy("label").count().collect()}
train_positive = train_counts.get(1, 0)
train_negative = train_counts.get(0, 0)

if train_positive == 0 or train_negative == 0:
    raise RuntimeError("Training split is missing a class. Cannot train a binary classifier.")

# Compensate for the rare positive class using training data only.
positive_weight = float(train_negative) / float(train_positive)
train = train.withColumn(
    "class_weight",
    F.when(F.col("label") == 1.0, F.lit(positive_weight)).otherwise(F.lit(1.0))
)
test = test.withColumn("class_weight", F.lit(1.0))

assembler = VectorAssembler(
    inputCols=["order_count", "recency_days", "tenure_days"],
    outputCol="features"
)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    predictionCol="prediction",
    probabilityCol="probability",
    rawPredictionCol="rawPrediction",
    weightCol="class_weight",
    numTrees=100,
    maxDepth=8,
    seed=42
)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(train)
predictions = model.transform(test).cache()

auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
).evaluate(predictions)

pr_auc = BinaryClassificationEvaluator(
    labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderPR"
).evaluate(predictions)

f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
).evaluate(predictions)

precision = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedPrecision"
).evaluate(predictions)

recall = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="weightedRecall"
).evaluate(predictions)

tp = predictions.filter((F.col("label") == 1.0) & (F.col("prediction") == 1.0)).count()
fp = predictions.filter((F.col("label") == 0.0) & (F.col("prediction") == 1.0)).count()
fn = predictions.filter((F.col("label") == 1.0) & (F.col("prediction") == 0.0)).count()
positive_f1 = (2 * tp / (2 * tp + fp + fn)) if (2 * tp + fp + fn) else 0.0

print("===== RANDOM FOREST EVALUATION =====")
print(f"TEST_CUSTOMERS={test.count()}")
print(f"ROC_AUC={auc:.4f}")
print(f"PR_AUC={pr_auc:.4f}")
print(f"WEIGHTED_F1={f1:.4f}")
print(f"WEIGHTED_PRECISION={precision:.4f}")
print(f"WEIGHTED_RECALL={recall:.4f}")
print(f"POSITIVE_CLASS_PRECISION={(tp / (tp + fp)) if (tp + fp) else 0.0:.4f}")
print(f"POSITIVE_CLASS_RECALL={(tp / (tp + fn)) if (tp + fn) else 0.0:.4f}")
print(f"POSITIVE_CLASS_F1={positive_f1:.4f}")
print(f"TRUE_POSITIVES={tp} FALSE_POSITIVES={fp} FALSE_NEGATIVES={fn}")

rf_model = model.stages[-1]
importance = {
    name: float(value)
    for name, value in zip(
        ["order_count", "recency_days", "tenure_days"],
        rf_model.featureImportances.toArray()
    )
}
print("FEATURE_IMPORTANCE=" + json.dumps(importance, sort_keys=True))

# Persist artifacts to a unique run directory; never overwrite earlier runs.
model.write().save(f"{OUTPUT}/model")
predictions.select(
    "customer_unique_id", "label", "prediction", "probability",
    "order_count", "recency_days", "tenure_days"
).write.mode("errorifexists").parquet(f"{OUTPUT}/test_predictions")

metrics = [{
    "run_id": RUN_ID,
    "feature_cutoff": str(cutoff),
    "label_window_end": str(max_ts),
    "customers": int(data.count()),
    "train_customers": int(train.count()),
    "test_customers": int(test.count()),
    "positive_labels": int(positive),
    "roc_auc": float(auc),
    "pr_auc": float(pr_auc),
    "weighted_f1": float(f1),
    "weighted_precision": float(precision),
    "weighted_recall": float(recall),
    "positive_class_precision": float(tp / (tp + fp)) if (tp + fp) else 0.0,
    "positive_class_recall": float(tp / (tp + fn)) if (tp + fn) else 0.0,
    "positive_class_f1": float(positive_f1),
    "true_positives": int(tp),
    "false_positives": int(fp),
    "false_negatives": int(fn),
    "feature_importance": json.dumps(importance, sort_keys=True)
}]
spark.createDataFrame(metrics).coalesce(1).write.mode("errorifexists").json(
    f"{OUTPUT}/metrics"
)

print(f"MODEL_PATH={OUTPUT}/model")
print(f"PREDICTIONS_PATH={OUTPUT}/test_predictions")
print(f"METRICS_PATH={OUTPUT}/metrics")
print("PURCHASE_PROPENSITY=PASS")

predictions.unpersist()
train.unpersist()
test.unpersist()
data.unpersist()
spark.stop()
