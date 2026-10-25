import json
import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

INPUT = "hdfs://localhost:9000/ecommerce_customer360/processed/customer_orders"
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
BASE = f"hdfs://localhost:9000/ecommerce_customer360/ml/purchase_propensity_reworked/run_{RUN_ID}"
HORIZON_DAYS = 90
SEED = 42
TARGET_PRECISION = 0.70
TARGET_RECALL = 0.80

spark = (SparkSession.builder.appName("Customer360-PurchasePropensity-Reworked").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

def metrics_at_threshold(frame, threshold):
    scored = frame.withColumn(
        "pred", F.when(F.col("p1") >= F.lit(float(threshold)), 1.0).otherwise(0.0)
    )
    row = scored.agg(
        F.sum(F.when((F.col("label")==1) & (F.col("pred")==1), 1).otherwise(0)).alias("tp"),
        F.sum(F.when((F.col("label")==0) & (F.col("pred")==1), 1).otherwise(0)).alias("fp"),
        F.sum(F.when((F.col("label")==1) & (F.col("pred")==0), 1).otherwise(0)).alias("fn")
    ).first()
    tp, fp, fn = int(row.tp or 0), int(row.fp or 0), int(row.fn or 0)
    precision = tp/(tp+fp) if tp+fp else 0.0
    recall = tp/(tp+fn) if tp+fn else 0.0
    f1 = 2*precision*recall/(precision+recall) if precision+recall else 0.0
    return {"threshold": float(threshold), "precision": precision, "recall": recall,
            "f1": f1, "tp": tp, "fp": fp, "fn": fn}

try:
    raw = (spark.read.option("header", True).option("inferSchema", True)
           .option("timestampFormat", "yyyy-MM-dd HH:mm:ss").csv(INPUT))
    raw = raw.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    raw = raw.filter(F.col("customer_unique_id").isNotNull() &
                     F.col("order_purchase_timestamp").isNotNull())

    max_date = raw.agg(F.max("order_purchase_timestamp")).first()[0]
    cutoff = max_date - timedelta(days=HORIZON_DAYS)
    hist = raw.filter(F.col("order_purchase_timestamp") <= F.lit(cutoff))
    future = raw.filter((F.col("order_purchase_timestamp") > F.lit(cutoff)) &
                       (F.col("order_purchase_timestamp") <= F.lit(max_date)))

    # Customer-level features using only information available at cutoff.
    aggs = [
        F.countDistinct("order_id").cast("double").alias("order_count"),
        F.datediff(F.lit(cutoff), F.max("order_purchase_timestamp")).cast("double").alias("recency_days"),
        F.datediff(F.lit(cutoff), F.min("order_purchase_timestamp")).cast("double").alias("tenure_days"),
        F.countDistinct(F.to_date("order_purchase_timestamp")).cast("double").alias("active_purchase_days"),
        F.countDistinct("customer_city").cast("double").alias("city_count"),
        F.max("customer_state").alias("customer_state"),
        F.avg(F.dayofweek("order_purchase_timestamp")).cast("double").alias("avg_purchase_weekday"),
        F.avg(F.month("order_purchase_timestamp")).cast("double").alias("avg_purchase_month"),
        F.avg(F.hour("order_purchase_timestamp")).cast("double").alias("avg_purchase_hour"),
        F.sum(F.when(F.col("order_status")=="delivered",1).otherwise(0)).cast("double").alias("delivered_count"),
        F.sum(F.when(F.col("order_status")=="canceled",1).otherwise(0)).cast("double").alias("canceled_count"),
        F.sum(F.when(F.col("order_status")=="shipped",1).otherwise(0)).cast("double").alias("shipped_count"),
        F.sum(F.when(F.col("order_status")=="invoiced",1).otherwise(0)).cast("double").alias("invoiced_count"),
        F.sum(F.when(F.col("order_status")=="processing",1).otherwise(0)).cast("double").alias("processing_count"),
    ]
    for col, name, divisor in [
        ("order_approved_at","approval_delay_hours",3600.0),
        ("order_delivered_customer_date","delivery_duration_days",86400.0),
        ("order_estimated_delivery_date","estimated_duration_days",86400.0),
    ]:
        if col in hist.columns:
            aggs.append(F.avg(F.when(F.col(col).isNotNull(),
                (F.unix_timestamp(col)-F.unix_timestamp("order_purchase_timestamp"))/divisor
            )).cast("double").alias("avg_"+name))

    features = hist.groupBy("customer_unique_id").agg(*aggs)
    features = features.withColumn("orders_per_tenure",
        F.col("order_count")/(F.col("tenure_days")+1.0))
    features = features.withColumn("delivered_rate",
        F.col("delivered_count")/F.greatest(F.col("order_count"),F.lit(1.0)))
    features = features.withColumn("cancel_rate",
        F.col("canceled_count")/F.greatest(F.col("order_count"),F.lit(1.0)))
    labels = future.select("customer_unique_id").distinct().withColumn("label",F.lit(1.0))
    data = features.join(labels,"customer_unique_id","left").fillna({"label":0.0})
    numeric = [c for c in data.columns if c not in ("customer_unique_id","customer_state","label")]
    data = data.fillna({c:0.0 for c in numeric}).fillna({"customer_state":"UNKNOWN"})
    data = data.cache()

    print("RUN_ID", RUN_ID, "CUTOFF", cutoff)
    data.groupBy("label").count().show()
    positives = data.filter("label=1").count()
    negatives = data.filter("label=0").count()
    print("CUSTOMERS", data.count(), "POSITIVE", positives, "NEGATIVE", negatives)

    # Stratified customer split.
    pos, neg = data.filter("label=1"), data.filter("label=0")
    ptrain,pval,ptest = pos.randomSplit([.70,.15,.15],SEED)
    ntrain,nval,ntest = neg.randomSplit([.70,.15,.15],SEED)
    train = ptrain.unionByName(ntrain)
    validation = pval.unionByName(nval)
    test = ptest.unionByName(ntest)
    print("SPLIT_COUNTS")
    for name, frame in [("train",train),("validation",validation),("test",test)]:
        print(name, frame.groupBy("label").count().collect())

    # Class weighting; cap weight to avoid extreme instability on very rare labels.
    counts = {int(r["label"]): int(r["count"]) for r in train.groupBy("label").count().collect()}
    n0,n1 = max(counts.get(0,1),1),max(counts.get(1,1),1)
    pos_weight = min(float(n0)/float(n1), 100.0)
    train = train.withColumn("class_weight",
        F.when(F.col("label")==1.0,F.lit(pos_weight)).otherwise(F.lit(1.0)))

    indexer = StringIndexer(inputCol="customer_state",outputCol="state_idx",handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["state_idx"],outputCols=["state_vec"],handleInvalid="keep")
    numeric_cols = numeric
    assembler = VectorAssembler(inputCols=numeric_cols+["state_vec"],outputCol="features",handleInvalid="keep")

    # Single model fit; no repeated training loops. GBT handles nonlinear interactions.
    classifier = GBTClassifier(labelCol="label",featuresCol="features",weightCol="class_weight",
                              maxIter=80,maxDepth=4,stepSize=.05,seed=SEED)
    pipeline = Pipeline(stages=[indexer,encoder,assembler,classifier])
    model = pipeline.fit(train)

    prob_udf = F.udf(lambda v: float(v[1]) if v is not None and len(v)>1 else 0.0,"double")
    def score(frame):
        return model.transform(frame).withColumn("p1",prob_udf("probability")).select("label","p1","rawPrediction").cache()
    val_scored, test_scored = score(validation), score(test)

    thresholds = sorted(set([i/1000 for i in range(1,501)]+[.0001,.0005,.75,.9,.99]))
    val_metrics = [metrics_at_threshold(val_scored,t) for t in thresholds]
    feasible = [m for m in val_metrics if m["precision"]>=TARGET_PRECISION and m["recall"]>=TARGET_RECALL]
    pfeasible = [m for m in val_metrics if m["precision"]>=TARGET_PRECISION]
    if feasible:
        chosen=max(feasible,key=lambda m:m["f1"]); status="TARGETS_MET_VALIDATION"
    elif pfeasible:
        chosen=max(pfeasible,key=lambda m:(m["recall"],m["f1"])); status="PRECISION_MET_RECALL_NOT_MET"
    else:
        chosen=max(val_metrics,key=lambda m:m["f1"]); status="NO_THRESHOLD_MEETS_PRECISION_TARGET"

    test_metrics=metrics_at_threshold(test_scored,chosen["threshold"])
    evaluator=BinaryClassificationEvaluator(labelCol="label",rawPredictionCol="rawPrediction",metricName="areaUnderROC")
    roc_auc=evaluator.evaluate(test_scored)
    evaluator.setMetricName("areaUnderPR")
    pr_auc=evaluator.evaluate(test_scored)
    metrics={"run_id":RUN_ID,"feature_cutoff":str(cutoff),"horizon_days":HORIZON_DAYS,
      "positive_count":positives,"negative_count":negatives,"positive_weight":pos_weight,
      "target_precision":TARGET_PRECISION,"target_recall":TARGET_RECALL,
      "validation_status":status,"validation_selected_metrics":chosen,
      "test_metrics_at_validation_threshold":test_metrics,"test_roc_auc":roc_auc,"test_pr_auc":pr_auc,
      "note":"Threshold selected on validation only. Test set was not used for threshold selection."}

    model.write().overwrite().save(BASE+"/model")
    test_scored.withColumn("prediction_at_threshold",
      F.when(F.col("p1")>=F.lit(chosen["threshold"]),1.0).otherwise(0.0)
    ).write.mode("overwrite").parquet(BASE+"/test_predictions")
    spark.createDataFrame([(json.dumps(metrics),)],["metrics_json"]).write.mode("overwrite").parquet(BASE+"/metrics")
    print("VALIDATION_STATUS",status)
    print("VALIDATION_METRICS",json.dumps(chosen))
    print("TEST_METRICS",json.dumps(test_metrics))
    print("ROC_AUC",roc_auc,"PR_AUC",pr_auc)
    print("MODEL_PATH",BASE+"/model")
    print("PREDICTIONS_PATH",BASE+"/test_predictions")
    print("METRICS_PATH",BASE+"/metrics")
finally:
    spark.stop()
