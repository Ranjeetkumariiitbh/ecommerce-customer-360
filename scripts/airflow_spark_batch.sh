#!/usr/bin/env bash
set -euo pipefail

cd /home/ranjeet/ecommerce-customer-360

export PYTHONPATH="/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
export PYSPARK_PYTHON=/usr/bin/python3

RUN_ID="${1:?Pass a run ID such as run_20261023_0100}"

if [[ ! "$RUN_ID" =~ ^run_[0-9]{8}_[0-9]{4}$ ]]; then
  echo "Invalid run ID format" >&2
  exit 2
fi

export CUSTOMER360_BATCH_OUT="hdfs://localhost:9000/ecommerce_customer360/processed/batch_runs/${RUN_ID}"

echo "Batch output: ${CUSTOMER360_BATCH_OUT}"

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  src/batch/process_olist.py
