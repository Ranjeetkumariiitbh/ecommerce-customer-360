#!/usr/bin/env bash
set -euo pipefail

cd /home/ranjeet/ecommerce-customer-360

export PYTHONPATH="/opt/spark/python/lib/pyspark.zip:/opt/spark/python/lib/py4j-0.10.9.7-src.zip"
export PYSPARK_PYTHON=/usr/bin/python3

exec /opt/spark/bin/spark-submit \
  --master 'local[2]' \
  /tmp/ecommerce_spark_preflight.py
