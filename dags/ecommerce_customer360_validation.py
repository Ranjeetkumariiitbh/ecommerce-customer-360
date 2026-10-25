from datetime import datetime

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


WEBHDFS_URL = "http://host.docker.internal:9870/webhdfs/v1"

HDFS_PATHS = {
    "orders_clean": "/ecommerce_customer360/processed/orders_clean",
    "customers_clean": "/ecommerce_customer360/processed/customers_clean",
    "products_clean": "/ecommerce_customer360/processed/products_clean",
    "customer_orders": "/ecommerce_customer360/processed/customer_orders",
    "streaming": "/ecommerce_customer360/processed/streaming",
}


def validate_hdfs_outputs():
    session = requests.Session()

    for name, path in HDFS_PATHS.items():
        response = session.get(
            f"{WEBHDFS_URL}{path}",
            params={"op": "LISTSTATUS"},
            timeout=15,
        )
        response.raise_for_status()

        entries = response.json().get(
            "FileStatuses", {}
        ).get("FileStatus", [])

        if not entries:
            raise ValueError(
                f"HDFS output is empty: {path}"
            )

        print(
            f"PASS | {name} | "
            f"entries={len(entries)} | path={path}"
        )

    print("Ecommerce Customer 360 HDFS validation passed.")


with DAG(
    dag_id="ecommerce_customer360_validation",
    description="Read-only validation of existing Ecommerce Customer 360 HDFS outputs",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "customer360", "validation"],
) as dag:

    validate_outputs = PythonOperator(
        task_id="validate_hdfs_outputs",
        python_callable=validate_hdfs_outputs,
    )
