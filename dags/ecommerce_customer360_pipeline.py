from datetime import datetime
import subprocess
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

SSH_KEY = "/opt/airflow/ecommerce_airflow_ed25519"
SSH_HOST = "172.23.62.202"
SSH_USER = "ranjeet"
WEBHDFS_URL = "http://host.docker.internal:9870/webhdfs/v1"

BASE_PATHS = {
    "orders_clean": "/ecommerce_customer360/processed/orders_clean",
    "customers_clean": "/ecommerce_customer360/processed/customers_clean",
    "products_clean": "/ecommerce_customer360/processed/products_clean",
    "customer_orders": "/ecommerce_customer360/processed/customer_orders",
    "streaming": "/ecommerce_customer360/processed/streaming",
}


def ssh_run(remote_command=None, timeout=900):
    command = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "IdentitiesOnly=yes",
        "-o", "BatchMode=yes",
        "-o", "ConnectTimeout=10",
        f"{SSH_USER}@{SSH_HOST}",
    ]
    if remote_command:
        command.append(remote_command)

    result = subprocess.run(
        command, capture_output=True, text=True,
        timeout=timeout, check=False,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(
            f"Remote operation failed with exit code {result.returncode}"
        )
    return result.stdout


def run_spark_preflight():
    output = ssh_run()
    if "SPARK_PREFLIGHT=PASS" not in output:
        raise RuntimeError("Spark preflight PASS marker missing")


def run_batch_processing(**context):
    # Router only accepts this exact run-ID format.
    run_id = datetime.utcnow().strftime("run_%Y%m%d_%H%M")

    output_path = (
        f"/ecommerce_customer360/processed/batch_runs/{run_id}"
    )

    # Fail safely if this run path already exists.
    response = requests.get(
        f"{WEBHDFS_URL}{output_path}",
        params={"op": "GETFILESTATUS"},
        timeout=20,
    )
    if response.status_code != 404:
        response.raise_for_status()
        raise RuntimeError(
            f"Batch output already exists; refusing to overwrite: {output_path}"
        )

    output = ssh_run(f"batch {run_id}", timeout=900)

    if "Saved " not in output or "BATCH VALIDATION" not in output:
        raise RuntimeError(
            "Batch command returned without expected completion markers"
        )

    context["ti"].xcom_push(key="batch_run_id", value=run_id)
    print(f"BATCH_RUN_ID={run_id}")
    print(f"BATCH_OUTPUT_PATH={output_path}")


def list_data_files(path, session):
    pending = [path]
    count = 0

    while pending:
        current = pending.pop()
        response = session.get(
            f"{WEBHDFS_URL}{current}",
            params={"op": "LISTSTATUS"},
            timeout=30,
        )
        response.raise_for_status()

        entries = response.json().get(
            "FileStatuses", {}
        ).get("FileStatus", [])

        for item in entries:
            name = item.get("pathSuffix", "")
            if name.startswith("_") or name.startswith("."):
                continue

            child = f"{current.rstrip('/')}/{name}"
            if item.get("type") == "DIRECTORY":
                pending.append(child)
            elif item.get("type") == "FILE":
                count += 1

    return count


def validate_hdfs_outputs(**context):
    session = requests.Session()

    paths = dict(BASE_PATHS)
    run_id = context["ti"].xcom_pull(
        task_ids="run_batch_processing",
        key="batch_run_id",
    )
    if not run_id:
        raise RuntimeError("Batch run ID missing from Airflow XCom")

    paths["batch_run"] = (
        f"/ecommerce_customer360/processed/batch_runs/{run_id}"
    )

    for name, path in paths.items():
        count = list_data_files(path, session)
        if count == 0:
            raise ValueError(f"No data files found under {path}")

        print(f"PASS | {name} | data_files={count} | path={path}")

    print("ECOMMERCE_PIPELINE_VALIDATION=PASS")


with DAG(
    dag_id="ecommerce_customer360_pipeline",
    description="Spark preflight, isolated batch processing, and HDFS validation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "customer360", "spark"],
) as dag:

    spark_preflight = PythonOperator(
        task_id="spark_hdfs_preflight",
        python_callable=run_spark_preflight,
    )

    batch_processing = PythonOperator(
        task_id="run_batch_processing",
        python_callable=run_batch_processing,
    )

    validate_outputs = PythonOperator(
        task_id="validate_hdfs_outputs",
        python_callable=validate_hdfs_outputs,
    )

    spark_preflight >> batch_processing >> validate_outputs
