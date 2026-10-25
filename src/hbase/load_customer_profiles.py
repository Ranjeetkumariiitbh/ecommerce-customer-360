import csv
import happybase
from pathlib import Path

CSV_PATH = Path("data/raw/olist_customers_dataset.csv")
HBASE_HOST = "127.0.0.1"
HBASE_PORT = 9090
TABLE_NAME = "ecommerce:customer_360"
BATCH_SIZE = 500

if not CSV_PATH.exists():
    raise FileNotFoundError(f"Customer CSV not found: {CSV_PATH.resolve()}")

connection = happybase.Connection(
    HBASE_HOST,
    port=HBASE_PORT,
    timeout=10000,
    autoconnect=False,
)

loaded = 0

try:
    connection.open()
    table = connection.table(TABLE_NAME)

    with CSV_PATH.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        required = {
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        }
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"Unexpected CSV columns: {reader.fieldnames}")

        with table.batch(batch_size=BATCH_SIZE) as batch:
            for row in reader:
                customer_id = (row.get("customer_id") or "").strip()
                if not customer_id:
                    continue

                batch.put(
                    customer_id.encode("utf-8"),
                    {
                        b"profile:customer_unique_id":
                            (row.get("customer_unique_id") or "").encode("utf-8"),
                        b"profile:zip_code":
                            (row.get("customer_zip_code_prefix") or "").encode("utf-8"),
                        b"profile:city":
                            (row.get("customer_city") or "").encode("utf-8"),
                        b"profile:state":
                            (row.get("customer_state") or "").encode("utf-8"),
                    },
                )
                loaded += 1

    print(f"Rows submitted to HBase: {loaded}")

finally:
    connection.close()
