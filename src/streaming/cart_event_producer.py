import csv
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"

CUSTOMERS_FILE = RAW_DIR / "olist_customers_dataset.csv"
PRODUCTS_FILE = RAW_DIR / "olist_products_dataset.csv"

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
)
TOPIC = "cart_events"
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "1"))

def load_ids(file_path, column):
    with file_path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        ids = [
            row[column].strip()
            for row in reader
            if row.get(column) and row[column].strip()
        ]
    if not ids:
        raise ValueError(f"No IDs found in {file_path}: {column}")
    return ids

def main():
    customers = load_ids(CUSTOMERS_FILE, "customer_id")
    products = load_ids(PRODUCTS_FILE, "product_id")

    print(f"Loaded {len(customers):,} customer IDs")
    print(f"Loaded {len(products):,} product IDs")
    print(f"Kafka: {BOOTSTRAP_SERVERS}; topic: {TOPIC}")
    print("Mode: SYNTHETIC test events, not observed historical cart activity")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        key_serializer=lambda key: key.encode("utf-8"),
    )

    try:
        while True:
            customer_id = random.choice(customers)
            product_id = random.choice(products)

            event = {
                "event_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "event_type": random.choice(
                    ["view_product", "add_to_cart", "remove_from_cart"]
                ),
                "product_id": product_id,
                "event_time": datetime.now(timezone.utc).isoformat(),
                "event_source": "synthetic_test",
                "is_synthetic": True,
            }

            future = producer.send(
                TOPIC,
                key=customer_id,
                value=event,
            )
            metadata = future.get(timeout=15)

            print(
                f"Sent event_id={event['event_id']} "
                f"type={event['event_type']} "
                f"partition={metadata.partition} "
                f"offset={metadata.offset}"
            )

            time.sleep(EVENT_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    except KafkaError as exc:
        print(f"Kafka publish failed: {exc}")
        raise
    finally:
        producer.flush(timeout=10)
        producer.close()

if __name__ == "__main__":
    main()
