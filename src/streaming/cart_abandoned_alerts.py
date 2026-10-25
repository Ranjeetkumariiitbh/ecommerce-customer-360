import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
)
TOPIC = "cart_events"

IDLE_SECONDS = int(os.getenv("CART_IDLE_SECONDS", "900"))
POLL_SECONDS = float(os.getenv("ALERT_POLL_SECONDS", "5"))

STATE_PATH = Path(
    os.getenv(
        "CART_ALERT_STATE",
        str(PROJECT_ROOT / "data/processed/streaming/cart_alert_state.sqlite"),
    )
)
ALERT_LOG = Path(
    os.getenv(
        "CART_ALERT_LOG",
        str(PROJECT_ROOT / "data/logs/cart_abandoned_alerts.jsonl"),
    )
)


def utc_now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def connect_db():
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(STATE_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS carts (
            customer_id TEXT PRIMARY KEY,
            last_activity REAL NOT NULL,
            is_synthetic INTEGER NOT NULL,
            event_source TEXT,
            alert_sent INTEGER NOT NULL DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cart_items (
            customer_id TEXT NOT NULL,
            product_id TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            PRIMARY KEY (customer_id, product_id)
        )
    """)
    conn.commit()
    return conn


def process_event(conn, event):
    customer_id = event.get("customer_id")
    product_id = event.get("product_id")
    event_type = event.get("event_type")

    if not customer_id or not product_id:
        return
    if event_type not in {"add_to_cart", "remove_from_cart"}:
        return

    now_ts = time.time()
    is_synthetic = bool(event.get("is_synthetic", False))
    source = event.get("event_source", "unknown")

    with conn:
        conn.execute("""
            INSERT INTO carts (
                customer_id, last_activity, is_synthetic, event_source,
                alert_sent
            )
            VALUES (?, ?, ?, ?, 0)
            ON CONFLICT(customer_id) DO UPDATE SET
                last_activity = excluded.last_activity,
                is_synthetic = excluded.is_synthetic,
                event_source = excluded.event_source,
                alert_sent = 0
        """, (customer_id, now_ts, int(is_synthetic), source))

        if event_type == "add_to_cart":
            conn.execute("""
                INSERT INTO cart_items (customer_id, product_id, quantity)
                VALUES (?, ?, 1)
                ON CONFLICT(customer_id, product_id)
                DO UPDATE SET quantity = quantity + 1
            """, (customer_id, product_id))

        elif event_type == "remove_from_cart":
            conn.execute("""
                UPDATE cart_items
                SET quantity = quantity - 1
                WHERE customer_id = ? AND product_id = ?
            """, (customer_id, product_id))
            conn.execute("""
                DELETE FROM cart_items
                WHERE customer_id = ? AND product_id = ? AND quantity <= 0
            """, (customer_id, product_id))

        # Remove a customer from tracking when no products remain.
        item_count = conn.execute("""
            SELECT COUNT(*) FROM cart_items
            WHERE customer_id = ? AND quantity > 0
        """, (customer_id,)).fetchone()[0]

        if item_count == 0:
            conn.execute("DELETE FROM carts WHERE customer_id = ?", (customer_id,))


def emit_due_alerts(conn):
    now_ts = time.time()
    due = conn.execute("""
        SELECT c.customer_id, c.last_activity, c.is_synthetic, c.event_source
        FROM carts c
        WHERE c.alert_sent = 0
          AND c.last_activity <= ?
          AND EXISTS (
              SELECT 1 FROM cart_items i
              WHERE i.customer_id = c.customer_id AND i.quantity > 0
          )
    """, (now_ts - IDLE_SECONDS,)).fetchall()

    if not due:
        return

    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)

    for customer_id, last_activity, is_synthetic, source in due:
        item_rows = conn.execute("""
            SELECT product_id, quantity FROM cart_items
            WHERE customer_id = ? AND quantity > 0
        """, (customer_id,)).fetchall()

        alert_time = utc_now()
        alert = {
            "alert_id": f"{customer_id}:{int(last_activity)}",
            "alert_type": "abandoned_cart",
            "customer_id": customer_id,
            "product_count": len(item_rows),
            "items": [
                {"product_id": product_id, "quantity": quantity}
                for product_id, quantity in item_rows
            ],
            "last_cart_activity_epoch": last_activity,
            "last_cart_activity_time": iso(
                datetime.fromtimestamp(last_activity, timezone.utc)
            ),
            "alert_time": iso(alert_time),
            "idle_threshold_seconds": IDLE_SECONDS,
            "is_synthetic": bool(is_synthetic),
            "event_source": source,
        }

        with ALERT_LOG.open("a", encoding="utf-8") as f:
            f.write(json.dumps(alert) + "\n")

        print("ABANDONED_CART_ALERT=" + json.dumps(alert), flush=True)

        with conn:
            conn.execute("""
                UPDATE carts SET alert_sent = 1 WHERE customer_id = ?
            """, (customer_id,))


def main():
    conn = connect_db()
    print(f"Kafka: {BOOTSTRAP_SERVERS}; topic: {TOPIC}", flush=True)
    print(f"Idle threshold: {IDLE_SECONDS} seconds", flush=True)
    print(f"Poll interval: {POLL_SECONDS} seconds", flush=True)
    print(f"SQLite state: {STATE_PATH}", flush=True)
    print(f"Alert log: {ALERT_LOG}", flush=True)
    print(
        "WARNING: events and alerts may be synthetic; "
        "this is a local functional test, not SLA certification.",
        flush=True,
    )

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        enable_auto_commit=True,
        auto_offset_reset="latest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        consumer_timeout_ms=1000,
        group_id="c360-cart-abandoned-alerts",
    )

    try:
        while True:
            for message in consumer:
                try:
                    process_event(conn, message.value)
                except (ValueError, TypeError, KeyError) as exc:
                    print(f"INVALID_EVENT: {exc}", flush=True)

            emit_due_alerts(conn)
            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\nAlert consumer stopped.", flush=True)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
