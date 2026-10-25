import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
TOPIC = "cart_events"
GROUP_ID = os.environ.get("C360_ALERT_GROUP_ID", "c360-cart-abandoned-alerts-v2")

IDLE_SECONDS = int(os.getenv("CART_IDLE_SECONDS", "900"))
POLL_SECONDS = float(os.getenv("ALERT_POLL_SECONDS", "5"))

STATE_PATH = Path(os.getenv(
    "CART_ALERT_STATE",
    str(PROJECT_ROOT / "data/processed/streaming/cart_alert_state.sqlite")
))
ALERT_LOG = Path(os.getenv(
    "CART_ALERT_LOG",
    str(PROJECT_ROOT / "data/logs/cart_abandoned_alerts.jsonl")
))


def utc_now():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def connect_db():
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(STATE_PATH, timeout=30)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA journal_mode=WAL")

    # Existing tables are preserved.
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

    # Durable event deduplication and alert outbox.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            event_id TEXT PRIMARY KEY,
            processed_at REAL NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alert_outbox (
            alert_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at REAL NOT NULL,
            delivered_at REAL
        )
    """)
    conn.commit()
    return conn


def process_event(conn, event):
    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object")

    event_id = event.get("event_id")
    customer_id = event.get("customer_id")
    product_id = event.get("product_id")
    event_type = event.get("event_type")

    if not event_id:
        raise ValueError("Missing event_id; refusing non-idempotent event")

    # Ignore unrelated event types, but remember their IDs too.
    if event_type not in {"add_to_cart", "remove_from_cart"}:
        with conn:
            conn.execute(
                "INSERT OR IGNORE INTO processed_events VALUES (?, ?)",
                (str(event_id), time.time())
            )
        return

    if not customer_id or not product_id:
        raise ValueError("Cart event missing customer_id or product_id")

    now_ts = time.time()
    is_synthetic = bool(event.get("is_synthetic", False))
    source = event.get("event_source", "unknown")

    with conn:
        inserted = conn.execute("""
            INSERT OR IGNORE INTO processed_events(event_id, processed_at)
            VALUES (?, ?)
        """, (str(event_id), now_ts)).rowcount

        if not inserted:
            print(f"DUPLICATE_EVENT_IGNORED={event_id}", flush=True)
            return

        if event_type == "add_to_cart":
            conn.execute("""
                INSERT INTO cart_items(customer_id, product_id, quantity)
                VALUES (?, ?, 1)
                ON CONFLICT(customer_id, product_id)
                DO UPDATE SET quantity = quantity + 1
            """, (customer_id, product_id))
        else:
            conn.execute("""
                UPDATE cart_items
                SET quantity = quantity - 1
                WHERE customer_id = ? AND product_id = ?
            """, (customer_id, product_id))
            conn.execute("""
                DELETE FROM cart_items
                WHERE customer_id = ? AND product_id = ? AND quantity <= 0
            """, (customer_id, product_id))

        item_count = conn.execute("""
            SELECT COUNT(*) FROM cart_items
            WHERE customer_id = ? AND quantity > 0
        """, (customer_id,)).fetchone()[0]

        if item_count == 0:
            conn.execute("DELETE FROM carts WHERE customer_id = ?", (customer_id,))
        else:
            conn.execute("""
                INSERT INTO carts(customer_id, last_activity, is_synthetic,
                                  event_source, alert_sent)
                VALUES (?, ?, ?, ?, 0)
                ON CONFLICT(customer_id) DO UPDATE SET
                    last_activity = excluded.last_activity,
                    is_synthetic = excluded.is_synthetic,
                    event_source = excluded.event_source,
                    alert_sent = 0
            """, (customer_id, now_ts, int(is_synthetic), source))


def enqueue_due_alerts(conn):
    now_ts = time.time()
    due = conn.execute("""
        SELECT customer_id, last_activity, is_synthetic, event_source
        FROM carts
        WHERE alert_sent = 0 AND last_activity <= ?
          AND EXISTS (
              SELECT 1 FROM cart_items i
              WHERE i.customer_id = carts.customer_id AND i.quantity > 0
          )
    """, (now_ts - IDLE_SECONDS,)).fetchall()

    for customer_id, last_activity, is_synthetic, source in due:
        items = conn.execute("""
            SELECT product_id, quantity FROM cart_items
            WHERE customer_id = ? AND quantity > 0
            ORDER BY product_id
        """, (customer_id,)).fetchall()

        # Deterministic for this cart activity; uniqueness is enforced by SQLite.
        alert_id = f"{customer_id}:{last_activity:.6f}"
        alert = {
            "alert_id": alert_id,
            "alert_type": "abandoned_cart",
            "customer_id": customer_id,
            "product_count": len(items),
            "items": [
                {"product_id": product_id, "quantity": quantity}
                for product_id, quantity in items
            ],
            "last_cart_activity_epoch": last_activity,
            "last_cart_activity_time": iso(
                datetime.fromtimestamp(last_activity, timezone.utc)
            ),
            "alert_time": iso(utc_now()),
            "idle_threshold_seconds": IDLE_SECONDS,
            "is_synthetic": bool(is_synthetic),
            "event_source": source,
        }

        with conn:
            conn.execute("""
                INSERT OR IGNORE INTO alert_outbox
                    (alert_id, customer_id, payload, created_at, delivered_at)
                VALUES (?, ?, ?, ?, NULL)
            """, (alert_id, customer_id, json.dumps(alert), now_ts))
            conn.execute("""
                UPDATE carts SET alert_sent = 1 WHERE customer_id = ?
            """, (customer_id,))


def deliver_outbox(conn):
    pending = conn.execute("""
        SELECT alert_id, payload
        FROM alert_outbox
        WHERE delivered_at IS NULL
        ORDER BY created_at, alert_id
    """).fetchall()

    if not pending:
        return

    ALERT_LOG.parent.mkdir(parents=True, exist_ok=True)

    # Recover safely if a prior run appended a line but crashed before
    # marking its SQLite outbox row as delivered.
    existing_ids = set()
    if ALERT_LOG.exists():
        with ALERT_LOG.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    item = json.loads(line)
                    if item.get("alert_id"):
                        existing_ids.add(item["alert_id"])
                except json.JSONDecodeError:
                    print("WARNING: malformed line in alert log", flush=True)

    for alert_id, payload in pending:
        alert = json.loads(payload)

        if alert_id not in existing_ids:
            with ALERT_LOG.open("a", encoding="utf-8") as f:
                f.write(json.dumps(alert) + "\n")
                f.flush()
                os.fsync(f.fileno())
            existing_ids.add(alert_id)

        with conn:
            conn.execute("""
                UPDATE alert_outbox
                SET delivered_at = ?
                WHERE alert_id = ? AND delivered_at IS NULL
            """, (time.time(), alert_id))

        print("ABANDONED_CART_ALERT=" + payload, flush=True)


def main():
    conn = connect_db()
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        enable_auto_commit=False,
        auto_offset_reset="latest",
        group_id=GROUP_ID,
        value_deserializer=None,
        consumer_timeout_ms=1000,
        max_poll_records=100,
    )

    print(f"Kafka: {BOOTSTRAP_SERVERS}; topic: {TOPIC}", flush=True)
    print(f"Consumer group: {GROUP_ID}", flush=True)
    print(f"Idle threshold: {IDLE_SECONDS}s", flush=True)
    print(f"SQLite state: {STATE_PATH}", flush=True)
    print(f"Alert log: {ALERT_LOG}", flush=True)
    print("Manual offset commits enabled; local test, not SLA certification.",
          flush=True)

    consumer.subscribe([TOPIC])

    try:
        while True:
            records = consumer.poll(timeout_ms=1000, max_records=100)

            for tp, messages in records.items():
                last_processed_offset = None

                for message in messages:
                    try:
                        event = json.loads(message.value.decode("utf-8"))
                        process_event(conn, event)
                    except (UnicodeDecodeError, json.JSONDecodeError,
                            ValueError, TypeError) as exc:
                        # Stop processing this batch; do not commit past failure.
                        print(
                            f"EVENT_ERROR partition={message.partition} "
                            f"offset={message.offset}: {exc}",
                            flush=True
                        )
                        raise

                    last_processed_offset = message.offset

                # Commit only through the last consecutively processed message.
                if last_processed_offset is not None:
                    consumer.commit({
                        tp: OffsetAndMetadata(last_processed_offset + 1, "")
                    })

            enqueue_due_alerts(conn)
            deliver_outbox(conn)
            time.sleep(POLL_SECONDS)

    except KeyboardInterrupt:
        print("\nAlert consumer stopped.", flush=True)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
