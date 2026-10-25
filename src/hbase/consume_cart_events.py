import json
import os
from datetime import datetime, timezone

import happybase
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

KAFKA_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"
)
TOPIC = "cart_events"
GROUP_ID = os.getenv(
    "C360_HBASE_GROUP_ID", "c360-hbase-cart-activity-v1"
)
STARTING_OFFSETS = os.getenv("C360_HBASE_STARTING_OFFSETS", "latest")

HBASE_HOST = "127.0.0.1"
HBASE_PORT = 9090
PROFILE_TABLE = "ecommerce:customer_360"
EVENT_TABLE = "ecommerce:cart_activity_events"

VALID_EVENTS = {"view_product", "add_to_cart", "remove_from_cart"}


def as_bytes(value):
    return str(value).encode("utf-8")


def process_event(event, profiles, event_table):
    required = (
        "event_id", "customer_id", "event_type",
        "product_id", "event_time", "event_source"
    )
    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object")
    if any(not event.get(key) for key in required):
        raise ValueError("Event is missing a required field")

    event_id = str(event["event_id"]).strip()
    customer_id = str(event["customer_id"]).strip()
    event_type = str(event["event_type"]).strip()
    event_time = str(event["event_time"]).strip()

    if event_type not in VALID_EVENTS:
        raise ValueError(f"Unsupported event type: {event_type}")

    # Validate timestamp before writing anything.
    parsed_time = datetime.fromisoformat(event_time.replace("Z", "+00:00"))
    if parsed_time.tzinfo is None:
        raise ValueError("event_time must include a timezone")

    synthetic = event.get("is_synthetic")
    if not isinstance(synthetic, bool):
        raise ValueError("is_synthetic must be a JSON boolean")

    customer_key = as_bytes(customer_id)
    profile = profiles.row(customer_key)
    if not profile:
        raise ValueError(f"Customer profile not found: {customer_id}")

    # Idempotent event ledger row. A replay uses the same row key.
    existing_event = event_table.row(as_bytes(event_id))
    if existing_event:
        existing_customer = existing_event.get(b"event:customer_id", b"")
        if existing_customer != customer_key:
            raise ValueError("event_id already belongs to another customer")
        print(f"DUPLICATE_EVENT_REPLAY event_id={event_id}")
    else:
        event_table.put(
            as_bytes(event_id),
            {
                b"event:event_id": as_bytes(event_id),
                b"event:customer_id": customer_key,
                b"event:event_type": as_bytes(event_type),
                b"event:product_id": as_bytes(event["product_id"]),
                b"event:event_time": as_bytes(event_time),
                b"event:event_source": as_bytes(event["event_source"]),
                b"event:is_synthetic": as_bytes(str(synthetic).lower()),
                b"event:processed_at": as_bytes(
                    datetime.now(timezone.utc).isoformat()
                ),
            },
        )

    # Repair the summary after a crash between ledger and profile writes.
    # Only replace the latest activity when this event is not older.
    current_time = profile.get(b"activity:last_event_time", b"").decode(
        "utf-8", errors="replace"
    )
    should_update = True

    if current_time:
        try:
            current_parsed = datetime.fromisoformat(
                current_time.replace("Z", "+00:00")
            )
            should_update = parsed_time >= current_parsed
        except ValueError:
            raise ValueError("Existing activity timestamp is invalid")

    if should_update:
        profiles.put(
            customer_key,
            {
                b"activity:last_event_id": as_bytes(event_id),
                b"activity:last_event_type": as_bytes(event_type),
                b"activity:last_product_id": as_bytes(event["product_id"]),
                b"activity:last_event_time": as_bytes(event_time),
                b"activity:last_event_source": as_bytes(event["event_source"]),
                b"activity:last_is_synthetic": as_bytes(str(synthetic).lower()),
            },
        )


def main():
    connection = happybase.Connection(
        HBASE_HOST,
        port=HBASE_PORT,
        timeout=10000,
        autoconnect=False,
    )
    consumer = None

    try:
        connection.open()
        profiles = connection.table(PROFILE_TABLE)
        event_table = connection.table(EVENT_TABLE)

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_SERVERS.split(","),
            group_id=GROUP_ID,
            enable_auto_commit=False,
            auto_offset_reset=STARTING_OFFSETS,
            key_deserializer=lambda value: (
                value.decode("utf-8") if value else None
            ),
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )
        consumer.subscribe([TOPIC])

        print("HBASE_CART_CONSUMER_STATUS=STARTED")
        print(f"Kafka topic: {TOPIC}; group: {GROUP_ID}")
        print(f"Starting offsets: {STARTING_OFFSETS}")
        print(f"Profile table: {PROFILE_TABLE}")
        print(f"Event ledger: {EVENT_TABLE}")

        while True:
            records = consumer.poll(timeout_ms=1000, max_records=20)

            for _partition, messages in records.items():
                for message in messages:
                    # Exceptions deliberately stop the consumer without
                    # committing the failed message's offset.
                    process_event(message.value, profiles, event_table)

                    # Commit only this successfully processed record,
                    # not later records that poll may have fetched.
                    tp = TopicPartition(message.topic, message.partition)
                    consumer.commit({
                        tp: OffsetAndMetadata(message.offset + 1, None)
                    })

                    print(
                        f"PROCESSED event_id={message.value['event_id']} "
                        f"customer_id={message.value['customer_id']} "
                        f"type={message.value['event_type']} "
                        f"synthetic={message.value['is_synthetic']} "
                        f"partition={message.partition} "
                        f"offset={message.offset}"
                    )

    finally:
        if consumer is not None:
            consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
