"""
Kafka Producer — Financial Transaction Event Simulator
Simulates a high-throughput stream of financial transaction events
at ~5,000 events/sec, publishing to a Kafka topic.
"""

import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "financial-transactions"

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
TRANSACTION_TYPES = ["PURCHASE", "TRANSFER", "WITHDRAWAL", "DEPOSIT", "REFUND"]
MERCHANT_CATEGORIES = ["RETAIL", "FOOD", "TRAVEL", "UTILITIES", "HEALTHCARE", "FINANCE"]


def generate_transaction_event() -> dict:
    """Generate a realistic financial transaction event."""
    amount = round(random.uniform(1.0, 15000.0), 2)
    return {
        "transaction_id": str(uuid.uuid4()),
        "customer_id": f"CUST_{random.randint(10000, 99999)}",
        "account_id": f"ACC_{random.randint(100000, 999999)}",
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "merchant_category": random.choice(MERCHANT_CATEGORIES),
        "merchant_id": f"MERCH_{random.randint(1000, 9999)}",
        "is_high_value": amount >= 10000.0,
        "status": random.choices(
            ["APPROVED", "DECLINED", "PENDING"],
            weights=[85, 10, 5]
        )[0],
        "event_timestamp": datetime.utcnow().isoformat(),
        "kafka_partition_key": f"CUST_{random.randint(10000, 99999)}",
    }


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        batch_size=65536,
        linger_ms=5,
        compression_type="snappy",
        acks="all",
        retries=3,
    )


def produce_events(
    producer: KafkaProducer,
    topic: str,
    target_rate: int = 5000,
    duration_seconds: int = 60,
) -> None:
    """
    Produce events at target_rate events/sec for duration_seconds.
    Default: ~5,000 events/sec for 60 seconds = ~300,000 events.
    """
    logger.info(f"Starting producer: {target_rate} events/sec for {duration_seconds}s")

    total_sent = 0
    start_time = time.time()
    interval = 1.0 / target_rate

    while time.time() - start_time < duration_seconds:
        event = generate_transaction_event()

        producer.send(
            topic=topic,
            key=event["kafka_partition_key"],
            value=event,
        )

        total_sent += 1
        elapsed = time.time() - start_time

        if total_sent % (target_rate * 10) == 0:
            actual_rate = total_sent / elapsed if elapsed > 0 else 0
            logger.info(
                f"Sent: {total_sent:,} events | "
                f"Elapsed: {elapsed:.1f}s | "
                f"Rate: {actual_rate:,.0f} events/sec"
            )

        time.sleep(interval)

    producer.flush()
    total_time = time.time() - start_time
    logger.info(
        f"Done: {total_sent:,} events in {total_time:.1f}s "
        f"({total_sent / total_time:,.0f} events/sec avg)"
    )


if __name__ == "__main__":
    producer = create_producer()
    try:
        produce_events(producer, TOPIC, target_rate=5000, duration_seconds=60)
    finally:
        producer.close()
