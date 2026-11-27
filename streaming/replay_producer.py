"""
replay producer: replays fraudTest.csv into kafka as live card transactions.

each transaction goes to "transactions" keyed by card number, so a card's events stay in order on one
partition. the fraud label goes separately to "transaction-labels", so the scorer never sees it.
gaps between transactions follow the original timestamps divided by --speed (60 = one hour of
transactions per minute), capped so quiet night hours do not stall the demo.
--no-pacing publishes as fast as kafka accepts, for full-set evaluation runs (latency then includes queue time).
published_at is the wall clock at send time, used for end-to-end latency.

data/fraudTest.csv -> kafka topics transactions, transaction-labels
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import KafkaError, Message, Producer

BOOTSTRAP_SERVERS: str = "localhost:9092"
TRANSACTIONS_TOPIC: str = "transactions"
LABELS_TOPIC: str = "transaction-labels"
SOURCE_FILE: str = "data/fraudTest.csv"
MAX_SLEEP_SECONDS: float = 5.0
PROGRESS_EVERY: int = 1000
FULL_QUEUE_WAIT_SECONDS: float = 0.5

# the fields a card network would send; is_fraud is not one of them
TRANSACTION_FIELDS: list[str] = [
    "trans_num",
    "trans_date_trans_time",
    "cc_num",
    "merchant",
    "category",
    "amt",
    "first",
    "last",
    "gender",
    "street",
    "city",
    "state",
    "zip",
    "lat",
    "long",
    "city_pop",
    "job",
    "dob",
    "merch_lat",
    "merch_long",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger: logging.Logger = logging.getLogger("replay_producer")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="replay fraudTest.csv into kafka")
    parser.add_argument("--speed", type=float, default=60.0, help="replay speed factor, 60 = one hour per minute")
    parser.add_argument("--limit", type=int, default=None, help="stop after this many transactions")
    parser.add_argument("--no-pacing", action="store_true", help="publish as fast as kafka accepts, ignore --speed")
    return parser.parse_args()


def load_transactions(source_file: str) -> pd.DataFrame:
    # every column stays a string, exactly as received, like bronze
    transactions: pd.DataFrame = pd.read_csv(source_file, dtype=str, index_col=0)
    transactions["transaction_time"] = pd.to_datetime(transactions["trans_date_trans_time"], format="%Y-%m-%d %H:%M:%S")
    return transactions.sort_values("transaction_time", kind="stable")


def report_delivery(error: KafkaError | None, message: Message) -> None:
    if error is not None:
        logger.error("delivery failed for key %s on %s: %s", message.key(), message.topic(), error)


def build_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,  # no duplicates if the producer retries
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
    })


def produce_waiting_when_full(producer: Producer, topic: str, key: str, value: str) -> None:
    # without pacing the local send queue fills up; wait for deliveries instead of failing
    while True:
        try:
            producer.produce(topic, key=key, value=value, on_delivery=report_delivery)
            return
        except BufferError:
            producer.poll(FULL_QUEUE_WAIT_SECONDS)


def replay(producer: Producer, transactions: pd.DataFrame, speed: float, limit: int | None, no_pacing: bool) -> int:
    published_count: int = 0
    previous_transaction_time: pd.Timestamp | None = None
    started_at: float = time.monotonic()

    for _, row in transactions.iterrows():
        if limit is not None and published_count >= limit:
            break

        if not no_pacing and previous_transaction_time is not None:
            gap_seconds: float = (row["transaction_time"] - previous_transaction_time).total_seconds() / speed
            if gap_seconds > 0:
                time.sleep(min(gap_seconds, MAX_SLEEP_SECONDS))
        previous_transaction_time = row["transaction_time"]

        published_at: str = datetime.now(timezone.utc).isoformat()
        card_number: str = row["cc_num"]

        transaction_event: dict = {field: row[field] for field in TRANSACTION_FIELDS}
        transaction_event["published_at"] = published_at
        label_event: dict = {
            "trans_num": row["trans_num"],
            "is_fraud": int(row["is_fraud"]),
            "published_at": published_at,
        }

        produce_waiting_when_full(producer, TRANSACTIONS_TOPIC, card_number, json.dumps(transaction_event))
        produce_waiting_when_full(producer, LABELS_TOPIC, card_number, json.dumps(label_event))
        producer.poll(0)
        published_count += 1

        if published_count % PROGRESS_EVERY == 0:
            elapsed_seconds: float = time.monotonic() - started_at
            logger.info("published %d transactions (%.0f per second), replay time %s", published_count, published_count / elapsed_seconds, row["trans_date_trans_time"])

    return published_count


def main() -> None:
    arguments: argparse.Namespace = parse_arguments()
    transactions: pd.DataFrame = load_transactions(SOURCE_FILE)
    pacing: str = "no pacing" if arguments.no_pacing else f"{arguments.speed}x"
    logger.info("loaded %d transactions, replaying at %s", len(transactions), pacing)

    producer: Producer = build_producer()
    published_count: int = 0
    try:
        published_count = replay(producer, transactions, arguments.speed, arguments.limit, arguments.no_pacing)
    except KeyboardInterrupt:
        logger.info("stopped by user")
    finally:
        remaining: int = producer.flush(30)
        logger.info("done: %d transactions published, %d messages not delivered", published_count, remaining)


if __name__ == "__main__":
    main()