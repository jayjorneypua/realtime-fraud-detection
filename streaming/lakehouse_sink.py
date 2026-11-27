"""
lakehouse sink: lands the live kafka topics in Databricks as batched JSON lines files.

consumes transactions, transaction-labels and scores, and every --batch-size messages (default 500) or 30 seconds
uploads one file per topic partition to /Volumes/fraud/landing/live/<topic>/, all files of a batch in parallel.
small batches land files quickly for demos; large batches (e.g. 25000) keep full replays fast with fewer uploads.
offsets are committed only after every upload succeeds (at-least-once). a retried batch lands twice, which is
harmless because silver and scores keep one row per transaction id. file names carry the upload time, so a new
session never overwrites an earlier one.

kafka transactions, transaction-labels, scores -> /Volumes/fraud/landing/live/<topic>/*.jsonl
"""

import argparse
import io
import logging
import os
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from confluent_kafka import Consumer, Message
from databricks.sdk import WorkspaceClient
from scoring_service import BOOTSTRAP_SERVERS, DATABRICKS_PROFILE

SINK_TOPICS: list[str] = ["transactions", "transaction-labels", "scores"]
CONSUMER_GROUP: str = "fraud-lakehouse-sink"
LANDING_VOLUME: str = "/Volumes/fraud/landing/live"
DEFAULT_BATCH_SIZE: int = 500
CONSUME_CHUNK_SIZE: int = 1000
FLUSH_EVERY_SECONDS: float = 30.0
POLL_TIMEOUT_SECONDS: float = 1.0
MAX_PARALLEL_UPLOADS: int = 9  # 3 topics x 3 partitions

logger: logging.Logger = logging.getLogger("lakehouse_sink")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="land kafka topics in the Databricks landing volume")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="messages per landed batch")
    return parser.parse_args()


def upload_file(workspace_client: WorkspaceClient, file_path: str, file_contents: bytes) -> None:
    workspace_client.files.upload(file_path, io.BytesIO(file_contents), overwrite=True)


def land_batch(
    workspace_client: WorkspaceClient,
    upload_pool: ThreadPoolExecutor,
    consumer: Consumer,
    buffered_messages: list[Message],
) -> int:
    messages_by_partition: dict[tuple[str, int], list[Message]] = defaultdict(list)
    for message in buffered_messages:
        messages_by_partition[(message.topic(), message.partition())].append(message)

    uploaded_at: str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    messages_per_topic: Counter[str] = Counter()
    pending_uploads = []
    for (topic, partition), partition_messages in messages_by_partition.items():
        first_offset: int = partition_messages[0].offset()
        last_offset: int = partition_messages[-1].offset()
        file_path: str = f"{LANDING_VOLUME}/{topic}/{uploaded_at}-p{partition}-o{first_offset}-{last_offset}.jsonl"
        file_contents: bytes = b"\n".join(message.value() for message in partition_messages) + b"\n"
        pending_uploads.append(upload_pool.submit(upload_file, workspace_client, file_path, file_contents))
        messages_per_topic[topic] += len(partition_messages)

    # result() re-raises any upload error, so nothing is committed unless every file landed
    for pending_upload in pending_uploads:
        pending_upload.result()

    consumer.commit(asynchronous=False)
    logger.info("landed %s", ", ".join(f"{topic} {count}" for topic, count in sorted(messages_per_topic.items())))
    return len(buffered_messages)


def main() -> None:
    arguments: argparse.Namespace = parse_arguments()
    batch_size: int = arguments.batch_size

    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", DATABRICKS_PROFILE)
    workspace_client: WorkspaceClient = WorkspaceClient()

    consumer: Consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit only after the files are uploaded
    })
    consumer.subscribe(SINK_TOPICS)

    buffered_messages: list[Message] = []
    landed_total: int = 0
    last_flush: float = time.monotonic()
    logger.info("sinking %s to %s, batch size %d", ", ".join(SINK_TOPICS), LANDING_VOLUME, batch_size)

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_UPLOADS) as upload_pool:
        try:
            while True:
                for message in consumer.consume(num_messages=CONSUME_CHUNK_SIZE, timeout=POLL_TIMEOUT_SECONDS):
                    if message.error():
                        logger.error("consume error: %s", message.error())
                        continue
                    buffered_messages.append(message)

                batch_is_full: bool = len(buffered_messages) >= batch_size
                batch_is_due: bool = len(buffered_messages) > 0 and time.monotonic() - last_flush >= FLUSH_EVERY_SECONDS
                if batch_is_full or batch_is_due:
                    landed_total += land_batch(workspace_client, upload_pool, consumer, buffered_messages)
                    buffered_messages = []
                    last_flush = time.monotonic()
        except KeyboardInterrupt:
            logger.info("stopped by user, landing the last batch")
            if buffered_messages:
                landed_total += land_batch(workspace_client, upload_pool, consumer, buffered_messages)
        finally:
            consumer.close()
            logger.info("done: %d messages landed", landed_total)


if __name__ == "__main__":
    main()