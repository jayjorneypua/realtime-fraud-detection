"""
scoring service: scores every card transaction from kafka in real time.

for each transaction: hash the card number into card_key (same salt and SHA-256 as silver), read the card's
last 30 days from redis, compute the 15 online features, add the transaction to the card's history, then
score the batch with the @champion model. every score goes to "scores"; scores at or above the champion's
alert threshold also go to "fraud-alerts". offsets are committed only after the scores are delivered
(at-least-once), and redis history entries are keyed by transaction id, so reprocessing a message is harmless.

the model is loaded from XGBoost's JSON format and merchant_category is encoded with the category list stored
on the model version, so scoring matches training exactly.

kafka transactions -> redis card history -> fraud.ml.fraud_detector@champion -> kafka scores, fraud-alerts
"""

import base64
import hashlib
import json
import logging
import os
import statistics
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone

import mlflow
import mlflow.xgboost
import pandas as pd
import redis
from confluent_kafka import Consumer, KafkaError, Message, Producer
from databricks.sdk import WorkspaceClient
from mlflow import MlflowClient
from online_features import LOOKBACK_SECONDS, SECONDS_IN_DAY, CardTransaction, build_features, to_epoch
from xgboost import XGBClassifier

DATABRICKS_PROFILE: str = "dbc-609647e1-c073"
BOOTSTRAP_SERVERS: str = "localhost:9092"
REDIS_HOST: str = "localhost"
REDIS_PORT: int = 6380
TRANSACTIONS_TOPIC: str = "transactions"
SCORES_TOPIC: str = "scores"
ALERTS_TOPIC: str = "fraud-alerts"
CONSUMER_GROUP: str = "fraud-scoring-service"
REGISTERED_MODEL_NAME: str = "fraud.ml.fraud_detector"
CHAMPION_ALIAS: str = "champion"
MODEL_FOLDER: str = "data/model"
SECRET_SCOPE: str = "fraud"
CARD_HASH_SALT_KEY: str = "card_hash_salt"
BATCH_SIZE: int = 500
POLL_TIMEOUT_SECONDS: float = 0.5
REPORT_EVERY_SECONDS: float = 10.0
# one day more than the feature lookback, so late or reprocessed events still find their window
HISTORY_RETENTION_SECONDS: int = LOOKBACK_SECONDS + SECONDS_IN_DAY

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger: logging.Logger = logging.getLogger("scoring_service")


@dataclass(frozen=True)
class ChampionModel:
    model: XGBClassifier
    version: str
    alert_threshold: float
    feature_names: list[str]
    merchant_categories: list[str]


def load_champion() -> ChampionModel:
    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    champion_version = MlflowClient().get_model_version_by_alias(REGISTERED_MODEL_NAME, CHAMPION_ALIAS)

    # load the exact version behind the alias, so the threshold and the model always belong together
    model_uri: str = f"models:/{REGISTERED_MODEL_NAME}/{champion_version.version}"
    model_folder: str = os.path.join(MODEL_FOLDER, f"v{champion_version.version}")
    os.makedirs(model_folder, exist_ok=True)

    return ChampionModel(
        model=mlflow.xgboost.load_model(model_uri, dst_path=model_folder),
        version=champion_version.version,
        alert_threshold=float(champion_version.tags["alert_threshold"]),
        # column order straight from the model signature, so it cannot drift from training
        feature_names=mlflow.models.get_model_info(model_uri).signature.inputs.input_names(),
        merchant_categories=json.loads(champion_version.tags["merchant_categories"]),
    )


def load_card_hash_salt() -> str:
    # same salt silver uses, read from the secret scope and kept in memory only
    secret = WorkspaceClient().secrets.get_secret(scope=SECRET_SCOPE, key=CARD_HASH_SALT_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


def report_delivery(error: KafkaError | None, message: Message) -> None:
    if error is not None:
        logger.error("delivery failed for key %s on %s: %s", message.key(), message.topic(), error)


def to_member(transaction: CardTransaction) -> str:
    # sorted keys make the same transaction always the same member, so ZADD is idempotent
    return json.dumps(asdict(transaction), sort_keys=True)


def from_member(member: bytes) -> CardTransaction:
    return CardTransaction(**json.loads(member))


def prepare_transaction(event: dict, card_hash_salt: str, redis_client: redis.Redis) -> dict:
    transaction_time: datetime = datetime.strptime(event["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S")
    card_key: str = hashlib.sha256((card_hash_salt + event["cc_num"]).encode("utf-8")).hexdigest()
    current = CardTransaction(
        transaction_id=event["trans_num"],
        transaction_epoch=to_epoch(transaction_time),
        amount=round(float(event["amt"]), 2),
        merchant_category=event["category"],
        merchant_name=event["merchant"].removeprefix("fraud_"),
        merchant_latitude=float(event["merch_lat"]),
        merchant_longitude=float(event["merch_long"]),
    )

    history_key: str = f"card:{card_key}:history"
    latest_key: str = f"card:{card_key}:latest"

    stored_members: list[bytes] = redis_client.zrangebyscore(history_key, current.transaction_epoch - LOOKBACK_SECONDS, current.transaction_epoch)
    history: list[CardTransaction] = [t for t in map(from_member, stored_members) if t.transaction_id != current.transaction_id]

    stored_latest: bytes | None = redis_client.get(latest_key)
    latest: CardTransaction | None = from_member(stored_latest) if stored_latest else None

    # previous transaction: the latest earlier one in the window, or the card's latest ever if the window is empty
    earlier: list[CardTransaction] = [t for t in history if t.order_key < current.order_key]
    if earlier:
        previous: CardTransaction | None = max(earlier, key=lambda t: t.order_key)
    elif latest is not None and latest.order_key < current.order_key:
        previous = latest
    else:
        previous = None

    features: dict[str, float | str] = build_features(
        current=current,
        transaction_time=transaction_time,
        customer_birth_date=date.fromisoformat(event["dob"]),
        customer_latitude=float(event["lat"]),
        customer_longitude=float(event["long"]),
        history=history,
        previous=previous,
    )

    pipeline = redis_client.pipeline()
    pipeline.zadd(history_key, {to_member(current): current.transaction_epoch})
    pipeline.zremrangebyscore(history_key, "-inf", current.transaction_epoch - HISTORY_RETENTION_SECONDS)
    if latest is None or current.order_key > latest.order_key:
        pipeline.set(latest_key, to_member(current))
    pipeline.execute()

    return {
        "transaction_id": current.transaction_id,
        "card_key": card_key,
        "transaction_time": transaction_time.isoformat(),
        "amount": current.amount,
        "merchant_category": current.merchant_category,
        "published_at": event["published_at"],
        "features": features,
    }


def to_model_frame(prepared_transactions: list[dict], champion: ChampionModel) -> pd.DataFrame:
    feature_frame: pd.DataFrame = pd.DataFrame([t["features"] for t in prepared_transactions])[champion.feature_names]
    feature_frame = feature_frame.astype({name: "float64" for name in champion.feature_names if name != "merchant_category"})
    # same category list as training; a category the model never saw becomes missing, which XGBoost handles
    feature_frame["merchant_category"] = pd.Categorical(feature_frame["merchant_category"], categories=champion.merchant_categories)
    return feature_frame


def score_and_publish(prepared_transactions: list[dict], champion: ChampionModel, producer: Producer) -> tuple[list[float], int]:
    fraud_scores = champion.model.predict_proba(to_model_frame(prepared_transactions, champion))[:, 1]

    latencies_ms: list[float] = []
    alert_count: int = 0
    for transaction, fraud_score in zip(prepared_transactions, fraud_scores):
        scored_at: datetime = datetime.now(timezone.utc)
        latency_ms: float = (scored_at - datetime.fromisoformat(transaction["published_at"])).total_seconds() * 1000
        is_alert: bool = bool(fraud_score >= champion.alert_threshold)

        score_event: dict = {
            **{key: value for key, value in transaction.items() if key != "published_at"},
            "fraud_score": float(fraud_score),
            "alert_threshold": champion.alert_threshold,
            "is_alert": is_alert,
            "model_version": champion.version,
            "published_at": transaction["published_at"],
            "scored_at": scored_at.isoformat(),
            "latency_ms": round(latency_ms, 2),
        }
        score_payload: str = json.dumps(score_event)

        producer.produce(SCORES_TOPIC, key=transaction["card_key"], value=score_payload, on_delivery=report_delivery)
        if is_alert:
            producer.produce(ALERTS_TOPIC, key=transaction["card_key"], value=score_payload, on_delivery=report_delivery)
            alert_count += 1
        latencies_ms.append(latency_ms)

    producer.poll(0)
    return latencies_ms, alert_count


def main() -> None:
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", DATABRICKS_PROFILE)

    champion: ChampionModel = load_champion()
    logger.info("loaded %s version %s, alert threshold %.4f", REGISTERED_MODEL_NAME, champion.version, champion.alert_threshold)
    card_hash_salt: str = load_card_hash_salt()

    redis_client: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    redis_client.ping()

    consumer: Consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit only after the scores are delivered
    })
    consumer.subscribe([TRANSACTIONS_TOPIC])
    producer: Producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "lz4",
    })

    recent_latencies_ms: deque[float] = deque(maxlen=5000)
    scored_since_report: int = 0
    alerts_since_report: int = 0
    last_report: float = time.monotonic()
    logger.info("waiting for transactions on %s", TRANSACTIONS_TOPIC)

    try:
        while True:
            messages: list[Message] = consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_SECONDS)
            prepared_transactions: list[dict] = []
            for message in messages:
                if message.error():
                    logger.error("consume error: %s", message.error())
                    continue
                prepared_transactions.append(prepare_transaction(json.loads(message.value()), card_hash_salt, redis_client))

            if prepared_transactions:
                latencies_ms, alert_count = score_and_publish(prepared_transactions, champion, producer)
                undelivered: int = producer.flush(10)
                if undelivered > 0:
                    raise RuntimeError(f"{undelivered} score messages were not delivered, offsets not committed")
                consumer.commit(asynchronous=False)
                recent_latencies_ms.extend(latencies_ms)
                scored_since_report += len(prepared_transactions)
                alerts_since_report += alert_count

            seconds_since_report: float = time.monotonic() - last_report
            if seconds_since_report >= REPORT_EVERY_SECONDS and scored_since_report > 0:
                latency_p95_ms: float = (
                    statistics.quantiles(recent_latencies_ms, n=20)[18] if len(recent_latencies_ms) >= 20 else max(recent_latencies_ms)
                )
                logger.info(
                    "scored %d (%.0f per second), alerts %d, latency p50 %.1f ms, p95 %.1f ms",
                    scored_since_report,
                    scored_since_report / seconds_since_report,
                    alerts_since_report,
                    statistics.median(recent_latencies_ms),
                    latency_p95_ms,
                )
                scored_since_report = 0
                alerts_since_report = 0
                last_report = time.monotonic()
    except KeyboardInterrupt:
        logger.info("stopped by user")
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()