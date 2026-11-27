"""
feature parity check: proves the online features match the sql features value by value.

takes every transaction in the last day of silver history, rebuilds its 15 features with online_features.py
(choosing "previous" the same way the scoring service does: the latest earlier transaction in the 30 day window,
otherwise the card's latest earlier transaction ever), and compares each value with
fraud.gold.transaction_features_history. exits with code 1 on any mismatch.

sql/feature_parity_sample.sql + sql/feature_parity_history.sql (sql warehouse) -> online features -> comparison
"""

import logging
import math
import os
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementResponse
from online_features import LOOKBACK_SECONDS, CardTransaction, build_features
from redis_backfill import WAREHOUSE_ID, to_card_transaction, wait_for_statement
from scoring_service import DATABRICKS_PROFILE

SQL_FOLDER: Path = Path(__file__).resolve().parent.parent / "sql"
SAMPLE_SQL_PATH: Path = SQL_FOLDER / "feature_parity_sample.sql"
HISTORY_SQL_PATH: Path = SQL_FOLDER / "feature_parity_history.sql"
ABSOLUTE_TOLERANCE: float = 1e-6
RELATIVE_TOLERANCE: float = 1e-9
MAX_EXAMPLES: int = 10
FEATURE_NAMES: list[str] = [
    "merchant_category",
    "amount",
    "hour_of_day",
    "day_of_week",
    "is_night",
    "customer_age_years",
    "customer_to_merchant_km",
    "card_transaction_count_1h",
    "card_amount_sum_1h",
    "card_transaction_count_24h",
    "card_amount_sum_24h",
    "amount_to_card_average_ratio",
    "seconds_since_previous_card_transaction",
    "km_from_previous_merchant",
    "is_new_category_for_card",
    "is_new_merchant_for_card",
]

logger: logging.Logger = logging.getLogger("feature_parity_check")


def run_sql_file(workspace_client: WorkspaceClient, sql_path: Path) -> list[dict[str, str | None]]:
    statement: StatementResponse = workspace_client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=sql_path.read_text(encoding="utf-8"),
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    statement = wait_for_statement(workspace_client, statement)

    column_names: list[str] = [column.name for column in statement.manifest.schema.columns]
    raw_rows: list[list[str | None]] = list(statement.result.data_array or [])
    next_chunk_index: int | None = statement.result.next_chunk_index
    while next_chunk_index is not None:
        chunk = workspace_client.statement_execution.get_statement_result_chunk_n(statement.statement_id, next_chunk_index)
        raw_rows.extend(chunk.data_array or [])
        next_chunk_index = chunk.next_chunk_index

    return [dict(zip(column_names, raw_row)) for raw_row in raw_rows]


def previous_from_lag(row: dict[str, str | None]) -> CardTransaction | None:
    if row["previous_transaction_id"] is None:
        return None
    return CardTransaction(
        transaction_id=row["previous_transaction_id"],
        transaction_epoch=int(row["previous_transaction_epoch"]),
        amount=round(float(row["previous_amount"]), 2),
        merchant_category=row["previous_merchant_category"],
        merchant_name=row["previous_merchant_name"],
        merchant_latitude=float(row["previous_merchant_latitude"]),
        merchant_longitude=float(row["previous_merchant_longitude"]),
    )


def values_match(feature_name: str, online_value: float | str, sql_value: str | None) -> bool:
    if sql_value is None:
        return False
    if feature_name == "merchant_category":
        return online_value == sql_value
    return math.isclose(float(online_value), float(sql_value), rel_tol=RELATIVE_TOLERANCE, abs_tol=ABSOLUTE_TOLERANCE)


def main() -> None:
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", DATABRICKS_PROFILE)
    workspace_client: WorkspaceClient = WorkspaceClient()

    logger.info("reading the parity sample and card history from warehouse %s", WAREHOUSE_ID)
    sample_rows: list[dict[str, str | None]] = run_sql_file(workspace_client, SAMPLE_SQL_PATH)
    history_rows: list[dict[str, str | None]] = run_sql_file(workspace_client, HISTORY_SQL_PATH)
    logger.info("comparing %d transactions using %d history rows", len(sample_rows), len(history_rows))

    history_by_card: dict[str, list[CardTransaction]] = defaultdict(list)
    for history_row in history_rows:
        history_by_card[history_row["card_key"]].append(to_card_transaction(history_row))

    mismatch_counts: Counter[str] = Counter()
    mismatch_examples: list[str] = []
    for sample_row in sample_rows:
        current: CardTransaction = to_card_transaction(sample_row)
        epoch: int = current.transaction_epoch

        # same window the scoring service reads from redis: prior 30 days up to and including the current epoch
        history: list[CardTransaction] = [
            transaction
            for transaction in history_by_card[sample_row["card_key"]]
            if epoch - LOOKBACK_SECONDS <= transaction.transaction_epoch <= epoch
            and transaction.transaction_id != current.transaction_id
        ]
        earlier: list[CardTransaction] = [transaction for transaction in history if transaction.order_key < current.order_key]
        previous: CardTransaction | None = (
            max(earlier, key=lambda transaction: transaction.order_key) if earlier else previous_from_lag(sample_row)
        )

        online_features: dict[str, float | str] = build_features(
            current=current,
            transaction_time=datetime.strptime(sample_row["transaction_time_text"], "%Y-%m-%d %H:%M:%S"),
            customer_birth_date=date.fromisoformat(sample_row["customer_birth_date"]),
            customer_latitude=float(sample_row["customer_latitude"]),
            customer_longitude=float(sample_row["customer_longitude"]),
            history=history,
            previous=previous,
        )

        for feature_name in FEATURE_NAMES:
            sql_value: str | None = sample_row[f"sql_{feature_name}"]
            if not values_match(feature_name, online_features[feature_name], sql_value):
                mismatch_counts[feature_name] += 1
                if len(mismatch_examples) < MAX_EXAMPLES:
                    mismatch_examples.append(
                        f"{current.transaction_id} {feature_name}: online {online_features[feature_name]} vs sql {sql_value}"
                    )

    if not mismatch_counts:
        logger.info("parity passed: all %d features match for %d transactions", len(FEATURE_NAMES), len(sample_rows))
        return

    for feature_name, mismatch_count in mismatch_counts.most_common():
        logger.error("%s: %d of %d transactions differ", feature_name, mismatch_count, len(sample_rows))
    for mismatch_example in mismatch_examples:
        logger.error("example %s", mismatch_example)
    sys.exit(1)


if __name__ == "__main__":
    main()