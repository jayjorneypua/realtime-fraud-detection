"""
redis backfill: loads the card history the scoring service needs before the replay starts.

the replay streams fraudTest, which begins where the silver history ends. without a backfill every card starts
with an empty 30 day window and the online features would not match the sql features. this runs
sql/redis_backfill_history.sql on the sql warehouse and writes the rows in exactly the format the scoring
service reads (same to_member, same keys). run it on an empty redis, right after start.ps1.

fraud.silver.transactions_history -> sql warehouse -> redis card:<card_key>:history and card:<card_key>:latest
"""

import logging
import os
import time
from pathlib import Path

import redis
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementResponse, StatementState
from online_features import CardTransaction
from scoring_service import DATABRICKS_PROFILE, REDIS_HOST, REDIS_PORT, to_member

WAREHOUSE_ID: str = "606b0c60dc21c8fd"
HISTORY_SQL_PATH: Path = Path(__file__).resolve().parent.parent / "sql" / "redis_backfill_history.sql"
POLL_SECONDS: int = 5
PIPELINE_BATCH_SIZE: int = 5000

logger: logging.Logger = logging.getLogger("redis_backfill")


def wait_for_statement(workspace_client: WorkspaceClient, statement: StatementResponse) -> StatementResponse:
    while statement.status.state in (StatementState.PENDING, StatementState.RUNNING):
        time.sleep(POLL_SECONDS)
        statement = workspace_client.statement_execution.get_statement(statement.statement_id)
    if statement.status.state != StatementState.SUCCEEDED:
        error_message: str = statement.status.error.message if statement.status.error else "no error message"
        raise RuntimeError(f"history query ended {statement.status.state.value}: {error_message}")
    return statement


def read_history_rows(workspace_client: WorkspaceClient) -> list[dict[str, str]]:
    statement: StatementResponse = workspace_client.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=HISTORY_SQL_PATH.read_text(encoding="utf-8"),
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    statement = wait_for_statement(workspace_client, statement)

    column_names: list[str] = [column.name for column in statement.manifest.schema.columns]
    raw_rows: list[list[str]] = list(statement.result.data_array or [])
    next_chunk_index: int | None = statement.result.next_chunk_index
    while next_chunk_index is not None:
        chunk = workspace_client.statement_execution.get_statement_result_chunk_n(statement.statement_id, next_chunk_index)
        raw_rows.extend(chunk.data_array or [])
        next_chunk_index = chunk.next_chunk_index

    return [dict(zip(column_names, raw_row)) for raw_row in raw_rows]


def to_card_transaction(row: dict[str, str]) -> CardTransaction:
    return CardTransaction(
        transaction_id=row["transaction_id"],
        transaction_epoch=int(row["transaction_epoch"]),
        amount=round(float(row["amount"]), 2),
        merchant_category=row["merchant_category"],
        merchant_name=row["merchant_name"],
        merchant_latitude=float(row["merchant_latitude"]),
        merchant_longitude=float(row["merchant_longitude"]),
    )


def write_history(history_rows: list[dict[str, str]], redis_client: redis.Redis) -> int:
    pipeline = redis_client.pipeline(transaction=False)
    latest_count: int = 0
    for row_number, row in enumerate(history_rows, start=1):
        transaction: CardTransaction = to_card_transaction(row)
        member: str = to_member(transaction)
        pipeline.zadd(f"card:{row['card_key']}:history", {member: transaction.transaction_epoch})
        if row["is_card_latest"] == "true":
            pipeline.set(f"card:{row['card_key']}:latest", member)
            latest_count += 1
        if row_number % PIPELINE_BATCH_SIZE == 0:
            pipeline.execute()
    pipeline.execute()
    return latest_count


def main() -> None:
    os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", DATABRICKS_PROFILE)

    redis_client: redis.Redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
    redis_client.ping()
    existing_key_count: int = redis_client.dbsize()
    if existing_key_count > 0:
        raise RuntimeError(f"redis already holds {existing_key_count} keys; run .\\stop.ps1 then .\\start.ps1 first")

    logger.info("reading the last 31 days of silver history from warehouse %s", WAREHOUSE_ID)
    history_rows: list[dict[str, str]] = read_history_rows(WorkspaceClient())
    logger.info("read %d rows, writing to redis", len(history_rows))

    card_count: int = write_history(history_rows, redis_client)
    logger.info("backfill done: %d rows for %d cards, %d redis keys", len(history_rows), card_count, redis_client.dbsize())


if __name__ == "__main__":
    main()