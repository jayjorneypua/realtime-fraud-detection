-- redis backfill history: every transaction in the last 31 days of silver history (the 30 day feature
-- lookback plus the scoring service's one day retention buffer), plus each card's latest transaction ever,
-- which the scoring service uses as "previous" when a card's 30 day window is empty.
-- fraud.silver.transactions_history -> streaming/redis_backfill.py -> redis card history
WITH silver_with_epoch AS (
    SELECT
        transaction_id
        , card_key
        , UNIX_TIMESTAMP(transaction_time) AS transaction_epoch
        , CAST(ROUND(amount, 2) AS DOUBLE) AS amount
        , merchant_category
        , merchant_name
        , CAST(merchant_latitude AS DOUBLE) AS merchant_latitude
        , CAST(merchant_longitude AS DOUBLE) AS merchant_longitude
    FROM fraud.silver.transactions_history
)
, ranked_by_card AS (
    SELECT
        silver_with_epoch.*
        , ROW_NUMBER() OVER (
            PARTITION BY card_key
            ORDER BY transaction_epoch DESC, transaction_id DESC
        ) AS card_recency_rank
    FROM silver_with_epoch
)
, history_end AS (
    SELECT
        MAX(transaction_epoch) AS last_epoch
    FROM silver_with_epoch
)
SELECT
    ranked_by_card.card_key
    , ranked_by_card.transaction_id
    , ranked_by_card.transaction_epoch
    , ranked_by_card.amount
    , ranked_by_card.merchant_category
    , ranked_by_card.merchant_name
    , ranked_by_card.merchant_latitude
    , ranked_by_card.merchant_longitude
    , ranked_by_card.card_recency_rank = 1 AS is_card_latest
FROM ranked_by_card
CROSS JOIN history_end
WHERE ranked_by_card.transaction_epoch > history_end.last_epoch - (31 * 86400)
    OR ranked_by_card.card_recency_rank = 1