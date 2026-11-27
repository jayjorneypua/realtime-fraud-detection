-- feature parity history: the 30 day card history the online features need for the parity sample, i.e. every
-- transaction of the sampled cards from 31 days before the end of silver history.
-- fraud.silver.transactions_history -> streaming/feature_parity_check.py
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
, history_end AS (
    SELECT
        MAX(transaction_epoch) AS last_epoch
    FROM silver_with_epoch
)
, sampled_cards AS (
    SELECT DISTINCT
        silver_with_epoch.card_key
    FROM silver_with_epoch
    CROSS JOIN history_end
    WHERE silver_with_epoch.transaction_epoch > history_end.last_epoch - 86400
)
SELECT
    silver_with_epoch.card_key
    , silver_with_epoch.transaction_id
    , silver_with_epoch.transaction_epoch
    , silver_with_epoch.amount
    , silver_with_epoch.merchant_category
    , silver_with_epoch.merchant_name
    , silver_with_epoch.merchant_latitude
    , silver_with_epoch.merchant_longitude
FROM silver_with_epoch
INNER JOIN sampled_cards
    ON sampled_cards.card_key = silver_with_epoch.card_key
CROSS JOIN history_end
WHERE silver_with_epoch.transaction_epoch >= history_end.last_epoch - (31 * 86400)