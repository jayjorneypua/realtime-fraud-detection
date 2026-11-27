-- each scored transaction with its true label, the base for every monitoring mart.
-- stg_live_transactions + stg_live_scores -> stg_scored_transactions (ephemeral)
SELECT
    transactions.transaction_id
    , transactions.transaction_time
    , transactions.transaction_date
    , transactions.merchant_category
    , transactions.amount
    , transactions.is_fraud
    , scores.fraud_score
    , scores.alert_threshold
    , scores.is_alert
    , scores.model_version
    , scores.latency_ms
FROM
    {{ ref('stg_live_transactions') }} AS transactions
INNER JOIN {{ ref('stg_live_scores') }} AS scores
    ON scores.transaction_id = transactions.transaction_id