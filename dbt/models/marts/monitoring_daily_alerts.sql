-- alerts per transaction day: volume, hits, misses, amounts, and daily precision and recall
-- (null on a day with no alerts or no fraud).
-- stg_scored_transactions -> fraud.gold.monitoring_daily_alerts
WITH daily_counts AS (
    SELECT
        transaction_date
        , COUNT(*) AS transactions
        , SUM(is_fraud) AS fraud_transactions
        , SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) AS alerts
        , SUM(CASE WHEN is_alert AND is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives
        , SUM(CASE WHEN is_alert AND is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives
        , SUM(CASE WHEN NOT is_alert AND is_fraud = 1 THEN 1 ELSE 0 END) AS missed_frauds
        , SUM(CASE WHEN is_alert THEN amount ELSE 0 END) AS alerted_amount
        , SUM(CASE WHEN NOT is_alert AND is_fraud = 1 THEN amount ELSE 0 END) AS missed_fraud_amount
    FROM
        {{ ref('stg_scored_transactions') }}
    GROUP BY
        transaction_date
)
SELECT
    transaction_date
    , transactions
    , fraud_transactions
    , alerts
    , true_positives
    , false_positives
    , missed_frauds
    , alerted_amount
    , missed_fraud_amount
    , ROUND(TRY_DIVIDE(alerts, transactions), 6) AS alert_rate
    , ROUND(TRY_DIVIDE(true_positives, alerts), 4) AS precision
    , ROUND(TRY_DIVIDE(true_positives, fraud_transactions), 4) AS recall
FROM
    daily_counts