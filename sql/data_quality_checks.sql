-- data quality checks on the live tables: one row per check with the number of failing rows.
-- severity: error = wrong data that must not reach marts (fails the job run), warning = suspect data to
-- investigate, info = tracked for monitoring only.
-- fraud.bronze.*_live, fraud.silver.transactions_live, fraud.gold.scores_live -> 08_data_quality.py
SELECT
    'transaction_id_missing' AS check_name
    , 'fraud.silver.transactions_live' AS table_name
    , 'error' AS severity
    , COUNT_IF(transaction_id IS NULL) AS failed_rows
    , COUNT(*) AS checked_rows
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'transaction_id_duplicated'
    , 'fraud.silver.transactions_live'
    , 'error'
    , COUNT(*) - COUNT(DISTINCT transaction_id)
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'card_key_missing'
    , 'fraud.silver.transactions_live'
    , 'error'
    , COUNT_IF(card_key IS NULL)
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'transaction_time_missing'
    , 'fraud.silver.transactions_live'
    , 'error'
    , COUNT_IF(transaction_time IS NULL)
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'amount_missing_or_not_positive'
    , 'fraud.silver.transactions_live'
    , 'error'
    , COUNT_IF(amount IS NULL OR amount <= 0)
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'customer_birth_date_missing'
    , 'fraud.silver.transactions_live'
    , 'error'
    , COUNT_IF(customer_birth_date IS NULL)
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'coordinates_out_of_range'
    , 'fraud.silver.transactions_live'
    , 'warning'
    , COUNT_IF(
        customer_latitude IS NULL OR customer_latitude NOT BETWEEN -90 AND 90
        OR customer_longitude IS NULL OR customer_longitude NOT BETWEEN -180 AND 180
        OR merchant_latitude IS NULL OR merchant_latitude NOT BETWEEN -90 AND 90
        OR merchant_longitude IS NULL OR merchant_longitude NOT BETWEEN -180 AND 180
    )
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'label_missing'
    , 'fraud.silver.transactions_live'
    , 'warning'
    , COUNT_IF(is_fraud IS NULL)
    , COUNT(*)
FROM
    fraud.silver.transactions_live

UNION ALL
SELECT
    'transaction_not_scored'
    , 'fraud.silver.transactions_live'
    , 'warning'
    , COUNT_IF(scores.transaction_id IS NULL)
    , COUNT(*)
FROM
    fraud.silver.transactions_live AS transactions
LEFT JOIN fraud.gold.scores_live AS scores
    ON scores.transaction_id = transactions.transaction_id

UNION ALL
SELECT
    'score_without_transaction'
    , 'fraud.gold.scores_live'
    , 'warning'
    , COUNT_IF(transactions.transaction_id IS NULL)
    , COUNT(*)
FROM
    fraud.gold.scores_live AS scores
LEFT JOIN fraud.silver.transactions_live AS transactions
    ON transactions.transaction_id = scores.transaction_id

UNION ALL
SELECT
    'score_out_of_range'
    , 'fraud.gold.scores_live'
    , 'error'
    , COUNT_IF(fraud_score IS NULL OR fraud_score NOT BETWEEN 0 AND 1)
    , COUNT(*)
FROM
    fraud.gold.scores_live

UNION ALL
SELECT
    'alert_flag_inconsistent'
    , 'fraud.gold.scores_live'
    , 'error'
    , COUNT_IF(is_alert IS NULL OR is_alert <> (fraud_score >= alert_threshold))
    , COUNT(*)
FROM
    fraud.gold.scores_live

UNION ALL
SELECT
    'model_version_missing'
    , 'fraud.gold.scores_live'
    , 'error'
    , COUNT_IF(model_version IS NULL)
    , COUNT(*)
FROM
    fraud.gold.scores_live

UNION ALL
SELECT
    'latency_over_5_seconds'
    , 'fraud.gold.scores_live'
    , 'info'
    , COUNT_IF(latency_ms > 5000)
    , COUNT(*)
FROM
    fraud.gold.scores_live

UNION ALL
SELECT
    'transaction_schema_drift'
    , 'fraud.bronze.transactions_live'
    , 'warning'
    , COUNT_IF(_rescued_data IS NOT NULL)
    , COUNT(*)
FROM
    fraud.bronze.transactions_live

UNION ALL
SELECT
    'score_schema_drift'
    , 'fraud.bronze.scores_live'
    , 'warning'
    , COUNT_IF(_rescued_data IS NOT NULL)
    , COUNT(*)
FROM
    fraud.bronze.scores_live