-- fails on any rate outside 0 to 1 in the monitoring marts.
SELECT
    'monitoring_daily_alerts' AS mart_name
    , CAST(transaction_date AS STRING) AS row_key
FROM
    {{ ref('monitoring_daily_alerts') }}
WHERE
    alert_rate NOT BETWEEN 0 AND 1
    OR precision NOT BETWEEN 0 AND 1
    OR recall NOT BETWEEN 0 AND 1
UNION ALL
SELECT
    'monitoring_score_drift' AS mart_name
    , CAST(transaction_date AS STRING) AS row_key
FROM
    {{ ref('monitoring_score_drift') }}
WHERE
    alert_rate NOT BETWEEN 0 AND 1
    OR average_score NOT BETWEEN 0 AND 1
UNION ALL
SELECT
    'monitoring_model_vs_actual' AS mart_name
    , CONCAT(model_version, ' ', merchant_category) AS row_key
FROM
    {{ ref('monitoring_model_vs_actual') }}
WHERE
    alert_rate NOT BETWEEN 0 AND 1
    OR precision NOT BETWEEN 0 AND 1
    OR recall NOT BETWEEN 0 AND 1