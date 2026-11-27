-- model performance against actual fraud per model version and merchant category.
-- stg_scored_transactions -> fraud.gold.monitoring_model_vs_actual
WITH category_counts AS (
    SELECT
        model_version
        , merchant_category
        , COUNT(*) AS transactions
        , SUM(is_fraud) AS fraud_transactions
        , SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) AS alerts
        , SUM(CASE WHEN is_alert AND is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives
        , SUM(CASE WHEN is_alert AND is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives
        , SUM(CASE WHEN NOT is_alert AND is_fraud = 1 THEN 1 ELSE 0 END) AS false_negatives
        , AVG(fraud_score) AS average_score
        , AVG(CASE WHEN is_fraud = 1 THEN fraud_score END) AS average_score_on_fraud
        , AVG(CASE WHEN is_fraud = 0 THEN fraud_score END) AS average_score_on_legitimate
    FROM
        {{ ref('stg_scored_transactions') }}
    GROUP BY
        model_version
        , merchant_category
)
SELECT
    model_version
    , merchant_category
    , transactions
    , fraud_transactions
    , alerts
    , true_positives
    , false_positives
    , false_negatives
    , ROUND(TRY_DIVIDE(fraud_transactions, transactions), 6) AS actual_fraud_rate
    , ROUND(TRY_DIVIDE(alerts, transactions), 6) AS alert_rate
    , ROUND(TRY_DIVIDE(true_positives, alerts), 4) AS precision
    , ROUND(TRY_DIVIDE(true_positives, fraud_transactions), 4) AS recall
    , ROUND(average_score, 6) AS average_score
    , ROUND(average_score_on_fraud, 6) AS average_score_on_fraud
    , ROUND(average_score_on_legitimate, 6) AS average_score_on_legitimate
FROM
    category_counts