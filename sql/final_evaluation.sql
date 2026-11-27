-- final evaluation: live scores from the full fraudTest replay against the true labels, at the model's alert
-- threshold (precision, recall, alerts) and over all thresholds (PR-AUC as average precision).
-- fraud.gold.scores_live + fraud.bronze.transaction_labels_live -> one summary row
WITH latest_labels AS (
    SELECT
        trans_num
        , CAST(is_fraud AS INT) AS is_fraud
    FROM
        fraud.bronze.transaction_labels_live
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY trans_num ORDER BY _ingested_at DESC) = 1
)
, scored_transactions AS (
    SELECT
        scores.transaction_id
        , scores.fraud_score
        , scores.is_alert
        , scores.alert_threshold
        , scores.model_version
        , latest_labels.is_fraud
    FROM
        fraud.gold.scores_live AS scores
    INNER JOIN latest_labels
        ON latest_labels.trans_num = scores.transaction_id
)
, ranked_transactions AS (
    SELECT
        is_fraud
        , SUM(is_fraud) OVER (ORDER BY fraud_score DESC, transaction_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS frauds_so_far
        , ROW_NUMBER() OVER (ORDER BY fraud_score DESC, transaction_id) AS rank_position
    FROM
        scored_transactions
)
, average_precision AS (
    SELECT
        SUM(CASE WHEN is_fraud = 1 THEN frauds_so_far / rank_position END) / SUM(is_fraud) AS pr_auc
    FROM
        ranked_transactions
)
, confusion_counts AS (
    SELECT
        COUNT(*) AS scored_and_labeled
        , SUM(is_fraud) AS fraud_transactions
        , SUM(CASE WHEN is_alert THEN 1 ELSE 0 END) AS alerts
        , SUM(CASE WHEN is_alert AND is_fraud = 1 THEN 1 ELSE 0 END) AS true_positives
        , SUM(CASE WHEN is_alert AND is_fraud = 0 THEN 1 ELSE 0 END) AS false_positives
        , SUM(CASE WHEN NOT is_alert AND is_fraud = 1 THEN 1 ELSE 0 END) AS false_negatives
        , MIN(alert_threshold) AS alert_threshold
        , COUNT(DISTINCT model_version) AS model_versions
    FROM
        scored_transactions
)
SELECT
    (SELECT COUNT(*) FROM fraud.gold.scores_live) AS scored_total
    , confusion_counts.scored_and_labeled
    , confusion_counts.fraud_transactions
    , confusion_counts.alerts
    , confusion_counts.true_positives
    , confusion_counts.false_positives
    , confusion_counts.false_negatives
    , ROUND(confusion_counts.true_positives / confusion_counts.alerts, 4) AS precision
    , ROUND(confusion_counts.true_positives / confusion_counts.fraud_transactions, 4) AS recall
    , ROUND(average_precision.pr_auc, 4) AS pr_auc
    , confusion_counts.alert_threshold
    , confusion_counts.model_versions
FROM
    confusion_counts
CROSS JOIN average_precision