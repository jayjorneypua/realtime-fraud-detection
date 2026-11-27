-- live scores with the fields monitoring needs.
-- fraud.gold.scores_live -> stg_live_scores (ephemeral)
SELECT
    transaction_id
    , fraud_score
    , alert_threshold
    , is_alert
    , model_version
    , latency_ms
FROM
    {{ source('gold', 'scores_live') }}