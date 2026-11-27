-- live scores: every score the scoring service published, typed, one row per transaction (latest score wins,
-- so a re-sent batch or a second replay of the same transaction does not double count).
-- fraud.bronze.scores_live -> fraud.gold.scores_live
WITH typed_scores AS (
    SELECT
        transaction_id
        , card_key
        , CAST(transaction_time AS TIMESTAMP) AS transaction_time
        , CAST(amount AS DECIMAL(12,2)) AS amount
        , merchant_category
        , CAST(fraud_score AS DOUBLE) AS fraud_score
        , CAST(alert_threshold AS DOUBLE) AS alert_threshold
        , CAST(is_alert AS BOOLEAN) AS is_alert
        , model_version
        , CAST(published_at AS TIMESTAMP) AS published_at
        , CAST(scored_at AS TIMESTAMP) AS scored_at
        , CAST(latency_ms AS DOUBLE) AS latency_ms
        , _source_file AS source_file
        , _ingested_at AS ingested_at
    FROM
        fraud.bronze.scores_live
)
SELECT
    *
FROM
    typed_scores
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY scored_at DESC, ingested_at DESC) = 1