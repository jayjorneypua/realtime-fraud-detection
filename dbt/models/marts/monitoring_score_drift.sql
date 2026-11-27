-- daily score distribution and drift against the first 7 days of live scores (the reference period).
-- psi compares the share of transactions in 10 fixed score bins; < 0.1 stable, < 0.25 moderate, else significant.
-- stg_scored_transactions -> fraud.gold.monitoring_score_drift
WITH scored_bins AS (
    SELECT
        transaction_date
        , fraud_score
        , is_alert
        , CASE
            WHEN fraud_score < 0.001 THEN 0
            WHEN fraud_score < 0.01 THEN 1
            WHEN fraud_score < 0.05 THEN 2
            WHEN fraud_score < 0.1 THEN 3
            WHEN fraud_score < 0.25 THEN 4
            WHEN fraud_score < 0.5 THEN 5
            WHEN fraud_score < 0.75 THEN 6
            WHEN fraud_score < 0.9 THEN 7
            WHEN fraud_score < 0.99 THEN 8
            ELSE 9
        END AS score_bin
    FROM
        {{ ref('stg_scored_transactions') }}
)
, reference_period AS (
    SELECT
        DATE_ADD(MIN(transaction_date), 6) AS last_reference_date
    FROM
        scored_bins
)
, all_bins AS (
    SELECT
        EXPLODE(SEQUENCE(0, 9)) AS score_bin
)
, daily_totals AS (
    SELECT
        transaction_date
        , COUNT(*) AS transactions
        , AVG(fraud_score) AS average_score
        , PERCENTILE_APPROX(fraud_score, 0.5) AS score_p50
        , PERCENTILE_APPROX(fraud_score, 0.95) AS score_p95
        , AVG(CASE WHEN is_alert THEN 1.0 ELSE 0.0 END) AS alert_rate
    FROM
        scored_bins
    GROUP BY
        transaction_date
)
, daily_bin_counts AS (
    SELECT
        transaction_date
        , score_bin
        , COUNT(*) AS bin_transactions
    FROM
        scored_bins
    GROUP BY
        transaction_date
        , score_bin
)
, reference_bin_counts AS (
    SELECT
        scored_bins.score_bin
        , COUNT(*) AS reference_transactions
    FROM
        scored_bins
    CROSS JOIN reference_period
    WHERE
        scored_bins.transaction_date <= reference_period.last_reference_date
    GROUP BY
        scored_bins.score_bin
)
, reference_total AS (
    SELECT
        SUM(reference_transactions) AS reference_transactions
    FROM
        reference_bin_counts
)
, bin_shares AS (
    -- a 0.0001 floor keeps LN defined for empty bins
    SELECT
        daily_totals.transaction_date
        , all_bins.score_bin
        , GREATEST(COALESCE(daily_bin_counts.bin_transactions, 0) / daily_totals.transactions, 0.0001) AS daily_share
        , GREATEST(COALESCE(reference_bin_counts.reference_transactions, 0) / reference_total.reference_transactions, 0.0001) AS reference_share
    FROM
        daily_totals
    CROSS JOIN all_bins
    CROSS JOIN reference_total
    LEFT JOIN daily_bin_counts
        ON daily_bin_counts.transaction_date = daily_totals.transaction_date
        AND daily_bin_counts.score_bin = all_bins.score_bin
    LEFT JOIN reference_bin_counts
        ON reference_bin_counts.score_bin = all_bins.score_bin
)
, daily_psi AS (
    SELECT
        transaction_date
        , SUM((daily_share - reference_share) * LN(daily_share / reference_share)) AS psi
    FROM
        bin_shares
    GROUP BY
        transaction_date
)
SELECT
    daily_totals.transaction_date
    , daily_totals.transactions
    , ROUND(daily_totals.average_score, 6) AS average_score
    , ROUND(daily_totals.score_p50, 6) AS score_p50
    , ROUND(daily_totals.score_p95, 6) AS score_p95
    , ROUND(daily_totals.alert_rate, 6) AS alert_rate
    , ROUND(daily_psi.psi, 4) AS psi_vs_reference
    , daily_totals.transaction_date <= reference_period.last_reference_date AS is_reference_period
    , CASE
        WHEN daily_psi.psi < 0.1 THEN 'stable'
        WHEN daily_psi.psi < 0.25 THEN 'moderate'
        ELSE 'significant'
    END AS drift_level
FROM
    daily_totals
INNER JOIN daily_psi
    ON daily_psi.transaction_date = daily_totals.transaction_date
CROSS JOIN reference_period