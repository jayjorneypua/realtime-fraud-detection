-- fails if the daily mart does not add up to every scored transaction.
WITH daily_total AS (
    SELECT
        SUM(transactions) AS daily_transactions
    FROM
        {{ ref('monitoring_daily_alerts') }}
)
, scored_total AS (
    SELECT
        COUNT(*) AS scored_transactions
    FROM
        {{ ref('stg_scored_transactions') }}
)
SELECT
    daily_total.daily_transactions
    , scored_total.scored_transactions
FROM
    daily_total
CROSS JOIN scored_total
WHERE
    daily_total.daily_transactions <> scored_total.scored_transactions