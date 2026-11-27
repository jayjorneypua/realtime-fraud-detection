-- fails if a model version and merchant category appear more than once.
SELECT
    model_version
    , merchant_category
    , COUNT(*) AS row_count
FROM
    {{ ref('monitoring_model_vs_actual') }}
GROUP BY
    model_version
    , merchant_category
HAVING
    COUNT(*) > 1