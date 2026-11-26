WITH transactions AS (
    SELECT
        *
        , UNIX_SECONDS(transaction_time) AS transaction_epoch
        , CAST(amount AS DOUBLE) AS amount_value
    FROM
        silver_transactions
)

, card_history AS (
    SELECT
        *
        , COUNT(*) OVER (PARTITION BY card_key ORDER BY transaction_epoch RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS card_transaction_count_1h
        , SUM(amount_value) OVER (PARTITION BY card_key ORDER BY transaction_epoch RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS card_amount_sum_1h
        , COUNT(*) OVER (PARTITION BY card_key ORDER BY transaction_epoch RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS card_transaction_count_24h
        , SUM(amount_value) OVER (PARTITION BY card_key ORDER BY transaction_epoch RANGE BETWEEN 86400 PRECEDING AND CURRENT ROW) AS card_amount_sum_24h
        , AVG(amount_value) OVER (PARTITION BY card_key ORDER BY transaction_epoch RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS card_average_amount_prior_30d
        , LAG(transaction_epoch) OVER (PARTITION BY card_key ORDER BY transaction_epoch, transaction_id) AS previous_transaction_epoch
        , LAG(merchant_latitude) OVER (PARTITION BY card_key ORDER BY transaction_epoch, transaction_id) AS previous_merchant_latitude
        , LAG(merchant_longitude) OVER (PARTITION BY card_key ORDER BY transaction_epoch, transaction_id) AS previous_merchant_longitude
        , COUNT(*) OVER (PARTITION BY card_key, merchant_category ORDER BY transaction_epoch RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS prior_category_transactions_30d
        , COUNT(*) OVER (PARTITION BY card_key, merchant_name ORDER BY transaction_epoch RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS prior_merchant_transactions_30d
    FROM
        transactions
)

SELECT
    transaction_id
    , transaction_time
    , card_key
    , merchant_category
    , amount_value AS amount
    , HOUR(transaction_time) AS hour_of_day
    , DAYOFWEEK(transaction_time) AS day_of_week
    , CASE WHEN HOUR(transaction_time) >= 22 OR HOUR(transaction_time) < 6 THEN 1 ELSE 0 END AS is_night
    , CAST(FLOOR(MONTHS_BETWEEN(transaction_time, customer_birth_date) / 12) AS INT) AS customer_age_years
    , 2 * 6371.0 * ASIN(SQRT(
        POWER(SIN(RADIANS(merchant_latitude - customer_latitude) / 2), 2)
        + COS(RADIANS(customer_latitude)) * COS(RADIANS(merchant_latitude))
        * POWER(SIN(RADIANS(merchant_longitude - customer_longitude) / 2), 2)
    )) AS customer_to_merchant_km
    , card_transaction_count_1h
    , card_amount_sum_1h
    , card_transaction_count_24h
    , card_amount_sum_24h
    , card_average_amount_prior_30d
    , COALESCE(TRY_DIVIDE(amount_value, card_average_amount_prior_30d), 1.0) AS amount_to_card_average_ratio
    , COALESCE(transaction_epoch - previous_transaction_epoch, 2592000) AS seconds_since_previous_card_transaction
    , COALESCE(2 * 6371.0 * ASIN(SQRT(
        POWER(SIN(RADIANS(merchant_latitude - previous_merchant_latitude) / 2), 2)
        + COS(RADIANS(previous_merchant_latitude)) * COS(RADIANS(merchant_latitude))
        * POWER(SIN(RADIANS(merchant_longitude - previous_merchant_longitude) / 2), 2)
    )), 0.0) AS km_from_previous_merchant
    , CASE WHEN prior_category_transactions_30d = 0 THEN 1 ELSE 0 END AS is_new_category_for_card
    , CASE WHEN prior_merchant_transactions_30d = 0 THEN 1 ELSE 0 END AS is_new_merchant_for_card
    , is_fraud
FROM
    card_history