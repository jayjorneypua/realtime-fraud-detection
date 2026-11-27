-- feature parity sample: every transaction in the last day of silver history, with its inputs, its previous
-- card transaction (same LAG ordering as transaction_features.sql) and the gold feature values to compare.
-- fraud.silver.transactions_history + fraud.gold.transaction_features_history -> streaming/feature_parity_check.py
WITH silver_with_epoch AS (
    SELECT
        transaction_id
        , card_key
        , transaction_time
        , UNIX_TIMESTAMP(transaction_time) AS transaction_epoch
        , CAST(ROUND(amount, 2) AS DOUBLE) AS amount
        , merchant_category
        , merchant_name
        , CAST(merchant_latitude AS DOUBLE) AS merchant_latitude
        , CAST(merchant_longitude AS DOUBLE) AS merchant_longitude
        , customer_birth_date
        , CAST(customer_latitude AS DOUBLE) AS customer_latitude
        , CAST(customer_longitude AS DOUBLE) AS customer_longitude
    FROM fraud.silver.transactions_history
)
, with_previous AS (
    SELECT
        silver_with_epoch.*
        , LAG(transaction_id) OVER card_order AS previous_transaction_id
        , LAG(transaction_epoch) OVER card_order AS previous_transaction_epoch
        , LAG(amount) OVER card_order AS previous_amount
        , LAG(merchant_category) OVER card_order AS previous_merchant_category
        , LAG(merchant_name) OVER card_order AS previous_merchant_name
        , LAG(merchant_latitude) OVER card_order AS previous_merchant_latitude
        , LAG(merchant_longitude) OVER card_order AS previous_merchant_longitude
    FROM silver_with_epoch
    WINDOW card_order AS (PARTITION BY card_key ORDER BY transaction_epoch, transaction_id)
)
, history_end AS (
    SELECT
        MAX(transaction_epoch) AS last_epoch
    FROM silver_with_epoch
)
SELECT
    with_previous.transaction_id
    , with_previous.card_key
    , DATE_FORMAT(with_previous.transaction_time, 'yyyy-MM-dd HH:mm:ss') AS transaction_time_text
    , with_previous.transaction_epoch
    , with_previous.amount
    , with_previous.merchant_category
    , with_previous.merchant_name
    , with_previous.merchant_latitude
    , with_previous.merchant_longitude
    , with_previous.customer_birth_date
    , with_previous.customer_latitude
    , with_previous.customer_longitude
    , with_previous.previous_transaction_id
    , with_previous.previous_transaction_epoch
    , with_previous.previous_amount
    , with_previous.previous_merchant_category
    , with_previous.previous_merchant_name
    , with_previous.previous_merchant_latitude
    , with_previous.previous_merchant_longitude
    , features.merchant_category AS sql_merchant_category
    , features.amount AS sql_amount
    , features.hour_of_day AS sql_hour_of_day
    , features.day_of_week AS sql_day_of_week
    , features.is_night AS sql_is_night
    , features.customer_age_years AS sql_customer_age_years
    , features.customer_to_merchant_km AS sql_customer_to_merchant_km
    , features.card_transaction_count_1h AS sql_card_transaction_count_1h
    , features.card_amount_sum_1h AS sql_card_amount_sum_1h
    , features.card_transaction_count_24h AS sql_card_transaction_count_24h
    , features.card_amount_sum_24h AS sql_card_amount_sum_24h
    , features.amount_to_card_average_ratio AS sql_amount_to_card_average_ratio
    , features.seconds_since_previous_card_transaction AS sql_seconds_since_previous_card_transaction
    , features.km_from_previous_merchant AS sql_km_from_previous_merchant
    , features.is_new_category_for_card AS sql_is_new_category_for_card
    , features.is_new_merchant_for_card AS sql_is_new_merchant_for_card
FROM with_previous
INNER JOIN fraud.gold.transaction_features_history AS features
    ON features.transaction_id = with_previous.transaction_id
CROSS JOIN history_end
WHERE with_previous.transaction_epoch > history_end.last_epoch - 86400