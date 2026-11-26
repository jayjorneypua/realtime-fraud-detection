SELECT
    trans_num AS transaction_id
    , TO_TIMESTAMP(trans_date_trans_time, 'yyyy-MM-dd HH:mm:ss') AS transaction_time
    , SHA2(CONCAT(:card_hash_salt, cc_num), 256) AS card_key
    , REGEXP_REPLACE(merchant, '^fraud_', '') AS merchant_name
    , category AS merchant_category
    , CAST(amt AS DECIMAL(12,2)) AS amount
    , gender AS customer_gender
    , city AS customer_city
    , state AS customer_state
    , SUBSTRING(LPAD(zip, 5, '0'), 1, 3) AS customer_zip3
    , CAST(lat AS DOUBLE) AS customer_latitude
    , CAST(`long` AS DOUBLE) AS customer_longitude
    , CAST(city_pop AS INT) AS customer_city_population
    , job AS customer_job
    , TO_DATE(dob) AS customer_birth_date
    , CAST(merch_lat AS DOUBLE) AS merchant_latitude
    , CAST(merch_long AS DOUBLE) AS merchant_longitude
    , CAST(is_fraud AS INT) AS is_fraud
    , _source_file AS source_file
    , _ingested_at AS ingested_at
FROM
    bronze_transactions
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY trans_num ORDER BY _ingested_at DESC) = 1