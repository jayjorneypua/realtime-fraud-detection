-- live bronze transactions in the shape of bronze history: the sink's transactions with their fraud label from
-- the separate labels topic (latest label per transaction), so sql/silver_transactions.sql runs unchanged.
-- fraud.bronze.transactions_live + fraud.bronze.transaction_labels_live -> bronze_transactions (temp view)
WITH latest_labels AS (
    SELECT
        trans_num
        , is_fraud
    FROM
        fraud.bronze.transaction_labels_live
    QUALIFY
        ROW_NUMBER() OVER (PARTITION BY trans_num ORDER BY _ingested_at DESC) = 1
)
SELECT
    transactions.trans_num
    , transactions.trans_date_trans_time
    , transactions.cc_num
    , transactions.merchant
    , transactions.category
    , transactions.amt
    , transactions.gender
    , transactions.city
    , transactions.state
    , transactions.zip
    , transactions.lat
    , transactions.`long`
    , transactions.city_pop
    , transactions.job
    , transactions.dob
    , transactions.merch_lat
    , transactions.merch_long
    , latest_labels.is_fraud
    , transactions._source_file
    , transactions._ingested_at
FROM
    fraud.bronze.transactions_live AS transactions
LEFT JOIN latest_labels
    ON latest_labels.trans_num = transactions.trans_num