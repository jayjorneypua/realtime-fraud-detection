-- live transactions with the fields monitoring needs.
-- fraud.silver.transactions_live -> stg_live_transactions (ephemeral)
SELECT
    transaction_id
    , transaction_time
    , CAST(transaction_time AS DATE) AS transaction_date
    , merchant_category
    , amount
    , is_fraud
FROM
    {{ source('silver', 'transactions_live') }}