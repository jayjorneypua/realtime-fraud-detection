-- fails if a score has no live transaction to join to (it would silently drop out of every mart).
SELECT
    scores.transaction_id
FROM
    {{ source('gold', 'scores_live') }} AS scores
LEFT JOIN {{ source('silver', 'transactions_live') }} AS transactions
    ON transactions.transaction_id = scores.transaction_id
WHERE
    transactions.transaction_id IS NULL