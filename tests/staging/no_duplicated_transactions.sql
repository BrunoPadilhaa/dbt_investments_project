SELECT 
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_transactions_clear') }}
HAVING COUNT(*) > 1
GROUP BY TRANSACTION_ID