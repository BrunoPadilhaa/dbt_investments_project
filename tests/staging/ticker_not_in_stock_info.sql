SELECT
    TICKER
FROM {{ref('stg_transactions_pt')}}
WHERE TRANSACTION_TYPE IN ('Stock Purchase','Stock Sale')
EXCEPT
SELECT
    TICKER
FROM {{ref('stg_ticker')}}
