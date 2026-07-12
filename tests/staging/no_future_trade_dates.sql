-- Test that no trades occurred after they were loaded into the system
SELECT *
FROM {{ ref('stg_transactions_xtb') }}
WHERE TRANSACTION_TIME > LOAD_TS