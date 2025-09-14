-- Test that no trades occurred after they were loaded into the system
SELECT *
FROM {{ ref('stg_exchange_rates') }}
WHERE RATE_DATE > LOAD_TS