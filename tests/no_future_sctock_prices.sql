-- Test that no stock prices occurred after they were loaded into the system
SELECT *
FROM {{ ref('stg_stock_prices') }}
WHERE PRICE_DATE > LOAD_TS