-- Test that no asset prices occurred after they were loaded into the system
SELECT *
FROM {{ ref('stg_asset_prices') }}
WHERE PRICE_DATE > LOAD_TS