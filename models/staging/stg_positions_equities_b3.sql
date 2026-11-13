WITH raw_equities AS (
SELECT
    IFF(ENDSWITH(ASSET_CODE,'F'), LEFT(ASSET_CODE, LENGTH(ASSET_CODE)-1), ASSET_CODE) AS ASSET_CODE,
    TO_DECIMAL(AVAILABLE_QUANTITY,10,2) AS SHARES_HELD,
    TO_DECIMAL(MARKET_VALUE_IN_BRL, 10,2) AS MARKET_VALUE_IN_BRL,
    SOURCE_FILE,
    SOURCE_SYSTEM
FROM {{source('raw', 'raw_positions_equities_b3')}}
-- subscription rights / receipts (e.g. XPML12, FLRY1, ITSA10) are not shares: no cost, no value
WHERE NOT REGEXP_LIKE(ASSET_CODE, '^[A-Z]{4}(1|2|9|10|12|13|14|15)$')
)

SELECT
    COALESCE(conv.CURRENT_ASSET_CODE, rawe.ASSET_CODE) AS ASSET_CODE,
    SHARES_HELD,
    MARKET_VALUE_IN_BRL,
    SOURCE_FILE,
    SOURCE_SYSTEM,
    {{dbt.current_timestamp()}} AS LOAD_TS    
FROM raw_equities rawe    
    
LEFT JOIN {{ref('raw_asset_conversion_mapping')}} conv
ON rawe.ASSET_CODE = conv.RAW_ASSET_CODE