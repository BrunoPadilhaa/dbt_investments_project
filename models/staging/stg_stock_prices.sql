{{
    config(
        materialized = 'incremental'
    ,   unique_key = ['STOCK_SYMBOL','PRICE_DATE']
    ,   on_schema_change = 'fail'
    )
}}

WITH stock_prices AS (
SELECT 
    STOCK_SYMBOL
,   CAST(PRICE_DATE AS DATE) AS PRICE_DATE
,   CAST(PRICE_OPEN AS NUMBER(10,2)) AS PRICE_OPEN
,   CAST(PRICE_HIGH AS NUMBER(10,2)) AS PRICE_HIGH
,   CAST(PRICE_LOW AS NUMBER(10,2)) AS PRICE_LOW
,   CAST(PRICE_CLOSE AS NUMBER(10,2)) AS PRICE_CLOSE
,   CAST(PRICE_ADJ_CLOSE AS NUMBER(10,2)) AS PRICE_ADJ_CLOSE
,   CAST(PRICE_VOLUME AS INT) AS PRICE_VOLUME
,   SOURCE_SYSTEM
,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
FROM {{source('raw','raw_stock_prices')}}

{% if is_incremental() %}

    WHERE LOAD_TS > COALESCE((SELECT MAX(LOAD_TS) FROM {{this}}),'1900-01-01 00:00:00')

{% endif %}

)

SELECT * FROM stock_prices