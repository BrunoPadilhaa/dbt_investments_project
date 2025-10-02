{{
    config(
        materialized='incremental'
    ,   unique_key='TRADE_ID'
    ,   on_schema_change='fail'
    )
}}

WITH cte_raw_trades AS 
(
    SELECT 
    CAST(TRADE_ID AS INT) AS TRADE_ID
    ,   CASE
            WHEN INITCAP(TRADE_TYPE) = 'Tax Iftt' THEN 'Tax IFTT'
            ELSE INITCAP(TRADE_TYPE) 
        END AS TRADE_TYPE
    ,   CAST(TRADE_TIME AS TIMESTAMP) AS TRADE_TIME
    ,   TRADE_COMMENT
    ,   STOCK_SYMBOL
    ,   CAST(AMOUNT AS NUMBER(10,2)) AS AMOUNT
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
    FROM {{source('raw','raw_trades_pt')}}
    WHERE TRADE_ID != 'Total'

    {% if is_incremental() %}
    -- Only run this WHERE clause on incremental runs
    AND LOAD_TS > COALESCE((SELECT MAX(LOAD_TS) FROM {{ this }}), '1900-01-01 00:00:00')
    {% endif %}
)

SELECT * FROM cte_raw_trades



