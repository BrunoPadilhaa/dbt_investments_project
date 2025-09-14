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
        CAST(ID AS INT) AS TRADE_ID
    ,   TRADE_TYPE
    ,   CAST(TRADE_TIME AS TIMESTAMP) AS TRADE_TIME
    ,   TRADE_COMMENT
    ,   SYMBOL
    ,   CAST(AMOUNT AS NUMBER(10,2)) AS AMOUNT
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
    FROM {{source('raw','raw_trades_pt')}}
    WHERE ID != 'Total'

    {% if is_incremental() %}
    -- Only run this WHERE clause on incremental runs
    AND LOAD_TS > (SELECT MAX(LOAD_TS) FROM {{ this }})
    {% endif %}
)

SELECT * FROM cte_raw_trades



