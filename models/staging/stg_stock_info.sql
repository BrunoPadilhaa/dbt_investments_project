{{
    config(
        materialized = 'incremental'
    ,   unique_key = 'stock_symbol'
    ,   incremental_strategy = 'merge'
    )
}}

WITH cte_stock_info AS 
(
SELECT
    STOCK_SYMBOL
,   LONGNAME AS NAME
,   QUOTETYPE
,   INITCAP(COALESCE(SECTOR,'Unknown')) AS SECTOR
,   COALESCE(INDUSTRY,'Unknown') AS INDUSTRY
,   UPPER(CURRENCY) AS CURRENCY
,   UPPER(EXCHANGE) AS EXCHANGE
,   CAST(INFO_FETCH_DATE AS TIMESTAMP) AS INFO_FETCH_DATE
,   SOURCE_SYSTEM
,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
FROM {{source('raw','raw_stock_info')}}
)

SELECT * FROM cte_stock_info
