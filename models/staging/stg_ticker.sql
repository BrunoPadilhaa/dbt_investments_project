{{
    config(
        materialized = 'incremental',
        unique_key = 'TICKER',
        incremental_strategy = 'merge'
    )
}}

WITH cte_stock_info AS 
(
SELECT
    SYMBOL AS TICKER
,   LONGNAME AS NAME
,   QUOTETYPE
,   INITCAP(COALESCE(SECTOR,'Unknown')) AS SECTOR
,   COALESCE(INDUSTRY,'Unknown') AS INDUSTRY
,   UPPER(CURRENCY) AS CURRENCY
,   UPPER(EXCHANGE) AS EXCHANGE
,   CAST(INFO_FETCH_DATE AS TIMESTAMP) AS INFO_FETCH_DATE
,   SOURCE_SYSTEM
,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
FROM {{source('raw','raw_symbol')}}
{% if is_incremental() %}

    WHERE LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{this}})
{% endif %}

)

SELECT * FROM cte_stock_info
