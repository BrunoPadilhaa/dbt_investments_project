{{
    config(
        materialized = 'incremental',
        unique_key = 'original_ticker',
        incremental_strategy = 'merge'
    )
}}

WITH cte_ticker AS 
(
SELECT
    UPPER(TRIM(SYMBOL)) AS ORIGINAL_TICKER
,   UPPER(TRIM(CURRENT_TICKER)) AS TICKER
,   UPPER(TRIM(TICKER_NAME)) AS TICKER_NAME
,   REPLACE(INITCAP(TRIM(BUCKET_TYPE)), '_', ' ') AS BUCKET_TYPE //Will create a mapping table for BUCKET_TYPE instead
,   UPPER(TRIM(EXCHANGE_SUFFIX)) AS EXCHANGE_SUFFIX
,   UPPER(TRIM(COUNTRY_CODE)) AS COUNTRY_CODE    
,   TRIM(SOURCE_SYSTEM) AS SOURCE_SYSTEM
,   TO_TIMESTAMP(LOAD_TS) AS LOAD_TS
FROM {{ ref('raw_ticker') }}
)

SELECT * FROM cte_ticker