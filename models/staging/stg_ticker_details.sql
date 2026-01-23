{{
    config(
        materialized = 'incremental',
        unique_key = 'original_ticker',
        incremental_strategy = 'merge'
    )
}}

SELECT
   UPPER(TRIM(TICKER)) AS ORIGINAL_TICKER, 
   UPPER(TRIM(YF_TICKER)) AS YAHOO_FINANCE_TICKER, 
   TRIM(COUNTRY) AS COUNTRY, 
   REGEXP_REPLACE(TRIM(SHORTNAME), '\\s+', ' ') AS SHORTNAME,
   TRIM(QUOTETYPE) AS QUOTETYPE, 
   TRIM(SECTOR) AS SECTOR,
   TRIM(INDUSTRY) AS INDUSTRY, 
   TRIM(CURRENCY) AS CURRENCY, 
   TRIM(EXCHANGE) AS EXCHANGE, 
   TO_TIMESTAMP(LOAD_TS) AS LOAD_TS
FROM {{source('raw','raw_ticker_details')}}