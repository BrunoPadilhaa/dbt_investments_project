{{
    config(
        materialized = 'incremental',
        unique_key = 'asset_code',
        incremental_strategy = 'merge'
    )
}}

WITH cte_asset_details AS 
(
SELECT
   UPPER(TRIM(ASSET_CODE)) AS ASSET_CODE, 
   UPPER(TRIM(YF_ASSET_CODE)) AS YAHOO_FINANCE_ASSET_CODE, 
   TRIM(COUNTRY) AS ASSET_COUNTRY, 
   REGEXP_REPLACE(TRIM(SHORTNAME), '\\s+', ' ') AS SHORTNAME,
   TRIM(QUOTETYPE) AS QUOTETYPE, 
   TRIM(SECTOR) AS SECTOR,
   TRIM(INDUSTRY) AS INDUSTRY, 
   TRIM(CURRENCY) AS CURRENCY, 
   TRIM(EXCHANGE) AS EXCHANGE, 
    TRIM(SOURCE_SYSTEM) AS SOURCE_SYSTEM,
   TO_TIMESTAMP(LOAD_TS) AS LOAD_TS
FROM {{source('raw','raw_asset_details')}}
)

SELECT * FROM cte_asset_details

