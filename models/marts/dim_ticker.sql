{{
    config(
        materialized='incremental',
        unique_key='ticker_id',
        incremental_strategy='merge'
    )
}}

WITH CTE_TICKER AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['TICK.ORIGINAL_TICKER'])}} AS TICKER_ID,
        TICK.ORIGINAL_TICKER,
        TICK.TICKER,
        TICK.TICKER_NAME,
        TICK.BUCKET_TYPE,
        TICK.COUNTRY_CODE,
        TIDE.YAHOO_FINANCE_TICKER,
        TIDE.COUNTRY,
        TIDE.SHORTNAME,
        TIDE.QUOTETYPE,
        TIDE.SECTOR,
        TIDE.INDUSTRY,
        TIDE.CURRENCY,
        TIDE.EXCHANGE,
        TICK.SOURCE_SYSTEM,
        TICK.LOAD_TS
    FROM {{ ref('stg_ticker') }} TICK
    
    LEFT JOIN {{ ref('stg_ticker_details') }} TIDE
        ON TICK.ORIGINAL_TICKER = TIDE.ORIGINAL_TICKER
)

SELECT * FROM CTE_TICKER