{{
    config(
        materialized='incremental',
        unique_key='ticker_id',
        incremental_strategy='merge'
    )
}}

WITH CTE_STOCK_COUNTRY AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['STIN.TICKER'])}} AS TICKER_ID,
        SPLIT_PART(STIN.TICKER, '.', 1) AS TICKER,
        STIN.TICKER AS ORIGINAL_TICKER,
        STIN.TICKER_NAME,
        STIN.QUOTETYPE,
        CASE
            WHEN STIN.SECTOR = 'Real Estate' THEN 'REIT'
            WHEN STIN.QUOTETYPE = 'EQUITY' THEN 'STOCKS'
            WHEN STIN.QUOTETYPE = 'ETF' THEN STIN.QUOTETYPE
            ELSE 'OTHERS'
        END AS TYPE,
        STIN.SECTOR,
        STIN.INDUSTRY,
        CURR.CURRENCY_ID,
        STIN.EXCHANGE,
        SCMA.COUNTRY_NAME,
        SCMA.CONTINENT,
        STIN.INFO_FETCH_DATE,
        STIN.LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ ref('stg_ticker') }} STIN
    LEFT JOIN {{ ref('stg_stock_country_mapping') }} SCMA
        ON SCMA.TICKER = STIN.TICKER
    LEFT JOIN {{ ref('dim_currency') }} CURR
        ON CURR.CURRENCY_ABRV = STIN.CURRENCY
)

SELECT * FROM CTE_STOCK_COUNTRY