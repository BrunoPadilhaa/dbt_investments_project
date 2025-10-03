{{
    config(
        materialized='incremental',
        unique_key='TICKER_ID',
        incremental_strategy='merge'
    )
}}

WITH CTE_STOCK_COUNTRY AS (
    SELECT
        ABS(HASH(STIN.TICKER, STIN.SOURCE_SYSTEM)) AS TICKER_ID,
        SPLIT_PART(STIN.TICKER, '.', 1) AS TICKER,
        STIN.TICKER AS ORIGINAL_TICKER,
        STIN.NAME,
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
        STIN.SOURCE_SYSTEM,
        STIN.INFO_FETCH_DATE,
        STIN.LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ ref('stg_ticker') }} STIN
    LEFT JOIN {{ ref('stg_stock_country_mapping') }} SCMA
        ON SCMA.TICKER = STIN.TICKER
    LEFT JOIN {{ ref('dim_currency') }} CURR
        ON CURR.CURRENCY = STIN.CURRENCY
        
    {% if is_incremental() %}
        WHERE STIN.LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
)

SELECT * FROM CTE_STOCK_COUNTRY