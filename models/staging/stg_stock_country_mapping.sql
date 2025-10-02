{{
    config(
        materialized = 'incremental'
    ,   unique_key = 'stock_symbol'
    ,   incremental_strategy = 'merge'
    )
}}

WITH cte_stock_country AS (
SELECT
    STOCK_SYMBOL,
    SUFFIX,
    YF_SUFFIX,
    COUNTRY_NAME,
    CONTINENT,
    SOURCE_SYSTEM,
    LOAD_TS
FROM {{source('raw','raw_stock_country_mapping')}}
)

SELECT * FROM cte_stock_country