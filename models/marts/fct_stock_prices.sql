{{
    config(
        materialized = 'incremental',
        unique_key=['ticker_id','price_date_id'],
        incremental_strategy = 'merge'
    )

}}

WITH t_stock_prices AS ( 
    SELECT
        TICK.TICKER_ID
    ,   TO_NUMBER(TO_VARCHAR(STPR.PRICE_DATE,'YYYYMMDD')) AS PRICE_DATE_ID
    ,   STPR.PRICE_OPEN
    ,   STPR.PRICE_HIGH
    ,   STPR.PRICE_LOW
    ,   STPR.PRICE_CLOSE
    ,   STPR.PRICE_ADJ_CLOSE
    ,   STPR.PRICE_VOLUME
    ,   STPR.SOURCE_SYSTEM
    ,   STPR.LOAD_TS
    ,   CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ref('stg_stock_prices')}} STPR

    LEFT
    JOIN {{ref('dim_ticker')}} TICK
    ON TICK.ORIGINAL_TICKER = STPR.TICKER

    {% if is_incremental() %}

        WHERE STPR.LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})

    {% endif %}
)

SELECT * FROM t_stock_prices


