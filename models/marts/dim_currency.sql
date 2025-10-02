{{
    config(
        materialized='incremental',
        unique_key='CURRENCY_ID'
    )
}}

WITH all_currencies AS (
    SELECT 
        CURRENCY,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM {{ref('stg_ticker')}}
    {% if is_incremental() %}
        WHERE LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CURRENCY, SOURCE_SYSTEM ORDER BY LOAD_TS DESC) = 1

    UNION

    SELECT 
        CURRENCY_FROM AS CURRENCY,
        SOURCE_SYSTEM,
        LOAD_TS 
    FROM {{ref('stg_exchange_rates')}}
    WHERE CURRENCY_FROM NOT IN (
        SELECT DISTINCT CURRENCY 
        FROM {{ref('stg_ticker')}}
    )
    {% if is_incremental() %}
        AND LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CURRENCY_FROM, SOURCE_SYSTEM ORDER BY LOAD_TS DESC) = 1
)

, currency_final AS (
    SELECT
        ABS(HASH(CURRENCY, SOURCE_SYSTEM)) AS CURRENCY_ID,
        CURRENCY,
        CASE 
            WHEN CURRENCY = 'USD' THEN 'US Dollar'
            WHEN CURRENCY = 'EUR' THEN 'Euro'
            WHEN CURRENCY = 'GBP' THEN 'British Pound'
            WHEN CURRENCY = 'BRL' THEN 'Brazilian Real'
            ELSE CURRENCY || ' Currency'
        END AS CURRENCY_NAME,
        SOURCE_SYSTEM,
        LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM all_currencies
)

SELECT * FROM currency_final