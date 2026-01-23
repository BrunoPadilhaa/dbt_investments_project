{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='CURRENCY_ABRV',
) }}

WITH all_currencies AS (
    -- SELECT UPPER(TRIM(CURRENCY)) AS CURRENCY_ABRV, MAX(LOAD_TS) AS LOAD_TS
    -- FROM {{ ref('stg_ticker') }}
    -- WHERE CURRENCY IS NOT NULL
    -- GROUP BY 1

    -- UNION ALL

    SELECT UPPER(TRIM(CURRENCY_FROM)) AS CURRENCY_ABRV, MAX(LOAD_TS) AS LOAD_TS
    FROM {{ ref('stg_exchange_rates') }}
    WHERE CURRENCY_FROM IS NOT NULL
    GROUP BY 1
),

currency_final AS (
    SELECT
       {{ dbt_utils.generate_surrogate_key(['CURRENCY_ABRV']) }} AS CURRENCY_ID,
        CURRENCY_ABRV,
        CASE CURRENCY_ABRV
            WHEN 'USD' THEN 'US Dollar'
            WHEN 'EUR' THEN 'Euro'
            WHEN 'GBP' THEN 'British Pound'
            WHEN 'BRL' THEN 'Brazilian Real'
            ELSE CURRENCY_ABRV || ' Currency'
        END AS CURRENCY_NAME,
        MAX(LOAD_TS) AS LOAD_TS,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM all_currencies
    GROUP BY CURRENCY_ABRV
)

SELECT 
    *
FROM currency_final
