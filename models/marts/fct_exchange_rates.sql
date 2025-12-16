{{ config(
    materialized = 'incremental',
    unique_key = ['RATE_DATE_ID', 'CURRENCY_ID_FROM', 'CURRENCY_ID_TO'],
    incremental_strategy = 'merge'
) }}

WITH t_exchange_rates AS (
    SELECT
        TO_NUMBER(TO_VARCHAR(EXRA.RATE_DATE, 'YYYYMMDD')) AS RATE_DATE_ID,
        CUFR.CURRENCY_ID AS CURRENCY_ID_FROM,
        CUTO.CURRENCY_ID AS CURRENCY_ID_TO,
        EXRA.EXCHANGE_RATE,
        EXRA.SOURCE_SYSTEM,
        EXRA.LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ ref('stg_exchange_rates') }} EXRA

    LEFT JOIN {{ ref('dim_currency') }} CUFR
        ON CUFR.CURRENCY_ABRV = UPPER(TRIM(EXRA.CURRENCY_FROM))

    LEFT JOIN {{ ref('dim_currency') }} CUTO
        ON CUTO.CURRENCY_ABRV = UPPER(TRIM(EXRA.CURRENCY_TO))

    {% if is_incremental() %}
        WHERE EXRA.LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
)

SELECT *
FROM t_exchange_rates
