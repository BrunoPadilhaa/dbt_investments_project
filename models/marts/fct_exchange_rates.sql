{{
    config(
        materialized = 'incremental',
        unique_key = 'EXCHANGE_RATE_ID',
        on_schema_change = 'fail'
    ,   
    )

}}

WITH t_exchange_rates AS (
    SELECT
        ABS(HASH(EXRA.RATE_DATE,EXRA.CURRENCY_FROM, EXRA.CURRENCY_TO, EXRA.SOURCE_SYSTEM)) AS EXCHANGE_RATE_ID
    ,   TO_NUMBER(TO_VARCHAR(EXRA.RATE_DATE, 'YYYYMMDD')) AS RATE_DATE_ID
    ,   CUFR.CURRENCY_ID AS CURRENCY_ID_FROM
    ,   CUTO.CURRENCY_ID AS CURRENCY_ID_TO
    ,   EXRA.EXCHANGE_RATE
    ,   EXRA.SOURCE_SYSTEM
    ,   EXRA.LOAD_TS
    ,   CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ref('stg_exchange_rates')}} EXRA

    LEFT JOIN {{ref('dim_currency')}}  CUFR
    ON CUFR.CURRENCY = EXRA.CURRENCY_FROM

    LEFT JOIN {{ref('dim_currency')}} CUTO
    ON CUTO.CURRENCY = EXRA.CURRENCY_TO

{% if is_incremental() %}
    WHERE EXRA.LOAD_TS > (
        SELECT COALESCE(MAX(LOAD_TS), '1900-01-01') 
        FROM {{ this }}
    )
{% endif %}
)

SELECT * FROM t_exchange_rates


