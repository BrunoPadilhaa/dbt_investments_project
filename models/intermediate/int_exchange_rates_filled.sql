{{ config(
    materialized = 'table'
) }}

WITH dates_spine AS (
    SELECT DATE_ID AS RATE_DATE_ID
    FROM {{ ref("dim_date") }}
),

t_exchange_rates AS (
    SELECT
        TO_NUMBER(TO_VARCHAR(EXRA.RATE_DATE, 'YYYYMMDD')) AS RATE_DATE_ID,
        CUFR.CURRENCY_ID                                  AS CURRENCY_ID_FROM,
        CUTO.CURRENCY_ID                                  AS CURRENCY_ID_TO,
        EXRA.EXCHANGE_RATE,
        EXRA.SOURCE_SYSTEM,
        EXRA.LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                AS DBT_UPDATED_AT
    FROM {{ ref('stg_exchange_rates') }} EXRA
    LEFT JOIN {{ ref('dim_currency') }} CUFR
        ON CUFR.CURRENCY_ABRV = UPPER(TRIM(EXRA.CURRENCY_FROM))
    LEFT JOIN {{ ref('dim_currency') }} CUTO
        ON CUTO.CURRENCY_ABRV = UPPER(TRIM(EXRA.CURRENCY_TO))
),

currency_pairs AS (
    SELECT DISTINCT CURRENCY_ID_FROM, CURRENCY_ID_TO
    FROM t_exchange_rates
),

spine_x_pairs AS (
    SELECT
        D.RATE_DATE_ID,
        P.CURRENCY_ID_FROM,
        P.CURRENCY_ID_TO
    FROM dates_spine D
    CROSS JOIN currency_pairs P
),

filled AS (
    SELECT
        S.RATE_DATE_ID,
        S.CURRENCY_ID_FROM,
        S.CURRENCY_ID_TO,
        LAST_VALUE(R.EXCHANGE_RATE IGNORE NULLS) OVER (
            PARTITION BY S.CURRENCY_ID_FROM, S.CURRENCY_ID_TO
            ORDER BY S.RATE_DATE_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                 AS EXCHANGE_RATE,
        LAST_VALUE(R.SOURCE_SYSTEM IGNORE NULLS) OVER (
            PARTITION BY S.CURRENCY_ID_FROM, S.CURRENCY_ID_TO
            ORDER BY S.RATE_DATE_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                 AS SOURCE_SYSTEM,
        LAST_VALUE(R.LOAD_TS IGNORE NULLS) OVER (
            PARTITION BY S.CURRENCY_ID_FROM, S.CURRENCY_ID_TO
            ORDER BY S.RATE_DATE_ID
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                 AS LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                AS DBT_UPDATED_AT
    FROM spine_x_pairs S
    LEFT JOIN t_exchange_rates R
        ON R.RATE_DATE_ID     = S.RATE_DATE_ID
        AND R.CURRENCY_ID_FROM = S.CURRENCY_ID_FROM
        AND R.CURRENCY_ID_TO   = S.CURRENCY_ID_TO
)

SELECT *
FROM filled
WHERE EXCHANGE_RATE IS NOT NULL  -- exclude dates before first ever rate was loaded
