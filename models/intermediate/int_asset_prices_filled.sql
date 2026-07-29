{{ config(
    materialized = 'table'
) }}

WITH base_prices AS (
    SELECT
        ASSET.ASSET_ID,
        TO_NUMBER(TO_VARCHAR(ASPR.PRICE_DATE,'YYYYMMDD')) AS PRICE_DATE_ID,
        ASPR.PRICE_ADJ_CLOSE,
        CURR.CURRENCY_ID AS PRICE_CURRENCY_ID
    FROM {{ ref('stg_asset_prices') }} ASPR
    LEFT JOIN {{ ref('dim_asset') }} ASSET
        ON ASSET.ASSET_CODE = ASPR.ASSET_CODE
    LEFT JOIN {{ ref('dim_currency') }} CURR
        ON CURR.CURRENCY_ABRV = ASPR.CURRENCY
),

dates_spine AS (
    SELECT DATE_ID AS PRICE_DATE_ID
    FROM {{ ref("dim_date") }}
),

asset_currency_pairs AS (
    SELECT DISTINCT
        ASSET_ID,
        PRICE_CURRENCY_ID
    FROM base_prices
),

spine_x_pairs AS (
    SELECT
        D.PRICE_DATE_ID,
        P.ASSET_ID,
        P.PRICE_CURRENCY_ID
    FROM dates_spine D
    CROSS JOIN asset_currency_pairs P
),

filled AS (
    SELECT
        S.PRICE_DATE_ID,
        S.ASSET_ID,
        S.PRICE_CURRENCY_ID,
        {{ forward_fill('R.PRICE_ADJ_CLOSE', 'S.ASSET_ID', 'S.PRICE_DATE_ID') }}
                                                            AS PRICE_ADJ_CLOSE
    FROM spine_x_pairs S
    LEFT JOIN base_prices R
        ON R.PRICE_DATE_ID = S.PRICE_DATE_ID
        AND R.ASSET_ID     = S.ASSET_ID
)

SELECT *
FROM filled
WHERE PRICE_ADJ_CLOSE IS NOT NULL  -- exclude dates before first ever price was loaded
