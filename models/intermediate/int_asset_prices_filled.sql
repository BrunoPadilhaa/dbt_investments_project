{{ config(
    materialized = 'table'
) }}

WITH dates_spine AS (
    SELECT DATE_ID AS PRICE_DATE_ID
    FROM {{ ref("dim_date") }}
),

asset_currency_pairs AS (
    SELECT DISTINCT
        ASSET_ID,
        PRICE_CURRENCY_ID
    FROM {{ ref('fct_asset_prices') }}
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
    LEFT JOIN {{ ref('fct_asset_prices') }} R
        ON R.PRICE_DATE_ID = S.PRICE_DATE_ID
        AND R.ASSET_ID     = S.ASSET_ID
)

SELECT *
FROM filled
WHERE PRICE_ADJ_CLOSE IS NOT NULL  -- exclude dates before first ever price was loaded
