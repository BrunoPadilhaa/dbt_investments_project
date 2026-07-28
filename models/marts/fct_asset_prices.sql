{{
    config(
        materialized = 'incremental',
        unique_key=['ASSET_ID','PRICE_DATE_ID'],
        incremental_strategy = 'merge'
    )

}}

WITH t_asset_prices AS ( 
    SELECT
        ASSET.ASSET_ID
    ,   TO_NUMBER(TO_VARCHAR(ASPR.PRICE_DATE,'YYYYMMDD')) AS PRICE_DATE_ID
    ,   ASPR.PRICE_OPEN
    ,   ASPR.PRICE_HIGH
    ,   ASPR.PRICE_LOW
    ,   ASPR.PRICE_CLOSE
    ,   ASPR.PRICE_ADJ_CLOSE
    ,   ASPR.PRICE_VOLUME
    ,   CURR.CURRENCY_ID AS PRICE_CURRENCY_ID
    ,   ASPR.SOURCE_SYSTEM
    ,   ASPR.LOAD_TS
    ,   {{ dbt_updated_at() }} AS DBT_UPDATED_AT
    FROM {{ref('stg_asset_prices')}} ASPR

    LEFT
    JOIN {{ref('dim_asset')}} ASSET
        ON ASSET.ASSET_CODE = ASPR.ASSET_CODE

    LEFT JOIN {{ ref('dim_currency') }} curr
        ON CURR.CURRENCY_ABRV = ASPR.CURRENCY

    {% if is_incremental() %}

        WHERE {{ incremental_load_filter('ASPR.LOAD_TS') }}

    {% endif %}
)

SELECT * FROM t_asset_prices


