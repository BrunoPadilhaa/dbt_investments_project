{{ config(
    materialized = 'table'
) }}

SELECT * FROM {{ ref('int_asset_prices_filled') }}
