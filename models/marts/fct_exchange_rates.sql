{{ config(
    materialized = 'table'
) }}

SELECT * FROM {{ ref('int_exchange_rates_filled') }}
