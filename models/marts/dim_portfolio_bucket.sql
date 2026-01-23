{{ config(
    materialized='incremental',
    unique_key='BUCKET_TYPE',
    incremental_strategy='merge'

) }}

SELECT
    {{dbt_utils.generate_surrogate_key(['BUCKET_TYPE'])}} AS BUCKET_ID,
    BUCKET_TYPE
FROM {{ ref('stg_portfolio_bucket') }} 