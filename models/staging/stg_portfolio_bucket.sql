
{{
    config(
        materialized='incremental',
        unique_key='bucket_type',
        incremental_strategy='merge'
    )
}}

SELECT 
    DISTINCT
TRIM(UPPER(BUCKET_TYPE)) AS BUCKET_TYPE
FROM {{source('raw', 'raw_ticker_seed')}} 
WHERE BUCKET_TYPE IS NOT NULL