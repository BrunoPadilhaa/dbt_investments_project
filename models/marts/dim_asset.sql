{{
    config(
        materialized='incremental',
        unique_key='asset_id',
        incremental_strategy='merge'
    )
}}

WITH CTE_ASSET AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['ORIGINAL_ASSET_CODE'])}} AS ASSET_ID,
        CURRENT_ASSET_CODE,
        ORIGINAL_ASSET_CODE,
        ASSET_NAME,
        ASSET_COUNTRY,
        INVESTMENT_COUNTRY,
        SOURCE_ASSET_SHORTNAME,
        SOURCE_QUOTE_TYPE,
        SECTOR,
        ASSET_CLASS,
        INDUSTRY,
        EXCHANGE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM {{ ref('int_assets') }}
)


SELECT * FROM CTE_ASSET