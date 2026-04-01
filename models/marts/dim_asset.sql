{{
    config(
        materialized='incremental',
        unique_key='asset_id',
        incremental_strategy='merge'
    )
}}

WITH CTE_ASSET AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['ASSET_CODE'])}} AS ASSET_ID,
        ASSET_CODE,
        ASSET_CODE_CURRENT,
        ASSET_NAME,
        ASSET_COUNTRY,
        INVESTMENT_COUNTRY,
        SHORTNAME,
        QUOTETYPE,
        SECTOR,
        ASSET_TYPE,
        INDUSTRY,
        EXCHANGE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM {{ ref('stg_asset') }}
)


SELECT * FROM CTE_ASSET