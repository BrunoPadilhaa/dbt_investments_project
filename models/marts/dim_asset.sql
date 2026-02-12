{{
    config(
        materialized='incremental',
        unique_key='asset_id',
        incremental_strategy='merge'
    )
}}

WITH CTE_ASSET AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['ASSE.ASSET_CODE'])}} AS ASSET_ID,
        ASSE.ASSET_CODE,
        ASSE.ASSET_CODE_CURRENT,
        ASSE.ASSET_NAME,
        ASDE.ASSET_COUNTRY,
        CASE 
            WHEN ASDE.ASSET_COUNTRY = 'Brazil' THEN 'Brazil'
            ELSE 'Portugal'
        END AS INVESTMENT_COUNTRY,
        ASDE.YAHOO_FINANCE_ASSET_CODE,
        ASDE.SHORTNAME,
        ASDE.QUOTETYPE,
        ASDE.SECTOR,
        ASSE.ASSET_TYPE,
        ASDE.INDUSTRY,
        ASDE.EXCHANGE,
        ASSE.SOURCE_SYSTEM,
        ASSE.LOAD_TS
    FROM {{ ref('stg_asset') }} ASSE
    
    LEFT JOIN {{ ref('stg_asset_details') }} ASDE
        ON ASSE.ASSET_CODE = ASDE.ASSET_CODE
)

SELECT * FROM CTE_ASSET