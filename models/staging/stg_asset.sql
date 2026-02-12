{{
    config(
        materialized = 'incremental',
        unique_key = 'ASSET_CODE',
        incremental_strategy = 'merge'
    )
}}

WITH cte_asset AS 
(
SELECT
    UPPER(TRIM(ASSET_CODE)) AS ASSET_CODE
,   UPPER(TRIM(ASSET_CODE_CURRENT)) AS ASSET_CODE_CURRENT
,   UPPER(TRIM(ASSET_NAME)) AS ASSET_NAME
,   UPPER(TRIM(ASSET_TYPE)) AS ASSET_TYPE
,   UPPER(TRIM(CODE_SUFFIX)) AS CODE_SUFFIX
,   TRIM(SOURCE_SYSTEM) AS SOURCE_SYSTEM
,   TO_TIMESTAMP(LOAD_TS) AS LOAD_TS
FROM {{source('raw','raw_asset')}}
)

SELECT * FROM cte_asset