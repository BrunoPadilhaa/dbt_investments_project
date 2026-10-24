WITH cte_asset_mapping AS 
(
    SELECT
        UPPER(TRIM(ASSET_CODE))         AS ASSET_CODE
    ,   TRIM(ASSET_NAME)                AS ASSET_NAME
    ,   UPPER(TRIM(ASSET_CLASS))        AS ASSET_CLASS
    ,   UPPER(TRIM(ASSET_CODE_SYSTEM))  AS ASSET_CODE_SYSTEM
    ,   'raw_asset_mapping.csv'         AS SOURCE_SYSTEM
    ,   CURRENT_TIMESTAMP()             AS LOAD_TS
    FROM {{ source('raw','raw_asset_mapping') }}
)

SELECT * FROM cte_asset_mapping