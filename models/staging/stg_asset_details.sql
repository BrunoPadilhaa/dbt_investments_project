WITH cte_asset_details  AS 
(
    SELECT
        UPPER(TRIM(ASSET_CODE))                         AS ASSET_CODE
    ,   UPPER(TRIM(ASSET_CODE_SYSTEM))                  AS ASSET_CODE_SYSTEM
    ,   TRIM(COUNTRY)                                   AS ASSET_COUNTRY
    ,   REGEXP_REPLACE(TRIM(SHORTNAME), '\\s+', ' ')    AS SOURCE_ASSET_SHORTNAME
    ,   TRIM(longname)                                  AS ASSET_NAME
    ,   TRIM(QUOTETYPE)                                 AS SOURCE_QUOTE_TYPE
    ,   TRIM(SECTOR)                                    AS SECTOR
    ,   TRIM(INDUSTRY)                                  AS INDUSTRY
    ,   TRIM(CURRENCY)                                  AS CURRENCY
    ,   TRIM(EXCHANGE)                                  AS EXCHANGE
    ,   TRIM(SOURCE_SYSTEM)                             AS SOURCE_SYSTEM
    ,   TO_TIMESTAMP(LOAD_TS)                           AS LOAD_TS
    FROM {{ source('raw','raw_asset_details') }}
)

SELECT
    ASSET_CODE
,   ASSET_CODE_SYSTEM
,   CASE 
        WHEN EXCHANGE = 'LSE' THEN 'England'
        WHEN EXCHANGE = 'GER' THEN 'Germany'
        WHEN EXCHANGE = 'AMS' THEN 'Netherlands' 
        ELSE ASSET_COUNTRY
    END AS ASSET_COUNTRY
,   SOURCE_ASSET_SHORTNAME
,   ASSET_NAME
,   SOURCE_QUOTE_TYPE
,   SECTOR
,   INDUSTRY
,   CURRENCY
,   EXCHANGE
,   SOURCE_SYSTEM
,   LOAD_TS
FROM cte_asset_details