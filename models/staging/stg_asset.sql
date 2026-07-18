{{
    config(
        materialized = 'incremental',
        unique_key = 'ASSET_CODE',
        incremental_strategy = 'merge'
    )
}}


WITH assets AS (
    SELECT
        DISTINCT SPLIT_PART(SYMBOL,'.',1) AS ASSET_CODE
    FROM {{ source('raw','raw_transactions_xtb') }}
    WHERE SYMBOL IS NOT NULL
)
, cte_asset_seed AS 
(
    SELECT
        UPPER(TRIM(ASSET_CODE))         AS ASSET_CODE
    ,   UPPER(TRIM(ASSET_CODE_CURRENT)) AS ASSET_CODE_CURRENT
    ,   TRIM(ASSET_NAME)                AS ASSET_NAME
    ,   UPPER(TRIM(ASSET_TYPE))         AS ASSET_TYPE
    ,   UPPER(TRIM(ASSET_CODE_SYSTEM))  AS ASSET_CODE_SYSTEM
    ,   'raw_asset_seed.csv'            AS SOURCE_SYSTEM
    ,   CURRENT_TIMESTAMP()             AS LOAD_TS
    FROM {{ source('raw', 'raw_asset_seed') }}
)

,   cte_asset_details  AS 
(
    SELECT
        UPPER(TRIM(ASSET_CODE))                         AS ASSET_CODE
    ,   UPPER(TRIM(ASSET_CODE_SYSTEM))                  AS ASSET_CODE_SYSTEM
    ,   TRIM(COUNTRY)                                   AS ASSET_COUNTRY
    ,   REGEXP_REPLACE(TRIM(SHORTNAME), '\\s+', ' ')    AS SHORTNAME
    ,   TRIM(QUOTETYPE)                                 AS QUOTETYPE
    ,   TRIM(SECTOR)                                    AS SECTOR
    ,   TRIM(INDUSTRY)                                  AS INDUSTRY
    ,   TRIM(CURRENCY)                                  AS CURRENCY
    ,   TRIM(EXCHANGE)                                  AS EXCHANGE
    ,   TRIM(SOURCE_SYSTEM)                             AS SOURCE_SYSTEM
    ,   TO_TIMESTAMP(LOAD_TS)                           AS LOAD_TS
    FROM {{ source('raw', 'raw_asset_details') }}
)

SELECT
    ASSE.ASSET_CODE
,   ASSD.ASSET_CODE_CURRENT
,   ASSD.ASSET_NAME
,   CASE 
        WHEN ASSET_DTL.EXCHANGE = 'LSE' THEN 'England'
        WHEN ASSET_DTL.EXCHANGE = 'GER' THEN 'Germany'
        WHEN ASSET_DTL.EXCHANGE = 'AMS' THEN 'Netherlands' 
        ELSE ASSET_DTL.ASSET_COUNTRY
    END AS ASSET_COUNTRY
,   'Portugal' AS INVESTMENT_COUNTRY
,   ASSET_DTL.SHORTNAME
,   ASSET_DTL.QUOTETYPE
,   ASSET_DTL.SECTOR
,   ASSD.ASSET_TYPE
,   ASSET_DTL.INDUSTRY
,   ASSET_DTL.EXCHANGE
,   ASSD.SOURCE_SYSTEM
,   ASSD.LOAD_TS

FROM assets asse

LEFT
JOIN cte_asset_seed assd
ON asse.ASSET_CODE = assd.ASSET_CODE

LEFT JOIN cte_asset_details ASSET_DTL
    ON  ASSE.ASSET_CODE        = ASSET_DTL.ASSET_CODE
    AND ASSD.ASSET_CODE_SYSTEM = ASSET_DTL.ASSET_CODE_SYSTEM