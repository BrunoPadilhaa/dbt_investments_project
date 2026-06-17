{{
    config(
        materialized='incremental',
        unique_key='TRANSACTION_ID',
        incremental_strategy='merge'
    )
}}



WITH STG_XTB AS (

    SELECT * FROM {{ref('stg_transactions_xtb')}}
    
        WHERE LOAD_TS > (
            SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ)
            FROM {{this}}
        )
    

),

STG_CLEAR AS (

    SELECT * FROM {{ref('stg_transactions_clear')}}
    
        WHERE LOAD_TS > (
            SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ)
            FROM {{this}}
        )
    

),

TRANSFORM_XTB AS (
    SELECT
        TRAN.TRANSACTION_ID::VARCHAR                                          AS TRANSACTION_ID,
        CAST(REPLACE(CAST(TRAN.TRANSACTION_TIME AS DATE), '-', '') AS INT)   AS TRANSACTION_DATE_ID,
        TRTY.TRANSACTION_TYPE_ID,
        TRTY.TRANSACTION_TYPE,
        ASSE.ASSET_ID,
        CURR.CURRENCY_ID,
        CASE
            WHEN TRTY.TRANSACTION_TYPE IN ('BUY', 'SELL') THEN
                CAST(
                    CASE
                        WHEN POSITION('/' IN TRAN.TRANSACTION_COMMENT) > 0
                            THEN SPLIT_PART(SPLIT_PART(TRAN.TRANSACTION_COMMENT, '/', 1), ' ', -1)
                        ELSE SPLIT_PART(SPLIT_PART(TRAN.TRANSACTION_COMMENT, '@', 1), ' ', 3)
                    END
                AS DECIMAL(10, 4))
        END                                                                   AS QUANTITY,
        CASE
            WHEN TRTY.TRANSACTION_TYPE IN ('BUY', 'SELL')
                THEN CAST(TRIM(SPLIT_PART(TRAN.TRANSACTION_COMMENT, '@', 2)) AS DECIMAL(10, 2))
        END                                                                   AS PRICE,
        TRAN.AMOUNT,
        TRAN.TRANSACTION_COMMENT                                              AS COMMENT,
        TRAN.SOURCE_FILE,
        TRAN.SOURCE_SYSTEM,
        TRAN.LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                                    AS DBT_UPDATED_AT
    FROM STG_XTB TRAN
    LEFT JOIN {{ref('dim_asset')}} ASSE
        ON ASSE.ASSET_CODE = TRAN.ASSET_CODE
    LEFT JOIN {{ref('dim_transaction_type')}} TRTY
        ON TRTY.TRANSACTION_TYPE = TRAN.TRANSACTION_TYPE
    LEFT JOIN {{ref('dim_currency')}} CURR
        ON CURR.CURRENCY_ABRV = 'EUR'
),

TRANSFORM_CLEAR AS (
    SELECT
        TRCL.TRANSACTION_ID::VARCHAR                                          AS TRANSACTION_ID,
        TO_NUMBER(TO_VARCHAR(TRCL.TRANSACTION_DATE, 'YYYYMMDD'))              AS TRANSACTION_DATE_ID,
        TRTY.TRANSACTION_TYPE_ID,
        TRTY.TRANSACTION_TYPE,
        ASSE.ASSET_ID,
        CURR.CURRENCY_ID,
        TRCL.QUANTITY                                                         AS QUANTITY,
        TRCL.UNIT_PRICE                                                       AS PRICE,
        TRCL.AMOUNT,                                                           
        TRCL.TRANSACTION_TYPE_RAW                                             AS COMMENT,
        TRCL.SOURCE_FILE,
        TRCL.SOURCE_SYSTEM,
        TRCL.LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ                                    AS DBT_UPDATED_AT
    FROM STG_CLEAR TRCL
    LEFT JOIN {{ref('dim_transaction_type')}} TRTY
        ON TRTY.TRANSACTION_TYPE = TRCL.TRANSACTION_TYPE
    LEFT JOIN {{ref('dim_asset')}} ASSE
        ON ASSE.ASSET_CODE = TRCL.ASSET_CODE
    LEFT JOIN {{ref('dim_currency')}} CURR
        ON CURR.CURRENCY_ABRV = TRCL.CURRENCY
),

UNIONED AS (
    SELECT
        TRANSACTION_ID,
        TRANSACTION_DATE_ID,
        TRANSACTION_TYPE_ID,
        ASSET_ID,
        CURRENCY_ID,
        CASE
            WHEN TRANSACTION_TYPE = 'SELL' THEN QUANTITY * -1
            ELSE QUANTITY
        END AS QUANTITY,
        PRICE,
        AMOUNT * -1 AS  AMOUNT, --Sell amounts should be negative and buy positive. 
        COMMENT,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS,
        DBT_UPDATED_AT
    FROM TRANSFORM_XTB

    UNION ALL

    SELECT
        TRANSACTION_ID,
        TRANSACTION_DATE_ID,
        TRANSACTION_TYPE_ID,
        ASSET_ID,
        CURRENCY_ID,
        QUANTITY,
        PRICE,
        AMOUNT,
        COMMENT,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS,
        DBT_UPDATED_AT
    FROM TRANSFORM_CLEAR
)

SELECT * FROM UNIONED

