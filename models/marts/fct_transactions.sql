{{
    config(
        materialized = 'incremental',
        unique_key = 'TRANSACTION_ID',
        incremental_strategy = 'merge'
    )
}}

WITH transform_trades AS (
    SELECT
        tran.TRANSACTION_ID,
        CAST(REPLACE(CAST(tran.TRANSACTION_TIME AS DATE),'-','') AS INT) AS TRANSACTION_DATE_ID,
        trty.transaction_type_id,
        asse.asset_id   ,
        CASE
            WHEN asse.INVESTMENT_COUNTRY = 'BRAZIL' THEN 'BRL'
            ELSE 'EUR'
        END AS CURRENCY_ABRV,
        CASE
            WHEN LOWER(tran.transaction_type) IN ('stock purchase', 'stock sale') THEN 
            CAST(
                CASE
                    WHEN POSITION('/' IN tran.TRANSACTION_COMMENT) > 0
                        THEN SPLIT_PART(SPLIT_PART(tran.TRANSACTION_COMMENT,'/',1),' ',-1)
                    ELSE SPLIT_PART(SPLIT_PART(tran.TRANSACTION_COMMENT,'@',1),' ',3)
                END AS DECIMAL(10,4)
            ) END AS QUANTITY,
        CASE
            WHEN LOWER(tran.transaction_type) IN ('stock purchase', 'stock sale') THEN CAST(TRIM(SPLIT_PART(tran.TRANSACTION_COMMENT,'@',2)) AS DECIMAL(10,2)) END AS PRICE,
        tran.AMOUNT AS AMOUNT,
        tran.TRANSACTION_COMMENT AS COMMENT,
        tran.SOURCE_FILE AS SOURCE_FILE,
        tran.SOURCE_SYSTEM AS SOURCE_SYSTEM,
        tran.LOAD_TS AS LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ ref('stg_transactions_xtb') }} tran
    LEFT JOIN {{ ref('dim_asset') }} asse
        ON asse.asset_code = tran.asset_code
    LEFT JOIN {{ ref('dim_transaction_type') }} trty
        ON trty.transaction_type = tran.transaction_type

     {% if is_incremental() %}
        WHERE tran.LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}


)

, f_result AS (
    SELECT
        TRTR.TRANSACTION_ID, 
        TRTR.TRANSACTION_DATE_ID, 
        TRTR.TRANSACTION_TYPE_ID, 
        TRTR.ASSET_ID, 
        CURR.CURRENCY_ID, 
        CASE 
            WHEN TRTR.TRANSACTION_TYPE_ID = 'Stock Sale' THEN TRTR.QUANTITY * -1 --sold 
            ELSE TRTR.QUANTITY
        END AS QUANTITY, 
        TRTR.PRICE, 
        CASE 
            WHEN TRTR.TRANSACTION_TYPE_ID = 'Stock Sale' THEN TRTR.AMOUNT * -1 --sold 
            ELSE TRTR.AMOUNT
        END AS AMOUNT, 
        TRTR.COMMENT, 
        TRTR.SOURCE_FILE, 
        TRTR.SOURCE_SYSTEM, 
        TRTR.LOAD_TS, 
        TRTR.DBT_UPDATED_AT 
    FROM transform_trades trtr

    LEFT JOIN {{ ref('dim_currency') }} curr
    ON curr.currency_abrv = trtr.currency_abrv  
)

SELECT * FROM f_result