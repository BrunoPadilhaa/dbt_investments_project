

WITH transform_trades AS (
    SELECT
        tran.TRANSACTION_ID,
        CAST(REPLACE(CAST(tran.TRANSACTION_TIME AS DATE),'-','') AS INT) AS TRANSACTION_DATE_ID,
        trty.transaction_type_id,
        stin.ticker_id,
        curr.currency_abrv,
        curr.currency_id,
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
    LEFT JOIN {{ ref('dim_ticker') }} stin
        ON stin.original_ticker = tran.ticker
    LEFT JOIN {{ ref('dim_transaction_type') }} trty
        ON trty.transaction_type = tran.transaction_type
    LEFT JOIN {{ ref('dim_currency') }} curr
        ON curr.currency_abrv = tran.currency  
)

, f_result AS (
    SELECT
        TRANSACTION_ID, 
        TRANSACTION_DATE_ID, 
        TRANSACTION_TYPE_ID, 
        TICKER_ID, 
        CURRENCY_ID, 
        CASE 
            WHEN TRANSACTION_TYPE_ID = 'f284e5640a390198e05c593e09286d01' THEN QUANTITY * -1 --sold 
            ELSE QUANTITY
        END AS QUANTITY, 
        PRICE, 
        CASE 
            WHEN TRANSACTION_TYPE_ID = 'f284e5640a390198e05c593e09286d01' THEN AMOUNT * -1 --sold 
            ELSE AMOUNT
        END AS AMOUNT, 
        COMMENT, 
        SOURCE_FILE, 
        SOURCE_SYSTEM, 
        LOAD_TS, 
        DBT_UPDATED_AT 
    FROM transform_trades
)

SELECT * FROM f_result