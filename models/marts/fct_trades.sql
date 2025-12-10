{{ 
    config( 
        materialized='incremental', 
        incremental_strategy='merge', 
        unique_key=['TRANSACTION_ID', 'SOURCE_SYSTEM'] ) 
}}

WITH transform_trades AS (
    SELECT
        ABS(HASH(tran.TRANSACTION_ID, tran.SOURCE_SYSTEM)) AS TRADE_ID,
        tran.TRANSACTION_ID,
        CAST(REPLACE(CAST(tran.TRANSACTION_TIME AS DATE),'-','') AS INT) AS TRANSACTION_DATE_ID,
        trty.transaction_type_id AS TRANSACTION_TYPE_ID,
        stin.ticker_id,
        stin.currency_id AS CURRENCY_ID,
        CAST(
            CASE
                WHEN POSITION('/' IN tran.TRANSACTION_COMMENT) > 0
                    THEN SPLIT_PART(SPLIT_PART(tran.TRANSACTION_COMMENT,'/',1),' ',-1)
                ELSE SPLIT_PART(SPLIT_PART(tran.TRANSACTION_COMMENT,'@',1),' ',3)
            END AS DECIMAL(10,4)
        ) AS QUANTITY,
        CAST(TRIM(SPLIT_PART(tran.TRANSACTION_COMMENT,'@',2)) AS DECIMAL(10,2)) AS PRICE,
        tran.AMOUNT * -1 AS AMOUNT,
        tran.TRANSACTION_COMMENT AS COMMENT,
        tran.SOURCE_FILE AS SOURCE_FILE,
        tran.SOURCE_SYSTEM AS SOURCE_SYSTEM,
        tran.LOAD_TS AS LOAD_TS,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS DBT_UPDATED_AT
    FROM {{ref('stg_transactions_pt')}} tran
    LEFT JOIN {{ref('dim_ticker')}} stin
        ON stin.original_ticker = tran.ticker
    LEFT JOIN {{ref('dim_transaction_type')}} trty
        ON trty.transaction_type = tran.transaction_type
    WHERE tran.transaction_type IN ('Stock Purchase','Stock Sale')
)

, f_result AS (
    SELECT
        TRADE_ID, 
        TRANSACTION_ID, 
        TRANSACTION_DATE_ID, 
        TRANSACTION_TYPE_ID, 
        TICKER_ID, 
        CURRENCY_ID, 
        CASE 
            WHEN TRANSACTION_TYPE_ID = 8900044673474300801 THEN QUANTITY * -1 
            ELSE QUANTITY
        END AS QUANTITY, 
        PRICE, 
        AMOUNT, 
        COMMENT, 
        SOURCE_FILE, 
        SOURCE_SYSTEM, 
        LOAD_TS, 
        DBT_UPDATED_AT 
    FROM transform_trades
)

SELECT * FROM f_result 