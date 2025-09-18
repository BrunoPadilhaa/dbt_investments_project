{{
    config(
        materialized = 'incremental',
        unique_key = 'TRADE_ID',
        on_schema_change = 'fail'
    )
}}

WITH trades AS (
    SELECT
        tran.TRADE_ID,
        CAST(REPLACE(CAST(tran.TRADE_TIME AS DATE),'-','') AS INT) AS TRADE_DATE_KEY,
        trty.transaction_type_id AS TRANSACTION_TYPE_ID,
        stin.stock_id AS STOCK_ID,
        stin.currency_id AS CURRENCY_ID,
        CAST(
            CASE
                WHEN POSITION('/' IN tran.TRADE_COMMENT) > 0
                    THEN SPLIT_PART(SPLIT_PART(tran.TRADE_COMMENT,'/',1),' ',-1)
                ELSE SPLIT_PART(SPLIT_PART(tran.TRADE_COMMENT,'@',1),' ',3)
            END AS DECIMAL(10,4)
        ) AS TRADE_QUANTITY,
        CAST(TRIM(SPLIT_PART(tran.TRADE_COMMENT,'@',2)) AS DECIMAL(10,2)) AS TRADE_PRICE,
        tran.AMOUNT * -1 AS TRADE_AMOUNT,
        tran.TRADE_COMMENT AS TRADE_COMMENT,
        tran.SOURCE_FILE AS SOURCE_FILE,
        tran.SOURCE_SYSTEM AS SOURCE_SYSTEM,
        tran.LOAD_TS AS LOAD_TS,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM {{ref('stg_trades_pt')}} tran
    LEFT JOIN {{ref('dim_stock_info')}} stin
        ON stin.original_symbol = tran.symbol
    LEFT JOIN {{ref('dim_transaction_type')}} trty
        ON trty.transaction_type_name = tran.trade_type
    WHERE tran.TRADE_TYPE IN ('Stock purchase','Stock sale')
    {% if is_incremental() %}
        AND tran.LOAD_TS > (SELECT MAX(LOAD_TS) FROM {{this}})
    {% endif %}
)

SELECT * FROM trades
