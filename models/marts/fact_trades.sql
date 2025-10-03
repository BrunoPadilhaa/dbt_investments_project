{{
    config(
        materialized = 'incremental',
        unique_key = 'TRADE_ID',
        on_schema_change = 'fail'
    )
}}

WITH trades AS (
    SELECT
        trad.TRADE_ID,
        CAST(REPLACE(CAST(trad.TRADE_TIME AS DATE),'-','') AS INT) AS TRADE_DATE_KEY,
        trty.transaction_type_id AS TRANSACTION_TYPE_ID,
        stin.ticker_id,
        stin.currency_id AS CURRENCY_ID,
        CAST(
            CASE
                WHEN POSITION('/' IN trad.TRADE_COMMENT) > 0
                    THEN SPLIT_PART(SPLIT_PART(trad.TRADE_COMMENT,'/',1),' ',-1)
                ELSE SPLIT_PART(SPLIT_PART(trad.TRADE_COMMENT,'@',1),' ',3)
            END AS DECIMAL(10,4)
        ) AS TRADE_QUANTITY,
        CAST(TRIM(SPLIT_PART(trad.TRADE_COMMENT,'@',2)) AS DECIMAL(10,2)) AS TRADE_PRICE,
        trad.AMOUNT * -1 AS TRADE_AMOUNT,
        trad.TRADE_COMMENT AS TRADE_COMMENT,
        trad.SOURCE_FILE AS SOURCE_FILE,
        trad.SOURCE_SYSTEM AS SOURCE_SYSTEM,
        trad.LOAD_TS AS LOAD_TS,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM {{ref('stg_trades_pt')}} trad
    LEFT JOIN {{ref('dim_ticker')}} stin
        ON stin.original_ticker = trad.ticker
    LEFT JOIN {{ref('dim_transaction_type')}} trty
        ON trty.transaction_type_name = trad.trade_type
    WHERE trad.TRADE_TYPE IN ('Stock Purchase','Stock Sale')
    {% if is_incremental() %}
        AND trad.LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS),'1900-01-01'::TIMESTAMP_NTZ) FROM {{this}})
    {% endif %}
)

SELECT * FROM trades
