WITH cte_raw_trades AS 
(
    SELECT 
    --    MTRTY.TRANSACTION_TYPE
        TRANSACTION_DATE
    --,   TRXT.TRANSACTION_COMMENT
    ,   ASSET_CODE
    ,   SHARES_TRADED
    ,   PRICE
    ,   NET_AMOUNT_TRADED_IN_EUR
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   {{ dbt.current_timestamp() }} AS LOAD_TS
    FROM {{ref('stg_transactions_xtb')}}

)

SELECT * FROM cte_raw_trades



