
  
    

create or replace transient table INVESTMENTS.STAGING.stg_raw_trades_pt
    
    
    
    as (WITH cte_raw_trades AS 
(
    SELECT 
        CAST(ID AS INT) AS TRADE_ID
    ,   TRADE_TYPE
    ,   CAST(TRADE_TIME AS DATETIME) AS TRADE_TIME
    ,   TRADE_COMMENT
    ,   SYMBOL
    ,   CAST(AMOUNT AS FLOAT) AS AMOUNT
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   CAST(LOAD_TS AS DATETIME) AS LOAD_TS
    FROM INVESTMENTS.RAW.RAW_TRADES_PT
    WHERE ID != 'Total'
)

SELECT * FROM cte_raw_trades
    )
;


  