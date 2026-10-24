WITH raw_trades AS 
(
    SELECT 
        TYPE AS TRANSACTION_TYPE_RAW
    ,   CAST(TIME AS DATE) AS TRANSACTION_DATE
    ,   COMMENT AS TRANSACTION_COMMENT
    ,   SPLIT_PART(SYMBOL,'.',1) AS ASSET_CODE
    ,   CAST(AMOUNT AS DECIMAL(10,2)) AS NET_AMOUNT_TRADED_IN_EUR
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   {{dbt.current_timestamp()}} AS LOAD_TS
    FROM {{source('raw','raw_transactions_xtb')}}
    
    WHERE ID != 'Total'
    AND TRANSACTION_TYPE_RAW IN ('Stock sale', 'Stock purchase')
)

, add_quantity AS
(
    SELECT 
        TRANSACTION_DATE
    ,   TRANSACTION_TYPE_RAW
    ,   ASSET_CODE
    ,   CAST(CASE
                WHEN POSITION('/' IN rt.TRANSACTION_COMMENT) > 0
                    THEN SPLIT_PART(SPLIT_PART(rt.TRANSACTION_COMMENT, '/', 1), ' ', -1)
                ELSE SPLIT_PART(SPLIT_PART(rt.TRANSACTION_COMMENT, '@', 1), ' ', 3) 
        END AS DECIMAL(10,2)) AS SHARES_TRADED   
    ,   CAST(SPLIT_PART(TRANSACTION_COMMENT,'@',2) AS NUMBER(10,2))  AS PRICE
    ,   TRANSACTION_COMMENT
    ,   NET_AMOUNT_TRADED_IN_EUR
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   LOAD_TS
    FROM raw_trades RT
)

, invert_signal AS (

    SELECT 
        TRANSACTION_DATE
    ,   TRANSACTION_TYPE_RAW
    ,   ASSET_CODE
    ,   CASE 
            WHEN TRANSACTION_TYPE_RAW = 'Stock sale' THEN -SHARES_TRADED
            ELSE SHARES_TRADED
        END AS SHARES_TRADED
    ,   PRICE
    ,   TRANSACTION_COMMENT
    ,   -NET_AMOUNT_TRADED_IN_EUR AS NET_AMOUNT_TRADED_IN_EUR
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   LOAD_TS
FROM add_quantity
)


select * from invert_signal
  