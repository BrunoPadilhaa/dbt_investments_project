{{
    config(
        materialized='incremental'
    ,   unique_key='TRANSACTION_ID'
    ,   on_schema_change='fail'
    )
}}

WITH cte_raw_trades AS 
(
    SELECT 
    CAST(ID AS INT) AS TRANSACTION_ID
    ,   CASE
            WHEN INITCAP(TYPE) = 'Tax Iftt' THEN 'Tax IFTT'
            ELSE INITCAP(TYPE) 
        END AS TRANSACTION_TYPE
    ,   CAST(TIME AS TIMESTAMP) AS TRANSACTION_TIME
    ,   COMMENT AS TRANSACTION_COMMENT
    ,   SYMBOL AS TICKER
    ,   'EUR' AS CURRENCY
    ,   CAST(AMOUNT AS NUMBER(10,2)) AS AMOUNT
    ,   SOURCE_FILE
    ,   SOURCE_SYSTEM
    ,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
    FROM {{source('raw','raw_transactions_xtb')}}
    WHERE ID != 'Total'

    {% if is_incremental() %}
    -- Only run this WHERE clause on incremental runs
    AND LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
)

SELECT * FROM cte_raw_trades



