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
    CAST(TRXT.ID AS INT) AS TRANSACTION_ID
    ,   TRXT.TYPE AS TRANSACTION_TYPE_RAW
    ,   MTRTY.TRANSACTION_TYPE
    ,   CAST(TRXT.TIME AS TIMESTAMP) AS TRANSACTION_TIME
    ,   TRXT.COMMENT AS TRANSACTION_COMMENT
    ,   SPLIT_PART(TRXT.SYMBOL,'.',1) AS ASSET_CODE
    ,   'EUR' AS CURRENCY
    ,   CAST(TRXT.AMOUNT AS NUMBER(10,2)) AS AMOUNT
    ,   TRXT.SOURCE_FILE
    ,   TRXT.SOURCE_SYSTEM
    ,   CAST(TRXT.LOAD_TS AS TIMESTAMP) AS LOAD_TS
    FROM {{source('raw','raw_transactions_xtb')}} TRXT

    LEFT JOIN {{ source('raw','map_transaction_type_source_seed') }} MTRTY
    ON UPPER(TRXT.TYPE) = UPPER(MTRTY.SOURCE_TRANSACTION_TYPE)
    
    WHERE ID != 'Total'

    {% if is_incremental() %}
    AND LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
    {% endif %}
)

SELECT * FROM cte_raw_trades



