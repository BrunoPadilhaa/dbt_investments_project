{{
    config(
            materialized = 'incremental',
            unique_key = 'TRANSACTION_TYPE',
            incremental_strategy = 'merge'
        )
}}

WITH transaction_types AS (
SELECT 
    TRANSACTION_TYPE, 
    AFFECTS_QUANTITY, 
    AFFECTS_CASH, 
    EXTERNAL_CASH_FLAG, 
    SOURCE_FILE, 
    LOAD_TS 
FROM {{ ref('stg_transaction_type')}}
)

SELECT
    {{dbt_utils.generate_surrogate_key(['TRANSACTION_TYPE'])}} AS transaction_type_id,
    TRANSACTION_TYPE,
    AFFECTS_QUANTITY,
    AFFECTS_CASH,
    EXTERNAL_CASH_FLAG,
    SOURCE_FILE,
    LOAD_TS
FROM transaction_types
