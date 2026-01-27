{{
    config(
            materialized = 'incremental',
            unique_key = 'transaction_type_id',
            incremental_strategy = 'merge'
        )
}}

WITH distinct_types AS (
    SELECT
        DISTINCT TRANSACTION_TYPE,
        SOURCE_SYSTEM,
        MIN(LOAD_TS) AS LOAD_TS
    FROM {{ ref('stg_transactions_xtb') }} 
    WHERE TRANSACTION_TYPE IS NOT NULL                   
    GROUP BY
        TRANSACTION_TYPE,
        SOURCE_SYSTEM
),
final_types AS (
    SELECT
        {{dbt_utils.generate_surrogate_key(['TRANSACTION_TYPE'])}} AS TRANSACTION_TYPE_ID,
        TRANSACTION_TYPE,
        CASE 
            WHEN TRANSACTION_TYPE IN ('Divident','Tax IFTT','Withholding Tax','Sec Fee','Free-Funds Interest Tax',
'Free-Funds Interest') THEN 'Divident' 
            ELSE TRANSACTION_TYPE 
        END AS TRANSACTION_TYPE_GROUP,
        LOAD_TS,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM distinct_types
)

SELECT * FROM final_types
