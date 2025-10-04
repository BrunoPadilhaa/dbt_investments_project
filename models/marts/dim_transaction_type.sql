{{
    config(
            materialized = 'incremental',
            unique_key = 'TRANSACTION_TYPE_ID',
            incremental_strategy = 'merge'
        )
}}

WITH distinct_types AS (
    SELECT
        DISTINCT TRANSACTION_TYPE,
        SOURCE_SYSTEM,
        MIN(LOAD_TS) AS LOAD_TS
    FROM {{ ref('stg_transactions_pt') }} 
    WHERE TRANSACTION_TYPE IS NOT NULL                   
    GROUP BY
        TRANSACTION_TYPE,
        SOURCE_SYSTEM
),
final_types AS (
    SELECT
        ABS(HASH(TRANSACTION_TYPE,SOURCE_SYSTEM)) AS TRANSACTION_TYPE_ID,
        TRANSACTION_TYPE,
        SOURCE_SYSTEM,
        LOAD_TS,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM distinct_types

    {% if is_incremental() %}

        WHERE LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS),'1900-01-01'::TIMESTAMP_NTZ) FROM {{this}})

    {% endif %}
)

SELECT * FROM final_types
