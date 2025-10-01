{{
    config(
            materialized = 'incremental',
            unique_key = ['TRANSACTION_TYPE_ID'],
            incremental_strategy = 'merge'
        )
}}

WITH distinct_types AS (
    SELECT
        DISTINCT TRADE_TYPE,
        SOURCE_SYSTEM,
        MIN(LOAD_TS) AS LOAD_TS
    FROM {{ ref('stg_trades_pt') }}  -- Just change to dbt source
    WHERE TRADE_TYPE IS NOT NULL                   -- Still good to filter nulls
    GROUP BY
        TRADE_TYPE,
        SOURCE_SYSTEM
),
final_types AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY TRADE_TYPE) AS TRANSACTION_TYPE_ID,
        TRADE_TYPE AS TRANSACTION_TYPE_NAME,
        SOURCE_SYSTEM,
        LOAD_TS,
        CURRENT_TIMESTAMP() AS DBT_UPDATED_AT
    FROM distinct_types
)

SELECT * FROM final_types
