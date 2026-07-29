{{
    config(
        materialized='incremental',
        unique_key='TRANSACTION_ID',
        incremental_strategy='merge'
    )
}}

WITH STG_XTB AS (
    SELECT * FROM {{ ref('stg_transactions_xtb') }}
    {% if is_incremental() %}
        WHERE {{ incremental_load_filter('LOAD_TS') }}
    {% endif %}
),

TRANSFORM_XTB AS (
    SELECT
        TRAN.TRANSACTION_ID::VARCHAR                                          AS TRANSACTION_ID,
        CAST(REPLACE(CAST(TRAN.TRANSACTION_TIME AS DATE), '-', '') AS INT)   AS TRANSACTION_DATE_ID,
        TRTY.TRANSACTION_TYPE_ID,
        TRTY.TRANSACTION_TYPE,
        ASSE.ASSET_ID,
        CURR.CURRENCY_ID,
        CASE
            WHEN TRTY.TRANSACTION_TYPE IN ('BUY', 'SELL') THEN
                CAST(
                    CASE
                        WHEN POSITION('/' IN TRAN.TRANSACTION_COMMENT) > 0
                            THEN SPLIT_PART(SPLIT_PART(TRAN.TRANSACTION_COMMENT, '/', 1), ' ', -1)
                        ELSE SPLIT_PART(SPLIT_PART(TRAN.TRANSACTION_COMMENT, '@', 1), ' ', 3)
                    END
                AS DECIMAL(10, 4))
        END                                                                   AS QUANTITY,
        CASE
            WHEN TRTY.TRANSACTION_TYPE IN ('BUY', 'SELL')
                THEN CAST(TRIM(SPLIT_PART(TRAN.TRANSACTION_COMMENT, '@', 2)) AS DECIMAL(10, 2))
        END                                                                   AS PRICE,
        TRAN.AMOUNT,
        TRAN.TRANSACTION_COMMENT                                              AS COMMENT,
        TRAN.SOURCE_FILE,
        TRAN.SOURCE_SYSTEM,
        TRAN.LOAD_TS,
        {{ dbt_updated_at() }}                                                AS DBT_UPDATED_AT
    FROM STG_XTB TRAN
    LEFT JOIN {{ ref('dim_asset') }} ASSE
        ON ASSE.ASSET_CODE = TRAN.ASSET_CODE
    LEFT JOIN {{ ref('dim_transaction_type') }} TRTY
        ON TRTY.TRANSACTION_TYPE = TRAN.TRANSACTION_TYPE
    LEFT JOIN {{ ref('dim_currency') }} CURR
        ON CURR.CURRENCY_ABRV = 'EUR'
),

UNIONED  AS (
    SELECT
        TRANSACTION_ID,
        TRANSACTION_DATE_ID,
        TRANSACTION_TYPE_ID,
        ASSET_ID,
        CURRENCY_ID,
        CASE
            WHEN TRANSACTION_TYPE = 'SELL' THEN QUANTITY * -1
            ELSE QUANTITY
        END AS QUANTITY,
        PRICE,
        -- BUY/SELL are flipped from raw so amount mirrors quantity's build/unwind
        -- polarity (BUY=+/+, SELL=-/-); every other type keeps its raw sign, which
        -- already matches the desired ledger sign (income +, tax/fee -, deposits +).
        CASE
            WHEN TRANSACTION_TYPE IN ('BUY', 'SELL') THEN AMOUNT * -1
            ELSE AMOUNT
        END AS AMOUNT,
        COMMENT,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS,
        DBT_UPDATED_AT
    FROM TRANSFORM_XTB
)

SELECT * FROM UNIONED
