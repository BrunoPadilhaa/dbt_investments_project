{{
    config(
        materialized='incremental',
        unique_key='TRANSACTION_ID',
        incremental_strategy='merge'
    )
}}

WITH RAW_TRANSACTIONS_CLEAR AS (
    SELECT * FROM {{ source('raw', 'raw_transactions_clear') }}

    UNION ALL

    SELECT
        CASH_FLOW_DIRECTION,
        TRANSACTION_DATE,
        TRANSACTION_TYPE_RAW,
        PRODUTO,
        INSTITUTION,
        QUANTITY,
        UNIT_PRICE,
        AMOUNT,
        'corporate_action_adjustments_seed.csv.' AS SOURCE_FILE,
        'manual_seed' AS SOURCE_SYSTEM,
        CURRENT_TIMESTAMP() AS LOAD_TS
    FROM {{ source('raw', 'corporate_action_adjustments_seed') }} --MANUAL SEED FOR CORPORATE ACTION ADJUSTMENTS, SINCE CLEAR DOES NOT PROVIDE THESE ADJUSTMENTS IN THE EXTRACTED FILES
)

,  TRANSACTIONS_CLEAR AS (

    SELECT
        TRIM(TRCL."Entrada/Saída")                                              AS CASH_FLOW_DIRECTION,
        COALESCE(
            TRY_TO_TIMESTAMP(TRCL."Data"),
            TRY_TO_DATE(TRCL."Data", 'DD/MM/YYYY')
        )                                                                        AS TRANSACTION_DATE,
        TRCL."Movimentação"                                                      AS TRANSACTION_TYPE_RAW,
        MTRTY.SOURCE_SIDE,
        MTRTY.IS_DEBIT,
        MTRTY.TRANSACTION_TYPE                                                  AS TRANSACTION_TYPE,
        TRIM(SPLIT_PART(TRCL."Produto", ' - ', 1))                              AS ASSET_CODE,
        TRIM(SPLIT_PART(TRCL."Produto", ' - ', 2))                              AS ASSET_CODE_NAME,
        TRIM(TRCL."Instituição")                                                 AS BROKER,
        CAST(TRCL."Quantidade" AS DECIMAL(10, 2))                               AS QUANTITY,
        CAST(NULLIF(TRCL."Preço unitário", '-') AS DECIMAL(10, 2))              AS UNIT_PRICE,
        CAST(NULLIF(TRCL."Valor da Operação", '-') AS DECIMAL(10, 2))           AS AMOUNT,
        'BRL'                                                                    AS CURRENCY,
        TRCL.SOURCE_FILE,
        TRCL.SOURCE_SYSTEM,
        TRCL.LOAD_TS
    FROM RAW_TRANSACTIONS_CLEAR TRCL
    LEFT JOIN {{ source('raw', 'map_transaction_type_source_seed') }} MTRTY
        ON UPPER(TRCL."Movimentação") = UPPER(MTRTY.SOURCE_TRANSACTION_TYPE)
        AND (
            MTRTY.SOURCE_SIDE IS NULL
            OR UPPER(TRCL."Entrada/Saída") = UPPER(MTRTY.SOURCE_SIDE)
        )

    {% if is_incremental() %}
        WHERE TRCL.LOAD_TS > (
            SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ)
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    MD5(
        CONCAT(
            COALESCE(SOURCE_SYSTEM, ''),             '|',
            COALESCE(TRANSACTION_DATE::VARCHAR, ''),  '|',
            COALESCE(CASH_FLOW_DIRECTION, ''),        '|',
            COALESCE(ASSET_CODE, ''),                 '|',
            COALESCE(TRANSACTION_TYPE_RAW, ''),       '|',
            COALESCE(QUANTITY::VARCHAR, ''),           '|',
            COALESCE(AMOUNT::VARCHAR, '')
        )
    )                                                                            AS TRANSACTION_ID,
    TRANSACTION_TYPE_RAW,
    TRANSACTION_DATE,
    TRANSACTION_TYPE,
    ASSET_CODE,
    ASSET_CODE_NAME,
    BROKER,
    CASE WHEN IS_DEBIT THEN QUANTITY * -1 ELSE QUANTITY END                      AS QUANTITY,
    UNIT_PRICE,
    CASE WHEN IS_DEBIT THEN AMOUNT * -1   ELSE AMOUNT   END                      AS AMOUNT,
    CURRENCY,
    SOURCE_FILE,
    SOURCE_SYSTEM,
    LOAD_TS
FROM TRANSACTIONS_CLEAR