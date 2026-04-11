{{
    config(
        materialized='incremental',
        unique_key='TRANSACTION_ID',
        incremental_strategy='merge'
    )
}}

WITH TRANSACTIONS_CLEAR AS (

    SELECT
        TRIM(TRCL."Entrada/Saída")                                              AS CASH_FLOW_DIRECTION,
        COALESCE(
            TRY_TO_TIMESTAMP(TRCL."Data"),
            TRY_TO_DATE(TRCL."Data", 'DD/MM/YYYY')
        )                                                                        AS TRANSACTION_DATE,
        TRCL."Movimentação"                                                      AS TRANSACTION_TYPE_RAW,
        MTRTY.TRANSACTION_TYPE,
        TRIM(SPLIT_PART(TRCL."Produto", ' - ', 1))                              AS ASSET_CODE,
        TRIM(SPLIT_PART(TRCL."Produto", ' - ', 2))                              AS ASSET_CODE_NAME,
        TRIM(TRCL."Instituição")                                                 AS BROKER,
        CAST(TRCL."Quantidade" AS DECIMAL(10, 2))                               AS QUANTITY,
        CAST(NULLIF(TRCL."Preço unitário", '-') AS DECIMAL(10, 2))              AS UNIT_PRICE,
        CAST(NULLIF(TRCL."Valor da Operação", '-') AS DECIMAL(10, 2))           AS VALUE,
        'BRL'                                                                    AS CURRENCY,
        TRCL.SOURCE_FILE,
        TRCL.SOURCE_SYSTEM,
        TRCL.LOAD_TS
    FROM {{ source('raw', 'raw_transactions_clear') }} TRCL
    LEFT JOIN {{ source('raw', 'map_transaction_type_source_seed') }} MTRTY
        ON UPPER(TRCL."Movimentação") = UPPER(MTRTY.SOURCE_TRANSACTION_TYPE)

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
            COALESCE(SOURCE_SYSTEM, ''),            '|',
            COALESCE(TRANSACTION_DATE::VARCHAR, ''), '|',
            COALESCE(CASH_FLOW_DIRECTION, ''),      '|',
            COALESCE(ASSET_CODE, ''),               '|',
            COALESCE(TRANSACTION_TYPE_RAW, ''),     '|',
            COALESCE(QUANTITY::VARCHAR, ''),        '|',
            COALESCE(VALUE::VARCHAR, '')
        )
    ) AS TRANSACTION_ID,
    TRANSACTION_TYPE_RAW,
    TRANSACTION_DATE,
    TRANSACTION_TYPE,
    ASSET_CODE,
    ASSET_CODE_NAME,
    BROKER,
    QUANTITY,
    UNIT_PRICE,
    VALUE,
    CURRENCY,
    SOURCE_FILE,
    SOURCE_SYSTEM,
    LOAD_TS
FROM TRANSACTIONS_CLEAR