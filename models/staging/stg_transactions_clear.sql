WITH transactions_clear AS (
    SELECT
        TRIM(TRCL."Entrada/Saída") AS CASH_FLOW_DIRECTION, 
        COALESCE(
                TRY_TO_TIMESTAMP(TRCL."Data"),
                TRY_TO_DATE(TRCL."Data", 'DD/MM/YYYY')
                )
        AS TRANSACTION_DATE,
        TRCL."Movimentação" AS TRANSACTION_TYPE_RAW, 
        MTRTY.TRANSACTION_TYPE,
        TRIM(SPLIT_PART(TRCL."Produto",' - ',1)) AS ASSET_CODE, 
        TRIM(SPLIT_PART(TRCL."Produto",' - ',2)) AS ASSET_CODE_NAME, 
        TRIM(TRCL."Instituição") AS BROKER, 
        CAST(TRCL."Quantidade" AS DECIMAL(10,2)) AS QUANTITY, 
        CAST(NULLIF(TRCL."Preço unitário",'-') AS DECIMAL(10,2)) AS UNIT_PRICE, 
        CAST(NULLIF(TRCL."Valor da Operação",'-') AS DECIMAL(10,2)) AS VALUE,
        TRCL.SOURCE_FILE, 
        TRCL.SOURCE_SYSTEM, 
        TRCL.LOAD_TS
    FROM {{ source('raw','raw_transactions_clear') }} TRCL

    LEFT JOIN {{ source('raw','map_transaction_type_source_seed') }} MTRTY
    ON UPPER(TRCL."Movimentação") = UPPER(MTRTY.SOURCE_TRANSACTION_TYPE)
    )

SELECT  MD5(
            CONCAT( 
                    SOURCE_SYSTEM, '|',
                    TRANSACTION_DATE, '|',
                    CASH_FLOW_DIRECTION, '|',
                    ASSET_CODE, '|',
                    TRANSACTION_TYPE_RAW, '|',
                    QUANTITY, '|',
                    VALUE
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
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
     FROM TRANSACTIONS_CLEAR