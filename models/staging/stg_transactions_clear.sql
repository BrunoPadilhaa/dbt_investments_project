WITH transactions_clear AS (
    SELECT
        TRIM("Entrada/Saída") AS TRANSACTION_TYPE, 
        COALESCE(
                TRY_TO_TIMESTAMP("Data"),
                TRY_TO_DATE("Data", 'DD/MM/YYYY')
                )
        AS TRANSACTION_DATE,
        "Movimentação" AS TRANSACTION_TYPE_DETAIL, 
        TRIM(SPLIT_PART("Produto",' - ',1)) AS ASSET_CODE, 
        TRIM(SPLIT_PART("Produto",' - ',2)) AS ASSET_CODE_NAME, 
        TRIM("Instituição") AS BROKER, 
        CAST("Quantidade" AS DECIMAL(10,2)) AS QUANTITY, 
        CAST(NULLIF("Preço unitário",'-') AS DECIMAL(10,2)) AS UNIT_PRICE, 
        CAST(NULLIF("Valor da Operação",'-') AS DECIMAL(10,2)) AS VALUE,
        SOURCE_FILE, 
        SOURCE_SYSTEM, 
        LOAD_TS
    FROM {{ source('raw','raw_transactions_clear') }}
    )

SELECT  MD5(
            CONCAT(
                    source_system, '|',
                    transaction_date, '|',
                    asset_code, '|',
                    transaction_type_detail, '|',
                    quantity, '|',
                    value
                    )
                  ) AS TRANSACTION_ID,
        TRANSACTION_TYPE,
        TRANSACTION_DATE,
        TRANSACTION_TYPE_DETAIL,
        ASSET_CODE,
        ASSET_CODE_NAME,
        BROKER,
        QUANTITY,
        UNIT_PRICE,
        VALUE,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
     FROM transactions_clear