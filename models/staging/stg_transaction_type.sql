SELECT 
    transaction_type,
    affects_quantity,
    affects_cash,
    external_cash_flag,
    'raw_transaction_type_seed.csv' AS source_file,
    CURRENT_TIMESTAMP() AS LOAD_TS
FROM {{ source('raw','raw_transaction_type_seed') }}

