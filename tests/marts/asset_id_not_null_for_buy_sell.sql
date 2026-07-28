-- Test that BUY/SELL transactions always have an asset_id (other transaction
-- types, like TAX or FEE, legitimately have no associated asset)
SELECT
    FCT.TRANSACTION_ID
FROM {{ ref('fct_transactions') }} FCT
INNER JOIN {{ ref('dim_transaction_type') }} TRTY
    ON TRTY.TRANSACTION_TYPE_ID = FCT.TRANSACTION_TYPE_ID
WHERE TRTY.TRANSACTION_TYPE IN ('BUY', 'SELL')
  AND FCT.ASSET_ID IS NULL
