-- Test that no trades occurred after they were loaded into the system
SELECT *
FROM {{ ref('stg_trades_pt') }}
WHERE TRADE_TIME > LOAD_TS