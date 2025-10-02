SELECT
    SYMBOL
FROM {{ref('stg_trades_pt')}}
WHERE TRADE_TYPE IN ('Stock purchase','Stock sale')
EXCEPT
SELECT
    TICKER
FROM {{ref('stg_ticker')}}
