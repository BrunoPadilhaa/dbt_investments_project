SELECT
    TICKER_ID
FROM {{ref('fct_trades')}}
WHERE TRANSACTION_TYPE_ID IN ('11441cc5a2e90d0a3462f4d5036d3c59','f284e5640a390198e05c593e09286d01') --Stock Purchase and Stock Sale
EXCEPT
SELECT
    TICKER_ID
FROM {{ref('dim_ticker')}}