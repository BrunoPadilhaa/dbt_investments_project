{{ config(
    materialized='view'
) }}

WITH dates AS (
    SELECT 
        LAST_DAY(date) AS month_end, MAX(date_key) date_key
    FROM {{ref('dim_date')}}
    WHERE date_key <= TO_NUMBER(TO_CHAR(CURRENT_DATE(),'YYYYMMDD')) 
    AND YEAR = 2025
    
    GROUP BY 1
    )

, all_tickers AS (

SELECT DISTINCT
     ticker_id
    ,date_key
FROM {{ref('dim_ticker')}} t

CROSS JOIN dates d
)

, trades AS (
    SELECT
        t.ticker_id,
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(TO_VARCHAR(t.transaction_date_key), 'YYYYMMDD')), 'YYYYMMDD')) AS transaction_date_key,
        SUM(t.quantity) AS quantity,
        SUM(t.amount) AS amount
    FROM {{ref('fct_trades')}} t
    GROUP BY
        t.ticker_id,
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(TO_VARCHAR(t.transaction_date_key), 'YYYYMMDD')), 'YYYYMMDD'))

)

, last_day_stock_price AS (

    --Gives the last stock price for each month. Then I standardize as last day of the month.
    SELECT 
        ticker_id
    ,   TO_DATE(price_date_key::STRING, 'YYYYMMDD') AS price_date
    ,    TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(price_date_key::STRING, 'YYYYMMDD')), 'YYYYMMDD')) AS last_day_id
    ,   price_adj_close
    FROM  {{ref('fct_stock_prices')}}
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY ticker_id, last_day_id ORDER BY price_date DESC) = 1

)

, f_result AS (

    SELECT 
        alti.date_key
    ,   tic.ticker
    ,   fct.amount
    ,   fct.quantity
    ,   COALESCE(SUM(fct.amount) OVER (PARTITION BY TICKER ORDER BY DATE_KEY),0) AS AMOUNT_CUMMULATED
    ,   COALESCE(SUM(fct.quantity) OVER (PARTITION BY TICKER ORDER BY DATE_KEY),0) AS QUANTITY_CUMMULATED
    FROM all_tickers alti
    
    LEFT JOIN {{ref('dim_ticker')}} tic
    ON tic.ticker_id = alti.ticker_id
    
    LEFT JOIN trades fct
    ON fct.ticker_id = alti.ticker_id
    AND alti.date_key = fct.transaction_date_key
)


SELECT * FROM f_result 
WHERE 1=1
AND QUANTITY_CUMMULATED > 0
