WITH dates AS (
    SELECT 
        LAST_DAY(date) AS month_end, MAX(date_key) date_key
    FROM dim_date
    WHERE date_key <= TO_NUMBER(TO_CHAR(CURRENT_DATE(),'YYYYMMDD')) 
    AND YEAR = 2025
    
    GROUP BY 1
    )

, all_tickers AS (

SELECT DISTINCT
     ticker_id
    ,date_key
FROM dim_ticker t

CROSS JOIN dates d
)

, trades AS (
    SELECT
        t.ticker_id,
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(TO_VARCHAR(t.transaction_date_key), 'YYYYMMDD')), 'YYYYMMDD')) AS transaction_date_key,
        SUM(t.quantity) AS quantity,
        SUM(t.amount) AS amount
    FROM prod.fct_trades t
    GROUP BY
        t.ticker_id,
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(TO_VARCHAR(t.transaction_date_key), 'YYYYMMDD')), 'YYYYMMDD'))

)

, stock_prices AS (

    SELECT 
        *
    FROM fct_stock_prices

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
    
    LEFT JOIN dim_ticker tic
    ON tic.ticker_id = alti.ticker_id
    
    LEFT JOIN trades fct
    ON fct.ticker_id = alti.ticker_id
    AND alti.date_key = fct.transaction_date_key
)


SELECT * FROM f_result 
WHERE 1=1
AND QUANTITY_CUMMULATED > 0
