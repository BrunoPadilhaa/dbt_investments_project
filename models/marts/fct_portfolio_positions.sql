WITH dates AS (
    SELECT 
        TO_NUMBER(TO_CHAR(LAST_DAY(DATE),'YYYYMMDD')) AS date_id --ALWAYS GET THE LAST DAY OF THE MONTH
    FROM {{ref('dim_date')}}
    WHERE date_key <= TO_NUMBER(TO_CHAR(CURRENT_DATE(),'YYYYMMDD')) 
    AND YEAR >= 2025
    QUALIFY ROW_NUMBER() OVER (PARTITION BY MONTH_NAME ORDER BY DATE_KEY DESC) =1

    )

, all_tickers AS (

SELECT DISTINCT
     ticker_id
    ,date_id
FROM {{ref('dim_ticker')}} t

CROSS JOIN dates d
)

, trades AS (
    SELECT
        t.ticker_id,
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(TO_VARCHAR(t.TRANSACTION_DATE_ID), 'YYYYMMDD')), 'YYYYMMDD')) AS TRANSACTION_DATE_ID,
        SUM(t.quantity) AS quantity,
        SUM(t.amount) AS amount
    FROM {{ref('fct_trades')}} t
    GROUP BY
        t.ticker_id,
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(TO_VARCHAR(t.TRANSACTION_DATE_ID), 'YYYYMMDD')), 'YYYYMMDD'))

)

, last_day_stock_price AS (

    //Gives the last stock price for each month. Then I standardize as last day of the month.
    SELECT 
        stc.ticker_id
    ,   tic.currency_id
    ,   TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(stc.price_date_id::STRING, 'YYYYMMDD')), 'YYYYMMDD')) AS last_day_id
    ,   stc.price_adj_close
    FROM {{ref('fct_stock_prices')}} stc

    LEFT JOIN prod.dim_ticker tic
    ON tic.ticker_id = stc.ticker_id
    
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY stc.ticker_id, last_day_id ORDER BY stc.price_date_id) = 1

)
, last_day_exchange_rate AS (

    //Gives the last stock price for each month. Then I standardize as last day of the month.
    SELECT 
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(rate_date_id::STRING, 'YYYYMMDD')), 'YYYYMMDD')) AS last_day_id --LAST DAY OF THE MONTH
    ,   currency_id_from AS currency_id
    ,   exchange_rate
    FROM {{ref('fct_exchange_rates')}}
    WHERE currency_id_to = 7571457297138968724 -- EUR
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY currency_id_from, last_day_id ORDER BY rate_date_id DESC) = 1
    )

, stock_price_in_eur AS (

    SELECT
        stc.last_day_id
    ,   tic.ticker_id
    ,   cur.currency_name
    ,   CAST(stc.price_adj_close * COALESCE(exr.exchange_rate,1) AS DECIMAL(10,2)) AS CurrentPriceEUR
    FROM last_day_stock_price stc

    LEFT JOIN last_day_exchange_rate exr
    ON stc.currency_id = exr.currency_id
    AND stc.last_day_id = exr.last_day_id

    LEFT JOIN prod.dim_ticker tic
    ON tic.ticker_id = stc.ticker_id

    LEFT JOIN prod.dim_currency cur
    ON cur.currency_id = tic.currency_id
)

, f_result AS (

    SELECT 
        alti.date_id
    ,   tic.ticker
    ,   fct.quantity
    ,   fct.amount
    ,   COALESCE(SUM(fct.quantity) OVER (PARTITION BY TICKER ORDER BY date_id),0) AS QUANTITY_CUMMULATED
    ,   COALESCE(SUM(fct.amount) OVER (PARTITION BY TICKER ORDER BY date_id),0) AS AMOUNT_CUMMULATED
    ,   CAST(COALESCE(SUM(fct.quantity) OVER (PARTITION BY TICKER ORDER BY date_id),0) * CurrentPriceEUR AS DECIMAL(10,2)) AS CurrentPrice
    FROM all_tickers alti
    
    LEFT JOIN {{ref('dim_ticker')}} tic
    ON tic.ticker_id = alti.ticker_id
    
    LEFT JOIN trades fct
    ON fct.ticker_id = alti.ticker_id
    AND alti.date_id = fct.TRANSACTION_DATE_ID

    LEFT JOIN stock_price_in_eur stc
    ON alti.ticker_id = stc.ticker_id
    AND alti.date_id = stc.last_day_id
)


SELECT * FROM f_result 
WHERE 1=1
AND QUANTITY_CUMMULATED > 0
