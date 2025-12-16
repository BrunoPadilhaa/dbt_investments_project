{{config(
    materialized='view',
    schema='prod'
)}}

WITH dates AS (
    SELECT 
        TO_NUMBER(TO_CHAR(LAST_DAY(DATE),'YYYYMMDD')) AS date_id --ALWAYS GET THE LAST DAY OF THE MONTH
    FROM {{ref('dim_date')}}
    WHERE date_id <= TO_NUMBER(TO_CHAR(CURRENT_DATE(),'YYYYMMDD')) 
    AND YEAR >= 2025
    QUALIFY ROW_NUMBER() OVER (PARTITION BY MONTH_NAME ORDER BY date_id DESC) =1

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

    --Gives the last stock price for each month. Then I standardize as last day of the month.
    SELECT 
        stp.ticker_id
    ,   tic.currency_id
    ,   TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(stp.price_date_id::STRING, 'YYYYMMDD')), 'YYYYMMDD')) AS last_day_id
    ,   stp.price_adj_close AS eom_price
    FROM {{ref('fct_stock_prices')}} stp

    LEFT JOIN prod.dim_ticker tic
    ON tic.ticker_id = stp.ticker_id
    
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY stp.ticker_id, last_day_id ORDER BY stp.price_date_id) = 1

)

, last_day_exchange_rate AS (

    --Gives the last stock price for each month. Then I standardize as last day of the month.
    SELECT 
        TO_NUMBER(TO_CHAR(LAST_DAY(TO_DATE(rate_date_id::STRING, 'YYYYMMDD')), 'YYYYMMDD')) AS last_day_id --LAST DAY OF THE MONTH
    ,   currency_id_from AS currency_id
    ,   COALESCE(exchange_rate,1) AS eom_exchange_rate
    FROM {{ref('fct_exchange_rates')}}
    WHERE currency_id_to = 'a055562bdb59ad8ba9cc680367308118' -- EUR
    QUALIFY  ROW_NUMBER() OVER (PARTITION BY currency_id_from, last_day_id ORDER BY rate_date_id DESC) = 1
    )

, f_result AS (

    SELECT 
        alti.date_id
    ,   tic.ticker_id
    ,   fct.quantity
    ,   fct.amount AS amount_invested
    ,   COALESCE(SUM(fct.quantity) OVER (PARTITION BY TICKER ORDER BY date_id),0) AS quantity_cummulated
    ,   COALESCE(SUM(fct.amount) OVER (PARTITION BY TICKER ORDER BY date_id),0) AS amount_invested_cummulated
    --  Multiply the current price times the quantity I have, after that, multiply by the euro exchange rate.
    ,   (CAST(COALESCE(SUM(fct.quantity) OVER (PARTITION BY TICKER ORDER BY date_id),0) * eom_price AS DECIMAL(10,2))) * eom_exchange_rate AS portfolio_value 
    FROM all_tickers alti
    
    LEFT JOIN {{ref('dim_ticker')}} tic
    ON tic.ticker_id = alti.ticker_id
    
    LEFT JOIN trades fct
    ON fct.ticker_id = alti.ticker_id
    AND alti.date_id = fct.TRANSACTION_DATE_ID

    LEFT JOIN last_day_stock_price stc
    ON alti.ticker_id = stc.ticker_id
    AND alti.date_id = stc.last_day_id

    LEFT JOIN last_day_exchange_rate exr
    ON alti.date_id = exr.last_day_id
    AND tic.currency_id = exr.currency_id
)

, current_position AS (
        SELECT 
            ticker_id
        ,   quantity_cummulated
        FROM f_result 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker_id ORDER BY date_id DESC) = 1
        )



SELECT 
    * 
FROM f_result 
WHERE ticker_id NOT IN (SELECT ticker_id FROM current_position WHERE quantity_cummulated = 0 )
