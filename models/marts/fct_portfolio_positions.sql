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
    FROM {{ref('fct_transactions')}} t
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
    alti.date_id,
    tic.ticker_id,
    fct.quantity,
    -- Current position value in EUR
    fct.amount AS amount_invested,
    ROUND(
        COALESCE(fct.quantity, 0) * COALESCE(stc.eom_price, 0) * COALESCE(exr.eom_exchange_rate, 1),
        2
    ) AS portfolio_value,
    -- Running totals partitioned by ticker
    SUM(COALESCE(fct.quantity, 0)) 
        OVER (PARTITION BY tic.ticker_id ORDER BY alti.date_id) AS quantity_cumulative,
    SUM(COALESCE(fct.amount, 0)) 
        OVER (PARTITION BY tic.ticker_id ORDER BY alti.date_id) AS amount_invested_cumulative,
    -- Cumulative position value in EUR
    ROUND(
        SUM(COALESCE(fct.quantity, 0)) 
            OVER (PARTITION BY tic.ticker_id ORDER BY alti.date_id) 
        * COALESCE(stc.eom_price, 0) 
        * COALESCE(exr.eom_exchange_rate, 1),
        2
    ) AS portfolio_value_cumulative

FROM all_tickers alti

LEFT JOIN {{ref('dim_ticker')}} tic
    ON tic.ticker_id = alti.ticker_id

LEFT JOIN trades fct
    ON fct.ticker_id = alti.ticker_id
    AND fct.transaction_date_id = alti.date_id

LEFT JOIN last_day_stock_price stc
    ON stc.ticker_id = alti.ticker_id
    AND stc.last_day_id = alti.date_id

LEFT JOIN last_day_exchange_rate exr
    ON exr.last_day_id = alti.date_id
    AND exr.currency_id = tic.currency_id
)

, current_position AS (
        SELECT 
            ticker_id
        ,   quantity_cumulative
        FROM f_result 
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker_id ORDER BY date_id DESC) = 1
        )



SELECT 
    * 
FROM f_result 
WHERE ticker_id NOT IN (SELECT ticker_id FROM current_position WHERE quantity_cumulative = 0 )
