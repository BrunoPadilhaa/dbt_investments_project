
//Select only open positions
WITH open_positions AS (
    SELECT
        ASSE.ASSET_ID
    ,   ASSE.ASSET_CODE_CURRENT
    ,   SUM(TRAN.QUANTITY)
    FROM PROD.FCT_TRANSACTIONS TRAN
    
    LEFT
    JOIN PROD.DIM_ASSET ASSE
    ON ASSE.ASSET_ID = TRAN.ASSET_ID

    LEFT 
    JOIN PROD.DIM_TRANSACTION_TYPE TRTY
    ON TRTY.TRANSACTION_TYPE_ID = TRAN.TRANSACTION_TYPE_ID
    
    WHERE TRTY.TRANSACTION_TYPE IN ('BUY','SELL','CORPORATE_ACTION')
    
    GROUP BY ALL
    
    HAVING SUM(TRAN.QUANTITY) > 0
)

//Selects dates from 2028 to D-1
, calendar AS (
    SELECT
        date_id
    FROM INVESTMENTS.PROD.dim_date
    WHERE DATE_ID BETWEEN 20180101 AND TO_NUMBER(TO_CHAR(current_date, 'YYYYMMDD'))-1
)

----------------------------- BASE TABLES ---------------------------------------------

--

-- ASSET PRICES
, asset_price_currency_asset AS (
    SELECT DISTINCT 
        price_date_id
    ,   asset_id
    ,   price_currency_id
    ,   price_adj_close
    ,   LAST_VALUE(price_adj_close IGNORE NULLS) OVER (
            PARTITION BY price_date_id, asset_id, price_currency_id
            ORDER BY price_date_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS price_adj_close_filled
    FROM INVESTMENTS.PROD.fct_asset_prices

--TRADES
SELECT 
    transaction_date_id
,   asset_id
,   quantity
,   amount_brl
,   amount_eur
FROM prod.fct_transactions;

    
)

------------------------------------- END ------------------------------------------------

----------------------------- CREATE FORWARD FILLS  ----------------------------------------------
// Forward fill asset prices and exchange rates when we have no data for it 
//(e.g. holidays and weekds)

, asset_date_spine AS (
    SELECT 
        cale.date_id
    ,   oppo.asset_id
    ,   oppo.asset_code_current
    ,   aspr.price_adj_close   
    ,   INTERPOLATE_FFILL(price_adj_close) OVER (PARTITION BY oppo.asset_id ORDER BY cale.date_id) ffil
    FROM calendar cale
    CROSS JOIN open_positions oppo

    LEFT 
    JOIN  asset_price_currency_asset aspr
    ON aspr.price_date_id = cale.date_id
    AND oppo.asset_id = aspr.asset_id
    
)

SELECT * FROM asset_date_spine where asset_code_current IN ('4GLD','COST');

------------------------------------- END ------------------------------------------------


-- Get all distinct currency pairs that exist in our exchange rate data
, currency_pairs AS (
    SELECT DISTINCT
        currency_id_from, 
        currency_id_to
    FROM INVESTMENTS.PROD.fct_exchange_rates
)

-- Create spine: every currency pair for every date
-- Allows forward-filling of exchange rates across non-trading days
-- , exchange_rates_spine AS (
--     SELECT
--         cale.date_id
--     ,   cupa.currency_id_from
--     ,   cupa.currency_id_to
--     FROM calendar cale
--     CROSS JOIN currency_pairs cupa
-- )

-- Forward-fill exchange rates to cover weekends and holidays
-- Uses LAST_VALUE with IGNORE NULLS to carry forward the most recent known rate
-- Special handling: EUR to EUR is always 1.0
, exchange_rates AS (
    SELECT
        ersp.date_id AS rate_date_id
    ,   ersp.currency_id_from
    ,   ersp.currency_id_to
    ,   exra.exchange_rate
    ,   LAST_VALUE(exra.exchange_rate IGNORE NULLS) OVER (
            PARTITION BY ersp.currency_id_from, ersp.currency_id_to
            ORDER BY ersp.date_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS exchange_rate_filled
    FROM exchange_rates_spine ersp
    LEFT JOIN (
        -- Set EUR to EUR conversion as 1.0, use actual rates for other currencies
        SELECT 
            exra.rate_date_id
        ,   exra.currency_id_from
        ,   exra.currency_id_to
        ,   CASE 
                WHEN curr.currency_abrv = 'EUR' THEN 1 
                ELSE exra.exchange_rate
            END AS exchange_rate
        FROM INVESTMENTS.PROD.fct_exchange_rates exra
        LEFT JOIN INVESTMENTS.PROD.dim_currency curr
            ON curr.currency_id = exra.currency_id_from
    ) exra
        ON exra.rate_date_id = ersp.date_id
        AND exra.currency_id_from = ersp.currency_id_from
        AND exra.currency_id_to = ersp.currency_id_to
)

-- Create spine: every asset for every date with its price currency
,   AS (
    SELECT
        cale.date_id
    ,   spct.asset_id
    ,   spct.price_currency_id
    FROM calendar cale
    CROSS JOIN asset_price_currency_asset spct
)

-- Forward-fill asset prices to cover weekends and holidays
-- Uses most recent closing price when markets are closed
, asset_prices_filled AS (
    SELECT 
        spsp.date_id AS price_date_id
    ,   spsp.asset_id
    ,   spsp.price_currency_id
    ,   stpr.price_adj_close
    ,   LAST_VALUE(stpr.price_adj_close IGNORE NULLS) OVER (
            PARTITION BY spsp.asset_id 
            ORDER BY spsp.date_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS price_adj_close_filled
    FROM asset_prices_spine spsp
    LEFT JOIN INVESTMENTS.PROD.fct_asset_prices stpr
        ON stpr.price_date_id = spsp.date_id
        AND stpr.asset_id = spsp.asset_id
)

-- Enrich asset prices with asset and currency metadata
-- Price remains in native currency at this stage (USD, BRL, EUR, etc.)
, asset_prices AS (
    SELECT 
        stpr.price_date_id
    ,   asse.asset_id
    ,   stpr.price_currency_id
    ,   curr.currency_abrv
    ,   asse.asset_code
    ,   stpr.price_adj_close_filled
    FROM asset_prices_filled stpr
    LEFT JOIN INVESTMENTS.PROD.dim_asset asse
        ON asse.asset_id = stpr.asset_id
    LEFT JOIN INVESTMENTS.PROD.dim_currency curr
        ON curr.currency_id = stpr.price_currency_id
)

-- Normalize all buy/sell transactions
-- Buys = positive quantity, Sells = negative quantity
-- Amount is inverted (negative = cash outflow for purchases)
, trades AS (
    SELECT 
        tran.transaction_date_id
    ,   asse.asset_id
    ,   asse.asset_code     
    ,   trty.transaction_type
    ,   SUM(quantity) AS quantity
    ,   SUM(tran.amount) AS amount  
    FROM INVESTMENTS.PROD.fct_transactions tran
    LEFT JOIN INVESTMENTS.PROD.dim_asset asse
        ON asse.asset_id = tran.asset_id
    LEFT JOIN INVESTMENTS.PROD.dim_transaction_type trty
        ON trty.transaction_type_id = tran.transaction_type_id
    WHERE lower(trty.transaction_type) IN ('buy', 'sell')

    GROUP BY ALL
)

-- Filter to only assets with current holdings (net position > 0)
-- Excludes assets that have been completely sold out
, opened_positions AS (
    SELECT 
        asset_id
    ,   SUM(quantity) as total_quantity
    FROM trades
    GROUP BY asset_id
    HAVING SUM(quantity) > 0
)

-- Final snapshot: combine all elements to create daily position and valuation records
, daily_snapshot AS (
    SELECT 
        tida.date_id
    ,   tida.asset_id
    ,   trad.quantity  -- Shares traded today (NULL if no trade)
    ,   trad.amount    -- Money invested/divested today (NULL if no trade)
    ,   stpr.price_currency_id
    
        -- Running total of shares owned from inception to this date
    ,   SUM(COALESCE(trad.quantity, 0)) OVER (
            PARTITION BY tida.asset_id 
            ORDER BY tida.date_id
        ) AS quantity_cumulative
    
        -- Running total of capital deployed from inception to this date
    ,   SUM(COALESCE(trad.amount, 0)) OVER (
            PARTITION BY tida.asset_id 
            ORDER BY tida.date_id
        ) AS amount_invested_cumulative
    
        -- Current market value: (cumulative shares) × (current price in native currency) × (FX rate to EUR)
    ,   CAST(
            quantity_cumulative * stpr.price_adj_close_filled * COALESCE(exra.exchange_rate_filled, 1) -- Default FX rate to 1 if missing (e.g., EUR to EUR)     
            AS DECIMAL(10,2)
        ) AS portfolio_value_eur
    
    FROM asset_date_spine tida
    
    -- Only include assets we currently own
    INNER JOIN opened_positions oppo
        ON oppo.asset_id = tida.asset_id
    
    -- Bring in actual trades (most dates will be NULL - no trading activity)
    LEFT JOIN trades trad
        ON trad.transaction_date_id = tida.date_id
        AND trad.asset_id = tida.asset_id
    
    -- Get forward-filled asset price in native currency
    LEFT JOIN asset_prices stpr
        ON stpr.price_date_id = tida.date_id
        AND stpr.asset_id = tida.asset_id
    
    -- Get forward-filled exchange rate to convert to EUR
    LEFT JOIN exchange_rates exra
        ON exra.rate_date_id = tida.date_id
        AND exra.currency_id_from = stpr.price_currency_id
)

SELECT * FROM daily_snapshot WHERE portfolio_value_eur > 0