/*
    Portfolio Daily Snapshots
    
    Purpose: Creates daily snapshots of portfolio positions and valuations
    
    Features:
    - Forward-fills stock prices and exchange rates for non-trading days (weekends/holidays)
    - Calculates cumulative positions (running total of shares owned)
    - Converts all positions to EUR for unified reporting
    - Tracks only currently held positions (excludes fully sold tickers)
    
    Grain: One row per ticker per date
    
    Output columns:
    - date_id: Snapshot date (YYYYMMDD format)
    - ticker_id: Unique ticker identifier
    - quantity: Shares traded on this specific date (NULL if no trade)
    - amount: Money invested/divested on this date (negative = sold)
    - quantity_cumulative: Total shares owned as of this date
    - amount_invested_cumulative: Total capital deployed as of this date
    - portfolio_value_eur: Current market value in EUR
*/

-- Get all calendar dates up to yesterday (ensures complete price/rate data availability)

/*
    DATA QUALITY & KEY MANAGEMENT CONSIDERATIONS
    
    CURRENT STATE:
    - transaction_id is currently unique in fct_transactions (XTB broker only)
    - transaction_id serves as the primary key for XTB transactions
    
    FUTURE CHALLENGE - MULTI-BROKER SUPPORT:
    When integrating additional brokers (e.g., Clear Broker), we face a key collision risk:
    - XTB transaction_ids and Clear Broker transaction_ids may overlap
    - transaction_id alone cannot serve as a unique identifier across multiple sources
    
    PROPOSED SOLUTION:
    Implement a composite key or generate a surrogate key:
    Option 1: Composite Key (broker_id + transaction_id)
    Option 2: Hash Key (MD5/SHA of broker_id + transaction_id + transaction_date)
    Option 3: Surrogate Key (auto-incrementing warehouse-generated ID)
    
    REAL-WORLD EXAMPLE - BROKER TRANSACTION SPLITTING:
    Asset: IDVY
    Date: 2024-09-30
    Scenario: Single trade split into 3 separate transactions by broker
    
    What we see in source system:
    - 3 rows with identical date, price, and amount
    - 3 different transaction_ids (broker-assigned)
    - This is CORRECT behavior - broker executes large orders in batches
    
    Why this matters:
    - Each split transaction has a unique ID in the broker's system
    - We must preserve all 3 records to maintain audit trail
    - Deduplication based on date/price/amount would be INCORRECT
    
    ACTION REQUIRED:
    Before adding new broker sources, implement a universal transaction key strategy
    to prevent ID conflicts and maintain data integrity across all sources.
*/

WITH calendar AS (
    SELECT
        date_id
    FROM {{ ref('dim_date') }}
    WHERE DATE_ID <= TO_NUMBER(TO_CHAR(current_date, 'YYYYMMDD'))-1
)

-- Create spine: every asset for every date
-- This ensures we have rows even on days with no trades, allowing cumulative calculations
, asset_date_spine AS (
    SELECT 
        cale.date_id
    ,   asse.asset_id
    ,   asse.asset_code
    FROM calendar cale
    CROSS JOIN {{ ref('dim_asset') }} asse
)

-- Get distinct asset-currency combinations from asset prices
-- Needed to know which currency each asset's prices are denominated in
, asset_price_currency_asset AS (
    SELECT DISTINCT 
        asset_id
    ,   price_currency_id
    FROM {{ ref('fct_asset_prices') }}
)

-- Get all distinct currency pairs that exist in our exchange rate data
, currency_pairs AS (
    SELECT DISTINCT
        currency_id_from, 
        currency_id_to
    FROM {{ ref('fct_exchange_rates') }}
)

-- Create spine: every currency pair for every date
-- Allows forward-filling of exchange rates across non-trading days
, exchange_rates_spine AS (
    SELECT
        cale.date_id
    ,   cupa.currency_id_from
    ,   cupa.currency_id_to
    FROM calendar cale
    CROSS JOIN currency_pairs cupa
)

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
        FROM {{ ref('fct_exchange_rates') }} exra
        LEFT JOIN {{ ref('dim_currency') }} curr
            ON curr.currency_id = exra.currency_id_from
    ) exra
        ON exra.rate_date_id = ersp.date_id
        AND exra.currency_id_from = ersp.currency_id_from
        AND exra.currency_id_to = ersp.currency_id_to
)

-- Create spine: every asset for every date with its price currency
, asset_prices_spine AS (
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
    LEFT JOIN {{ ref('fct_asset_prices') }} stpr
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
    LEFT JOIN {{ ref('dim_asset') }} asse
        ON asse.asset_id = stpr.asset_id
    LEFT JOIN {{ ref('dim_currency') }} curr
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
    ,   CASE
            WHEN trty.transaction_type = 'Stock Sale' THEN tran.quantity * -1 
            ELSE tran.quantity
        END AS quantity
    ,   tran.amount * -1 AS amount  -- Negative = money out (purchase), positive = money in (sale)
    FROM {{ ref('fct_transactions') }} tran
    LEFT JOIN {{ ref('dim_asset') }} asse
        ON asse.asset_id = tran.asset_id
    LEFT JOIN {{ ref('dim_transaction_type') }} trty
        ON trty.transaction_type_id = tran.transaction_type_id
    WHERE trty.transaction_type IN ('Stock Purchase','Stock Sale')
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

SELECT * FROM daily_snapshot WHERE date_id >= 20250101