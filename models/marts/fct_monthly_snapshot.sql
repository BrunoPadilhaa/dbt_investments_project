-------- DATE SPINE -----------
WITH date_spine AS (
    SELECT
        DISTINCT DATE_TRUNC(month, date) AS snapshot_date
    FROM
        PROD.DIM_DATE
    WHERE
        date >= '2019-10-01'
        AND date <= current_date()
),
asset_dates_spine AS (
    SELECT
        SNAPSHOT_DATE,
        ASSET_ID,
        CURRENT_ASSET_CODE AS ASSET_CODE
    FROM prod.dim_asset

    CROSS JOIN date_spine
) 

------------------- ASSET PRICES ------------
, asset_prices AS (
    SELECT
        DATE_TRUNC(month, price_date) AS price_date,
        ASSET_CODE,
        price_adj_close
    FROM
        PROD.FCT_ASSET_PRICES QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ASSET_CODE,
            TO_VARCHAR(price_date, 'MMYYYY')
            ORDER BY
                price_date DESC
        ) = 1
) ------------------- EXCHANGE_RATES ----------
, exchange_rates AS (
    SELECT
        DATE_TRUNC(month, rate_date) AS rate_date,
        CURRENCY_FROM,
        CURRENCY_TO,
        EXCHANGE_RATE
    FROM PROD.FCT_EXCHANGE_RATES 
    QUALIFY ROW_NUMBER() OVER (PARTITION BY currency_from, TO_VARCHAR(rate_date, 'MMYYYY') ORDER BY rate_date DESC) = 1

    ) 
 ------------------- XTB POSITIONS -----------

,xtb_positions AS (
    SELECT
        DATE_TRUNC(month, transaction_date) AS transaction_date,
        ASSET_CODE,
        SUM(SHARES_TRADED) AS SHARES_TRADED,
        SUM(NET_AMOUNT_TRADED_IN_EUR) AS NET_AMOUNT_TRADED_IN_EUR
    FROM intermediate.int_positions_xtb poxt
    
    GROUP BY
        ALL
),
xtb_positions_snapshot AS (
    SELECT
        spin.SNAPSHOT_DATE,
        spin.ASSET_CODE,
        --xtbp.SHARES,
        SUM(xtbp.SHARES_TRADED) OVER (PARTITION BY spin.ASSET_CODE ORDER BY snapshot_date) AS SHARES_HELD,
        xtbp.NET_AMOUNT_TRADED_IN_EUR,
        SUM(xtbp.NET_AMOUNT_TRADED_IN_EUR) OVER ( PARTITION BY spin.ASSET_CODE ORDER BY snapshot_date) AS COST_BASIS_IN_EUR
    FROM asset_dates_spine spin
    
    LEFT 
    JOIN xtb_positions xtbp ON spin.snapshot_date = xtbp.transaction_date
    AND spin.ASSET_CODE = xtbp.ASSET_CODE

    INNER
    JOIN prod.dim_asset asse
    ON xtbp.ASSET_CODE = asse.current_asset_code
       
    WHERE spin.SNAPSHOT_DATE >= '2025-04-01' --investing start date on europe
    ORDER BY 2,1
),
xtb_positions_snapshot_w_market_value AS (
    SELECT
        xtpo.SNAPSHOT_DATE,
        xtpo.ASSET_CODE,
        xtpo.SHARES_HELD,
        xtpo.NET_AMOUNT_TRADED_IN_EUR,
        xtpo.COST_BASIS_IN_EUR,
        xtpo.COST_BASIS_IN_EUR * exra.exchange_rate AS COST_BASIS_IN_BRL,
        xtpo.shares_held * aspr.price_adj_close AS MARKET_VALUE_IN_EUR,
        (xtpo.shares_held * aspr.price_adj_close) * exra.exchange_rate AS MARKET_VALUE_IN_BRL
    FROM
        xtb_positions_snapshot xtpo
        LEFT JOIN exchange_rates exra ON exra.rate_date = xtpo.snapshot_date
        AND CURRENCY_FROM = 'EUR'
        AND CURRENCY_TO = 'BRL'
        
        LEFT JOIN asset_prices aspr ON aspr.price_date = xtpo.snapshot_date
        AND aspr.ASSET_CODE = xtpo.ASSET_CODE
) ------------------- B3 POSITIONS -----------

, b3_positions_w_market_value AS ( 
    SELECT
        POSITION_DATE,
        ASSET_CODE,
        SHARES_HELD,
        COST_BASIS_IN_BRL,
        COST_BASIS_IN_BRL / exra.exchange_rate AS COST_BASIS_IN_EUR,
        MARKET_VALUE_IN_BRL,
        MARKET_VALUE_IN_BRL / exra.exchange_rate AS MARKET_VALUE_IN_EUR
    FROM
        INTERMEDIATE.INT_MONTHLY_POSITIONS_SNAPSHOT_B3 bpos
     LEFT JOIN exchange_rates exra 
     ON exra.rate_date = bpos.POSITION_DATE
     AND CURRENCY_FROM = 'EUR'
     AND CURRENCY_TO = 'BRL'

    )

, all_monthly_positions AS (

    SELECT 
           SNAPSHOT_DATE,
           ASSET_CODE,
           SHARES_HELD,
           COST_BASIS_IN_EUR,
           COST_BASIS_IN_BRL,
           MARKET_VALUE_IN_EUR,
           MARKET_VALUE_IN_BRL
    FROM xtb_positions_snapshot_w_market_value
    
    UNION ALL
    
    SELECT
        POSITION_DATE,
        ASSET_CODE,
        SHARES_HELD,
        COST_BASIS_IN_EUR,
        COST_BASIS_IN_BRL,
        MARKET_VALUE_IN_EUR,
        MARKET_VALUE_IN_BRL
    FROM b3_positions_w_market_value
    )

SELECT * FROM all_monthly_positions
WHERE COST_BASIS_IN_BRL is null 
OR COST_BASIS_IN_EUR IS NULL