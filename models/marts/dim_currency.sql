WITH all_currencies AS (
SELECT 
    DISTINCT CURRENCY, SOURCE_SYSTEM, MAX(LOAD_TS) AS LOAD_TS
FROM {{ref('stg_stock_info')}}
GROUP BY 1,2
UNION
SELECT 
    DISTINCT CURRENCY_FROM, source_system, load_ts 
FROM {{ref('stg_exchange_rates')}}
WHERE CURRENCY_FROM NOT IN (SELECT DISTINCT CURRENCY FROM {{ref('stg_stock_info')}})
)

, currency_final AS (
SELECT
    ROW_NUMBER() OVER (ORDER BY CURRENCY DESC) AS CURRENCY_ID,
    CURRENCY,
    CASE 
        WHEN CURRENCY = 'USD' THEN 'US Dollar'
        WHEN CURRENCY = 'EUR' THEN 'Euro'
        WHEN CURRENCY = 'GBP' THEN 'British Pound'
        WHEN CURRENCY = 'BRL' THEN 'Brazilian Real'
        ELSE CURRENCY || ' Currency'  -- Handle unmapped currencies
    END AS CURRENCY_NAME,
    SOURCE_SYSTEM,
    LOAD_TS,
    CURRENT_TIMESTAMP() AS DBT_UPDATED_AT

FROM all_currencies
)

SELECT * from currency_final