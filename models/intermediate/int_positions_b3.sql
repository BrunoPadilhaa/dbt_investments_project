
--Created by Bruno Padilha on 2024-06-19
--This model is responsible for creating the intermediate table that consolidates the positions of equities and bonds from B3, 
--along with the amount invested in each asset. The amount invested is calculated based on the transactions made in each asset, 
--which are aggregated by month. Markets values are also included in the final output and it is cumulative. 

WITH transactions_b3 AS (
    SELECT
         DATE_TRUNC(month,trade_date) AS transaction_date
    ,    ASSET_CODE
    ,    SUM(QUANTITY) AS QUANTITY
    ,    SUM(AMOUNT) AS AMOUNT
    FROM {{ref('stg_trades_b3')}} 
    GROUP BY ALL
)

, positions_b3 AS (
SELECT
    asset_code
,   quantity
,   0 AS INVESTED_VALUE_IN_BRL --this is updated in the cte populated_invested_brl_equities
,   market_value_in_brl
,   source_file
,   source_system
,   load_ts --replace with dbt function
FROM {{ref('stg_positions_equities_b3')}}

UNION ALL

SELECT
    PRODUCT,
    QUANTITY,
    INVESTED_VALUE_IN_BRL,
    MARKET_VALUE_IN_BRL,
    SOURCE_FILE,
    SOURCE_SYSTEM,
    LOAD_TS
FROM {{ref('stg_positions_bonds_b3')}}
)

, include_position_date AS (
SELECT
    try_to_date(
            split_part(source_file, '-', 4) || '-' || 
            case
                lower(
                    trim(
                        split_part(split_part(source_file, '-', 5), '.', 1)
                    )
                )
                when 'janeiro' then '01'
                when 'fevereiro' then '02'
                when 'março' then '03'
                when 'marco' then '03'
                when 'abril' then '04'
                when 'maio' then '05'
                when 'junho' then '06'
                when 'julho' then '07'
                when 'agosto' then '08'
                when 'setembro' then '09'
                when 'outubro' then '10'
                when 'novembro' then '11'
                when 'dezembro' then '12'
            end || '-01'
        ) as position_date
, *
FROM positions_b3 posi

)

, populate_amount_invested AS ( 
    SELECT
        posi.position_date
    ,   posi.ASSET_CODE
    ,   posi.QUANTITY AS QUANTITY_CUMULATIVE
    ,   COALESCE(tran.AMOUNT, posi.INVESTED_VALUE_IN_BRL) AS AMOUNT_INVESTED_IN_BRL
    ,   posi.MARKET_VALUE_IN_BRL AS MARKET_VALUE_IN_BRL_CUMULATIVE
    ,   {{ dbt.current_timestamp() }} AS LOAD_TS
    FROM include_position_date posi
    
    LEFT 
    JOIN transactions_b3 tran 
    ON tran.transaction_date = posi.position_date
    AND tran.asset_code = posi.asset_code
    )

SELECT * FROM populate_amount_invested


       