
--Created by Bruno Padilha on 2024-06-19
--This model is responsible for creating the intermediate table that consolidates the positions of equities and bonds from B3, 
--along with the amount invested in each asset. The amount invested is calculated based on the transactions made in each asset, 
--which are aggregated by month. Markets values are also included in the final output and it is cumulative.
--position that were closed is automatically removed from the source system. 

WITH transactions_b3 AS (
    SELECT
         DATE_TRUNC(month,trade_date) AS transaction_date
    ,    ASSET_CODE
    ,    SHARES_HELD
    ,    COST_BASIS_IN_BRL
    FROM {{ref('stg_trades_b3')}}

    QUALIFY ROW_NUMBER() OVER (partition by ASSET_CODE, DATE_TRUNC('month', TRADE_DATE) order by trade_date desc) = 1
)

, positions_b3 AS (
    SELECT
        asset_code
    ,   shares_held
    ,   0 AS cost_basis_in_brl --this is updated in the cte populated_invested_brl_equities
    ,   market_value_in_brl
    ,   source_file
    ,   source_system
    FROM {{ref('stg_positions_equities_b3')}}

UNION ALL

SELECT
    PRODUCT,
    SHARES_HELD,
    0 AS COST_BASIS_IN_BRL, 
    MARKET_VALUE_IN_BRL,
    SOURCE_FILE,
    SOURCE_SYSTEM,
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
    ,   posi.SHARES_HELD
    ,   {{forward_fill('tran.COST_BASIS_IN_BRL', 'posi.ASSET_CODE', 'posi.position_date')}} AS COST_BASIS_IN_BRL
    ,   posi.MARKET_VALUE_IN_BRL
    ,   {{ dbt.current_timestamp() }} AS LOAD_TS
    FROM include_position_date posi
    
    LEFT 
    JOIN transactions_b3 tran 
    ON tran.transaction_date = posi.position_date
    AND tran.asset_code = posi.asset_code
    )

SELECT * FROM populate_amount_invested      