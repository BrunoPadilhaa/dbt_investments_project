WITH assets AS (
    SELECT
        DISTINCT SPLIT_PART(SYMBOL,'.',1) AS ASSET_CODE
    ,   'Portugal' AS investment_country
    FROM {{ source('raw','raw_transactions_xtb')}}
    WHERE SYMBOL IS NOT NULL

    UNION ALL

    SELECT
        DISTINCT ASSET_CODE
    ,   'Brazil' AS investment_country
    FROM {{ source('raw','raw_positions_equities_b3') }}

    UNION ALL

    SELECT
        DISTINCT product
    ,   'Brazil' AS investment_country
    FROM {{source('raw','raw_positions_bonds_b3')}}

)

SELECT * FROM assets