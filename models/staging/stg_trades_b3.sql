WITH RECURSIVE 

raw_trades_b3 AS ( 
    SELECT
        TO_DATE(TRADE_DATE) AS TRADE_DATE,
        TRANSACTION_TYPE,
        IFF(ENDSWITH(ASSET_CODE,'F'), LEFT(ASSET_CODE, LENGTH(ASSET_CODE)-1), ASSET_CODE) AS ASSET_CODE,
        TO_DECIMAL(QUANTITY,10,2) AS SHARES_TRADED,
        TO_DECIMAL(PRICE,10,2) AS PRICE,
        TO_DECIMAL(AMOUNT,10,2) AS NET_AMOUNT_TRADED_BRL,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM {{source('raw', 'raw_trades_b3')}}
)

, counter_for_dup_rows AS (
    -- we can have identical trades, so we need to add a counter to add these in the trade_key
    SELECT
        TRADE_DATE,
        TRANSACTION_TYPE,
        ASSET_CODE,
        SHARES_TRADED,
        PRICE,
        NET_AMOUNT_TRADED_BRL,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS,
        ROW_NUMBER() OVER (
            PARTITION BY TRADE_DATE, TRANSACTION_TYPE, ASSET_CODE, SHARES_TRADED, PRICE, NET_AMOUNT_TRADED_BRL
            ORDER BY TRADE_DATE DESC
        ) AS DUP_SQ
    FROM raw_trades_b3
)

, invert_amount_quantity_signal AS (
    SELECT
        TRADE_DATE,
        TRANSACTION_TYPE,
        ASSET_CODE,
        CASE
            WHEN TRANSACTION_TYPE = 'Venda' THEN -SHARES_TRADED
            ELSE SHARES_TRADED
        END AS SHARES_TRADED,
        PRICE,
        CASE
            WHEN TRANSACTION_TYPE = 'Venda' THEN -NET_AMOUNT_TRADED_BRL
            ELSE NET_AMOUNT_TRADED_BRL
        END AS NET_AMOUNT_TRADED_BRL,
        DUP_SQ,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM counter_for_dup_rows
)

-- Manual corporate actions (splits, manual fixes), already signed in the seed
, corporate_adjustments AS (
    SELECT
        TO_DATE(ADJUSTMENT_DATE)                      AS TRADE_DATE,
        IFF(COST_TREATMENT = 'REMOVE_COST', 'Baixa', 'Ajuste') AS TRANSACTION_TYPE,
        ASSET_CODE,
        CAST(SHARES_DELTA AS NUMBER(10,2))            AS SHARES_TRADED,
        CAST(NULL AS NUMBER(10,2))                    AS PRICE,
        CAST(COST_AMOUNT AS NUMBER(10,2))             AS NET_AMOUNT_TRADED_BRL,
        ROW_NUMBER() OVER (
            PARTITION BY TRADE_DATE, TRANSACTION_TYPE, ASSET_CODE, SHARES_TRADED, PRICE, NET_AMOUNT_TRADED_BRL
            ORDER BY TRADE_DATE DESC
        )                                             AS DUP_SQ,
        'raw_corporate_action_adjustments.csv'        AS SOURCE_FILE,
        'manual'                                      AS SOURCE_SYSTEM,
        NULL                                          AS LOAD_TS
    FROM {{ref('raw_corporate_action_adjustments')}}
    WHERE APPLY_IN_MODEL = 'Y'
)

, all_events AS (
    SELECT * FROM invert_amount_quantity_signal
    UNION ALL
    SELECT * FROM corporate_adjustments
)

, add_trade_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['TRADE_DATE', 'TRANSACTION_TYPE','ASSET_CODE', 'SHARES_TRADED','PRICE', 'NET_AMOUNT_TRADED_BRL', 'DUP_SQ']) }} AS TRADE_KEY,
        TRADE_DATE,
        TRANSACTION_TYPE,
        ASSET_CODE,
        SHARES_TRADED,
        PRICE,
        NET_AMOUNT_TRADED_BRL,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM all_events
)

, normalize_asset_codes AS (
    SELECT
        TRADE_KEY,
        TRADE_DATE,
        TRANSACTION_TYPE,
        COALESCE(conv.CURRENT_ASSET_CODE, addt.ASSET_CODE) AS ASSET_CODE,
        SHARES_TRADED,
        PRICE,
        NET_AMOUNT_TRADED_BRL,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM add_trade_key addt
    LEFT JOIN {{ref('raw_asset_conversion_mapping')}} conv
    ON addt.ASSET_CODE = conv.RAW_ASSET_CODE
)

, classify_trades AS (
    SELECT
        TRADE_KEY,
        TRADE_DATE,
        TRANSACTION_TYPE,
        ASSET_CODE,
        SHARES_TRADED,
        PRICE,
        NET_AMOUNT_TRADED_BRL,
        ROW_NUMBER() OVER (
            PARTITION BY ASSET_CODE
            ORDER BY
                TRADE_DATE,
                CASE TRANSACTION_TYPE
                    WHEN 'Ajuste' THEN 1
                    WHEN 'Compra' THEN 2
                    ELSE 3
                END
        ) AS ROW_NUM,
        SOURCE_FILE,
        SOURCE_SYSTEM,
        LOAD_TS
    FROM normalize_asset_codes
)

, running_cost_basis AS (
    SELECT
        TRADE_KEY,
        ASSET_CODE,
        ROW_NUM,
        TRADE_DATE,
        CAST(SHARES_TRADED AS NUMBER(18,2)) AS SHARES_HELD,
        CAST(NET_AMOUNT_TRADED_BRL AS NUMBER(18,6)) AS COST_BASIS_IN_BRL,
        SOURCE_FILE,
        SOURCE_SYSTEM
    FROM classify_trades
    WHERE ROW_NUM = 1

    UNION ALL

    SELECT
        t.TRADE_KEY,
        t.ASSET_CODE,
        t.ROW_NUM,
        t.TRADE_DATE,
        CAST(p.SHARES_HELD + t.SHARES_TRADED AS NUMBER(18,2)) AS SHARES_HELD,
        CAST(
            CASE t.TRANSACTION_TYPE
                WHEN 'Compra' THEN p.COST_BASIS_IN_BRL + t.NET_AMOUNT_TRADED_BRL
                WHEN 'Venda'  THEN p.COST_BASIS_IN_BRL + t.SHARES_TRADED * DIV0(p.COST_BASIS_IN_BRL, p.SHARES_HELD)
                WHEN 'Baixa'  THEN p.COST_BASIS_IN_BRL + t.SHARES_TRADED * DIV0(p.COST_BASIS_IN_BRL, p.SHARES_HELD)
                WHEN 'Ajuste' THEN p.COST_BASIS_IN_BRL + t.NET_AMOUNT_TRADED_BRL
            END
        AS NUMBER(18,6)) AS COST_BASIS_IN_BRL,
        t.SOURCE_FILE,
        t.SOURCE_SYSTEM
    FROM running_cost_basis p
    INNER JOIN classify_trades t
        ON  t.ASSET_CODE = p.ASSET_CODE
        AND t.ROW_NUM    = p.ROW_NUM + 1
)

SELECT
    TRADE_KEY,
    TRADE_DATE,
    ASSET_CODE,
    SHARES_HELD,
    COST_BASIS_IN_BRL,
    DIV0(COST_BASIS_IN_BRL, SHARES_HELD) AS AVG_COST_PRICE,
    SOURCE_FILE,
    SOURCE_SYSTEM,
    {{dbt.current_timestamp()}} AS LOAD_TS
FROM running_cost_basis