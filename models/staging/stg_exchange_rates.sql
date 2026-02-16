{{
    config(
        materialized = 'incremental'
    ,   unique_key = ['CURRENCY_FROM', 'CURRENCY_FROM', 'RATE_DATE']
    ,   incremental_strategy = 'merge'
    ,   on_schema_change = 'fail'
    )

}}

WITH cte_exchange_rates AS 
(
SELECT
    CURRENCY_FROM
,   CURRENCY_TO
,   CAST(RATE_DATE AS TIMESTAMP) AS RATE_DATE
,   CAST(EXCHANGE_RATE AS NUMBER(10,4)) AS EXCHANGE_RATE
,   SOURCE_SYSTEM
,   CAST(LOAD_TS AS TIMESTAMP) AS LOAD_TS
FROM {{source('raw','raw_exchange_rates')}}

{% if is_incremental() %}

    WHERE LOAD_TS > (SELECT COALESCE(MAX(LOAD_TS), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})

{% endif %}

)

SELECT * FROM cte_exchange_rates