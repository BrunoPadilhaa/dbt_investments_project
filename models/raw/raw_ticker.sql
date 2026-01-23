select
    SYMBOL,
    CURRENT_TICKER,
    TICKER_NAME,
    BUCKET_TYPE,
    EXCHANGE_SUFFIX,
    COUNTRY_CODE,
    current_timestamp as load_ts,
    'raw_ticker_seed.csv' as source_system
from {{ ref('raw_ticker_seed') }}
