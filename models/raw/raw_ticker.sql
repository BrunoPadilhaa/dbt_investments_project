select
    ORIGINAL_SYMBOL ,
    CURRENT_TICKER,
    YF_SUFFIX,
    COUNTRY_CODE,
    ASSET_CLASSIFICATION,
    current_timestamp as load_ts,
    'raw_ticker_seed.csv' as source_system
from {{ ref('raw_ticker_seed') }}
