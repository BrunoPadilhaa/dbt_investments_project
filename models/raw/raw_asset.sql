select
    ASSET_CODE,
    ASSET_CODE_CURRENT,
    ASSET_NAME,
    CODE_SUFFIX,
    ASSET_TYPE,
    'raw_asset_seed.csv' as SOURCE_SYSTEM,
    CURRENT_TIMESTAMP() as LOAD_TS
from {{ ref('raw_asset_seed') }}
