import pandas as pd
import pycountry
import pycountry_convert as pc
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

# --- Snowflake connection ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'
RAW_TRADES_TABLE = 'RAW_TRANSACTIONS_PT'
RAW_SYMBOL_COUNTRY_TABLE = 'RAW_STOCK_COUNTRY_MAPPING'

ctx = snowflake.connector.connect(
    user=USER,
    password=PWD,
    account=ACCOUNT,
    warehouse=WH,
    database=DATABASE,
    schema=SCHEMA
)

# --- Fetch distinct symbols from raw_trades ---
df_symbols = pd.read_sql(
    f"SELECT DISTINCT SYMBOL FROM {SCHEMA}.{RAW_TRADES_TABLE} WHERE TYPE = 'Stock purchase'",
    ctx
)

# --- Fetch existing symbols from country mapping ---
df_existing = pd.read_sql(f"SELECT SYMBOL FROM {SCHEMA}.{RAW_SYMBOL_COUNTRY_TABLE}", ctx)
existing_set = set(df_existing['SYMBOL'])

# --- Filter new symbols only ---
df_new = df_symbols[~df_symbols['SYMBOL'].isin(existing_set)]

if df_new.empty:
    print("No new symbols to process.")
else:
    # --- Extract suffix ---
    df_new['SUFFIX'] = df_new['SYMBOL'].apply(
        lambda x: x.split('.')[-1] if x and '.' in x else 'US'
    )
    
    # --- Manual mapping for Yahoo exceptions ---
    yahoo_exceptions = {
        'FR': 'PA',
        'IT': 'MI',
        'NL': 'AS',
        'BE': 'BR',   # correct Yahoo suffix for Belgium
        'DE': 'DE',
        'UK': 'L',
        'US': ''
    }
    df_new['YF_SUFFIX'] = df_new['SUFFIX'].apply(lambda s: yahoo_exceptions.get(s, s))
    
    # --- Manual mapping for country code exceptions ---
    iso_country_mapping = {
        'UK': 'GB',   # UK symbols use GB for ISO country lookup
        # Add other exceptions as needed
    }
    
    # --- Get country name ---
    def get_country_name(suffix):
        try:
            # Apply mapping for non-standard codes like UK -> GB
            lookup_code = iso_country_mapping.get(suffix, suffix)
            country = pycountry.countries.get(alpha_2=lookup_code)
            return country.name if country else None
        except Exception:
            return None
    
    # --- Get continent ---
    def get_continent(alpha2):
        try:
            # Apply mapping for non-standard codes like UK -> GB
            lookup_code = iso_country_mapping.get(alpha2, alpha2)
            continent_code = pc.country_alpha2_to_continent_code(lookup_code)
            continents = {
                'AF': 'Africa',
                'AS': 'Asia',
                'EU': 'Europe',
                'NA': 'North America',
                'SA': 'South America',
                'OC': 'Oceania',
                'AN': 'Antarctica'
            }
            return continents.get(continent_code)
        except Exception:
            return None
    
    df_new['COUNTRY_NAME'] = df_new['SUFFIX'].apply(get_country_name)
    df_new['CONTINENT'] = df_new['SUFFIX'].apply(get_continent)
    df_new['SOURCE_SYSTEM'] = 'pycountry library'
    
    # --- Final columns ---
    df_final = df_new[['SYMBOL', 'SUFFIX', 'YF_SUFFIX', 'COUNTRY_NAME', 'CONTINENT', 'SOURCE_SYSTEM']]
    
    # --- Write new rows to Snowflake ---
    success, nchunks, nrows, *_ = write_pandas(ctx, df_final, RAW_SYMBOL_COUNTRY_TABLE, schema=SCHEMA)
    
    if success:
        print(f"Inserted {nrows} new symbols into {RAW_SYMBOL_COUNTRY_TABLE}.")
    else:
        print("Failed to insert new symbols.")

# --- Close connection ---
ctx.close()