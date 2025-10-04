import yfinance as yf
from datetime import datetime
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os

# --- Snowflake connection info ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'
RAW_TRADES_TABLE = 'RAW_TRANSACTIONS_PT'
RAW_STOCK_INFO_TABLE = 'RAW_SYMBOL'
RAW_TICKER_MAP_TABLE = 'RAW_STOCK_COUNTRY_MAPPING'

ctx = snowflake.connector.connect(
    user=USER,
    password=PWD,
    account=ACCOUNT,
    warehouse=WH,
    database=DATABASE,
    schema=SCHEMA
)
cs = ctx.cursor()

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Fetch distinct symbols from trades ---
cs.execute(f"SELECT DISTINCT SYMBOL FROM {SCHEMA}.{RAW_TRADES_TABLE} WHERE SYMBOL IS NOT NULL")
symbols = [row[0] for row in cs.fetchall()]
logger.info(f"Symbols from trades: {symbols}")

# --- Load ticker mapping from dbt seed ---
ticker_map_query = f"""
SELECT 
    SYMBOL,
    CASE 
        WHEN YF_SUFFIX IS NOT NULL AND YF_SUFFIX != '' 
        THEN SPLIT_PART(SYMBOL, '.', 1) || '.' || YF_SUFFIX
        ELSE SPLIT_PART(SYMBOL, '.', 1)
    END AS YF_SYMBOL
FROM {SCHEMA}.{RAW_TICKER_MAP_TABLE}
"""
ticker_map_df = pd.read_sql(ticker_map_query, ctx)
ticker_mapping = dict(zip(ticker_map_df["SYMBOL"], ticker_map_df["YF_SYMBOL"]))
logger.info(f"Loaded {len(ticker_mapping)} ticker mappings from {RAW_TICKER_MAP_TABLE}")

# --- Fetch stock info for ALL symbols ---
all_data = []

for stock in symbols:
    yf_symbol = ticker_mapping.get(stock, stock)
    
    try:
        logger.info(f"Fetching info for {stock} -> {yf_symbol}")
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        
        record = {
            "SYMBOL": stock,
            "SHORTNAME": info.get("shortName"),
            "LONGNAME": info.get("longName"),
            "QUOTETYPE": info.get("quoteType"),
            "SECTOR": info.get("sector"),
            "INDUSTRY": info.get("industry"),
            "CURRENCY": info.get("currency"),
            "EXCHANGE": info.get("exchange"),
            "INFO_FETCH_DATE": datetime.now().date(),
            "SOURCE_SYSTEM": "yahoo_finance"
        }
        all_data.append(record)
        logger.info(f"✅ Fetched info for {stock}")
        
    except Exception as e:
        logger.error(f"❌ Failed fetching info for {stock}: {e}")

# --- Truncate and Load into Snowflake ---
if all_data:
    df = pd.DataFrame(all_data)
    
    try:
        # Truncate existing data
        logger.info(f"Truncating {SCHEMA}.{RAW_STOCK_INFO_TABLE}")
        cs.execute(f"TRUNCATE TABLE IF EXISTS {SCHEMA}.{RAW_STOCK_INFO_TABLE}")
        
        # Load fresh data
        logger.info(f"Loading {len(df)} rows into {SCHEMA}.{RAW_STOCK_INFO_TABLE}")
        success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_STOCK_INFO_TABLE, schema=SCHEMA)
        
        if success:
            logger.info(f"✅ Successfully loaded {nrows} rows into {SCHEMA}.{RAW_STOCK_INFO_TABLE}")
        else:
            logger.error(f"❌ Failed to load data into Snowflake")
            
    except Exception as e:
        logger.error(f"❌ Error during truncate and load: {e}")
        
else:
    logger.warning("⚠️ No stock info fetched - table not updated")

# --- Cleanup ---
cs.close()
ctx.close()
logger.info("Script completed")