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
RAW_TRADES_TABLE = 'RAW_TRADES_PT'
RAW_STOCK_INFO_TABLE = 'RAW_STOCK_INFO'
RAW_TICKER_MAP_TABLE = 'RAW_SYMBOL_MAPPING'  # dbt seed

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

# --- Fetch existing SYMBOLs to avoid duplicates ---
cs.execute(f"SELECT SYMBOL FROM {SCHEMA}.{RAW_STOCK_INFO_TABLE}")
existing_symbols = {row[0] for row in cs.fetchall()}
logger.info(f"Existing symbols in RAW_STOCK_INFO: {len(existing_symbols)}")

# --- Load ticker mapping from dbt seed ---
ticker_map_df = pd.read_sql(f"SELECT SYMBOL, YF_SYMBOL FROM {SCHEMA}.{RAW_TICKER_MAP_TABLE}", ctx)
ticker_mapping = dict(zip(ticker_map_df["SYMBOL"], ticker_map_df["YF_SYMBOL"]))
logger.info(f"Loaded {len(ticker_mapping)} ticker mappings from {RAW_TICKER_MAP_TABLE}")

# --- Fetch stock info ---
all_data = []

for stock in symbols:
    if stock in existing_symbols:
        logger.info(f"Skipping {stock}, already exists in RAW_STOCK_INFO")
        continue

    yf_symbol = ticker_mapping.get(stock, stock)  # fallback to itself
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
            "FULLTIMEEMPLOYEES": info.get("fullTimeEmployees"),
            "MARKETCAP": info.get("marketCap"),
            "DIVIDENDRATE": info.get("dividendRate"),
            "DIVIDENDYIELD": info.get("dividendYield"),
            "BETA": info.get("beta"),
            "TRAILINGPE": info.get("trailingPE"),
            "FORWARDPE": info.get("forwardPE"),
            "FIFTYTWOWEEKHIGH": info.get("fiftyTwoWeekHigh"),
            "FIFTYTWOWEEKLOW": info.get("fiftyTwoWeekLow"),
            "REGULARMARKETPRICE": info.get("regularMarketPrice"),
            "CURRENCY": info.get("currency"),
            "EXCHANGE": info.get("exchange"),
            "INFO_FETCH_DATE": datetime.now().date(),
            "SOURCE_SYSTEM": "yahoo"  # explicit for RAW contract
        }

        all_data.append(record)
        logger.info(f"Fetched info for {stock}")

    except Exception as e:
        logger.error(f"Failed fetching info for {stock}: {e}")

# --- Load into Snowflake ---
if all_data:
    df = pd.DataFrame(all_data)
    success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_STOCK_INFO_TABLE, schema=SCHEMA)
    if success:
        logger.info(f"Loaded {nrows} new rows into {SCHEMA}.{RAW_STOCK_INFO_TABLE}")
    else:
        logger.error("Failed to load data into Snowflake")
else:
    logger.info("No new stock info to insert")
