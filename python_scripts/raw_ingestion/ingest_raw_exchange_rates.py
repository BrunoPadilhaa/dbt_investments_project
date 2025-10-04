import yfinance as yf
from datetime import datetime, timedelta
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
RAW_EXCHANGE_TABLE = 'RAW_EXCHANGE_RATES'

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

# --- Fetch last loaded date ---
cs.execute(f"SELECT MAX(RATE_DATE) FROM {SCHEMA}.{RAW_EXCHANGE_TABLE}")
max_date_result = cs.fetchone()[0]

# If table is empty, start from beginning of year
if max_date_result is None:
    start_date = datetime(2025, 1, 1).date()
    logger.info("Table is empty. Fetching from 2025-01-01")
else:
    # Start from day AFTER last loaded date
    start_date = max_date_result + timedelta(days=1)
    logger.info(f"Last loaded date: {max_date_result}. Fetching from: {start_date}")

end_date = datetime.now().date()
logger.info(f"Date range: {start_date} to {end_date}")

# --- Fetch existing currency/date pairs to avoid duplicates ---
if max_date_result:
    cs.execute(
        f"SELECT CURRENCY_FROM, CURRENCY_TO, RATE_DATE FROM {SCHEMA}.{RAW_EXCHANGE_TABLE} WHERE RATE_DATE >= %s",
        (start_date,)
    )
    existing_rows = cs.fetchall()
    existing_set = set((row[0], row[1], row[2]) for row in existing_rows)
else:
    existing_set = set()

# --- Currency pairs to fetch ---
currency_pairs = [
    ("USD", "EUR", "USDEUR=X"),
    ("BRL", "EUR", "BRLEUR=X"),
    ("GBP", "EUR", "GBPEUR=X")
]

# --- Fetch exchange rates ---
all_data = []

for from_cur, to_cur, yf_ticker in currency_pairs:
    try:
        logger.info(f"Fetching {yf_ticker} from {start_date} to {end_date}")
        ticker = yf.Ticker(yf_ticker)
        
        # Explicitly pass start and end dates
        data = ticker.history(start=start_date, end=end_date, interval="1d")
        
        if data.empty:
            logger.warning(f"No data for {yf_ticker}")
            continue
        
        rows_added = 0
        for date, row in data.iterrows():
            rate_date = date.date()
            
            if (from_cur, to_cur, rate_date) in existing_set:
                logger.debug(f"Skipping existing {from_cur}/{to_cur} on {rate_date}")
                continue
            
            record = {
                "CURRENCY_FROM": from_cur,
                "CURRENCY_TO": to_cur,
                "RATE_DATE": rate_date,
                "EXCHANGE_RATE": round(1 / row["Close"], 6),
                "SOURCE_SYSTEM": "yahoo finance"
            }
            all_data.append(record)
            rows_added += 1
        
        logger.info(f"Retrieved {rows_added} new rows for {from_cur}/{to_cur}")
        
    except Exception as e:
        logger.error(f"Failed fetching {yf_ticker}: {e}")

# --- Load into Snowflake ---
if all_data:
    df = pd.DataFrame(all_data)
    df = df.reset_index(drop=True)  # Fix pandas warning
    
    success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_EXCHANGE_TABLE, schema=SCHEMA)
    
    if success:
        logger.info(f"Loaded {nrows} rows into {SCHEMA}.{RAW_EXCHANGE_TABLE}")
    else:
        logger.error("Failed to load data into Snowflake")
else:
    logger.warning("No new exchange rate data to load.")

cs.close()
ctx.close()