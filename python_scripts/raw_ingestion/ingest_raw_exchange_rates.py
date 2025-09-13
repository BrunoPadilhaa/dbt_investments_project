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

# --- Fetch last loaded price date ---
cs.execute(f"SELECT COALESCE(MAX(RATE_DATE), TO_DATE('2025-01-01')) FROM {SCHEMA}.{RAW_EXCHANGE_TABLE}")
max_date = cs.fetchone()[0]
logger.info(f"Fetching exchange rates starting from: {max_date + timedelta(days=1)}")

# --- Fetch existing currency/date pairs to avoid duplicates ---
cs.execute(f"SELECT CURRENCY_FROM, CURRENCY_TO, RATE_DATE FROM {SCHEMA}.{RAW_EXCHANGE_TABLE} WHERE RATE_DATE >= %s", (max_date,))
existing_rows = cs.fetchall()
existing_set = set((row[0], row[1], row[2]) for row in existing_rows)

# --- Currency pairs to fetch (from -> to, yf ticker) ---
currency_pairs = [
    ("USD", "EUR", "USDEUR=X"),
    ("BRL", "EUR", "BRLEUR=X"),
    ("GBP", "EUR", "GBPEUR=X")
]

# --- Fetch exchange rates ---
all_data = []

for from_cur, to_cur, yf_ticker in currency_pairs:
    try:
        start_date = max_date + timedelta(days=1)
        end_date = datetime.now().date()

        ticker = yf.Ticker(yf_ticker)
        data = ticker.history(start=start_date, end=end_date)

        if data.empty:
            logger.warning(f"No data for {yf_ticker}")
            continue

        for date, row in data.iterrows():
            rate_date = date.date()
            if (from_cur, to_cur, rate_date) in existing_set:
                logger.info(f"Skipping existing rate for {from_cur}/{to_cur} on {rate_date}")
                continue

            record = {
                "CURRENCY_FROM": from_cur,
                "CURRENCY_TO": to_cur,
                "RATE_DATE": rate_date,
                "EXCHANGE_RATE": round(1 / row["Close"], 6),  # invert for from -> to
                "SOURCE_SYSTEM": "yahoo_finance"
            }
            all_data.append(record)

        logger.info(f"Retrieved {len(data)} rows for {from_cur}/{to_cur}")

    except Exception as e:
        logger.error(f"Failed fetching {yf_ticker}: {e}")

# --- Load into Snowflake ---
if all_data:
    df = pd.DataFrame(all_data)
    success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_EXCHANGE_TABLE, schema=SCHEMA)
    if success:
        logger.info(f"Loaded {nrows} rows into {SCHEMA}.{RAW_EXCHANGE_TABLE}")
    else:
        logger.error("Failed to load data into Snowflake")
else:
    logger.warning("No new exchange rate data to load.")
