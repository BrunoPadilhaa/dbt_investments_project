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
RAW_STOCK_PRICES_TABLE = 'RAW_STOCK_PRICES'
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
cs.execute(f"SELECT DISTINCT SYMBOL FROM {SCHEMA}.{RAW_TRADES_TABLE}")
symbols = [row[0] for row in cs.fetchall()]
logger.info(f"Symbols from trades: {symbols}")

# --- Fetch last loaded price date ---
cs.execute(f"SELECT COALESCE(MAX(PRICE_DATE), TO_DATE('2025-01-01')) FROM {SCHEMA}.{RAW_STOCK_PRICES_TABLE}")
max_date = cs.fetchone()[0]
logger.info(f"Fetching prices starting from: {max_date}")

# --- Fetch existing symbol/date pairs to avoid duplicates ---
cs.execute(f"SELECT SYMBOL, PRICE_DATE FROM {SCHEMA}.{RAW_STOCK_PRICES_TABLE} WHERE PRICE_DATE >= %s", (max_date,))
existing_rows = cs.fetchall()
existing_set = set((row[0], row[1]) for row in existing_rows)

# --- Load ticker mapping from dbt seed ---
ticker_map_df = pd.read_sql(f"SELECT SYMBOL, YF_SYMBOL FROM {SCHEMA}.{RAW_TICKER_MAP_TABLE}", ctx)
ticker_mapping = dict(zip(ticker_map_df["SYMBOL"], ticker_map_df["YF_SYMBOL"]))
logger.info(f"Loaded {len(ticker_mapping)} ticker mappings from {RAW_TICKER_MAP_TABLE}")

def fetch_stock_data(stocks, start_date, end_date):
    all_data = []

    for stock in stocks:
        yf_symbol = ticker_mapping.get(stock, stock)  # fallback to original symbol
        try:
            logger.info(f"Fetching {stock} -> {yf_symbol}")
            ticker = yf.Ticker(yf_symbol)
            # Force daily interval and keep Close and Adj Close
            data = ticker.history(start=start_date, end=end_date, interval="1d", auto_adjust=False)

            if data.empty:
                logger.warning(f"No data for {stock}")
                continue

            # Normalize schema to RAW_STOCK_PRICES
            for date, row in data.iterrows():
                record = {
                    "SYMBOL": stock,
                    "PRICE_DATE": date.date(),
                    "PRICE_OPEN": round(row["Open"], 2),
                    "PRICE_HIGH": round(row["High"], 2),
                    "PRICE_LOW": round(row["Low"], 2),
                    "PRICE_CLOSE": round(row["Close"], 2),
                    "PRICE_ADJ_CLOSE": round(row["Adj Close"], 2) if "Adj Close" in row else None,
                    "PRICE_VOLUME": int(row["Volume"]),
                    "SOURCE_SYSTEM": "yahoo"
                }
                all_data.append(record)

            logger.info(f"Retrieved {len(data)} rows for {stock}")

        except Exception as e:
            logger.error(f"Failed {stock}: {e}")

    if not all_data:
        logger.error("No data retrieved")
        return None

    return pd.DataFrame(all_data)

# --- Main execution ---
if __name__ == "__main__":
    end_date = datetime.now().date()
    df = fetch_stock_data(symbols, max_date, end_date)

    if df is not None and not df.empty:
        # Filter out existing rows
        df = df[~df.apply(lambda row: (row["SYMBOL"], row["PRICE_DATE"]) in existing_set, axis=1)]

        if df.empty:
            logger.info("No new rows to insert after filtering existing prices.")
        else:
            logger.info(f"Total new rows to insert: {len(df)}")
            success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_STOCK_PRICES_TABLE, schema=SCHEMA)
            if success:
                logger.info(f"Loaded {nrows} rows into {SCHEMA}.{RAW_STOCK_PRICES_TABLE}")
            else:
                logger.error("Failed to load data into Snowflake")
    else:
        logger.warning("No data to load.")
