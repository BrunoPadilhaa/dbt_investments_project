import yfinance as yf
from datetime import datetime, timedelta, date
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
RAW_STOCK_PRICES_TABLE = 'RAW_STOCK_PRICES'
RAW_TICKER_TABLE = 'RAW_TICKER'

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
cs.execute(
    f"""
    SELECT COALESCE(MAX(PRICE_DATE), TO_DATE('2025-01-01'))
    FROM {SCHEMA}.{RAW_STOCK_PRICES_TABLE}
    """
)
max_date = cs.fetchone()[0]

# Normalize Snowflake DATE -> python date
if isinstance(max_date, datetime):
    max_date = max_date.date()

start_date = max_date + timedelta(days=1)

logger.info(f"Last loaded date: {max_date}")
logger.info(f"Fetching prices starting from: {start_date}")

# --- Fetch existing symbol/date pairs to avoid duplicates ---
cs.execute(
    f"""
    SELECT SYMBOL, PRICE_DATE
    FROM {SCHEMA}.{RAW_STOCK_PRICES_TABLE}
    WHERE PRICE_DATE >= '{start_date}'
    """
)
existing_rows = cs.fetchall()
existing_set = set((row[0], row[1]) for row in existing_rows)

# --- Load ticker mapping and symbols from RAW_TICKER ---
ticker_map_query = f"""
    SELECT 
        SYMBOL,
        CASE 
            WHEN EXCHANGE_SUFFIX IS NOT NULL AND EXCHANGE_SUFFIX != '' 
            THEN CURRENT_TICKER || '.' || EXCHANGE_SUFFIX
            ELSE CURRENT_TICKER
        END AS YF_TICKER
    FROM {SCHEMA}.{RAW_TICKER_TABLE}
"""
ticker_map_df = pd.read_sql(ticker_map_query, ctx)
ticker_mapping = dict(zip(ticker_map_df["SYMBOL"], ticker_map_df["YF_TICKER"]))
symbols = ticker_map_df["SYMBOL"].tolist()

logger.info(f"Loaded {len(ticker_mapping)} tickers from {RAW_TICKER_TABLE}")
logger.info(f"Symbols to fetch: {symbols}")


def fetch_stock_data(stocks, start_date, end_date, ticker_map):
    """Fetch stock price data from Yahoo Finance for given symbols."""
    all_data = []

    for stock in stocks:
        yf_symbol = ticker_map.get(stock, stock)
        try:
            logger.info(f"Fetching {stock} -> {yf_symbol}")
            ticker = yf.Ticker(yf_symbol)
            data = ticker.history(
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=False
            )

            if data.empty:
                logger.warning(f"No data for {stock} (YF: {yf_symbol})")
                continue

            for dt, row in data.iterrows():
                record = {
                    "SYMBOL": stock,  # original symbol name
                    "PRICE_DATE": dt.date(),
                    "PRICE_OPEN": round(row["Open"], 2),
                    "PRICE_HIGH": round(row["High"], 2),
                    "PRICE_LOW": round(row["Low"], 2),
                    "PRICE_CLOSE": round(row["Close"], 2),
                    "PRICE_ADJ_CLOSE": (
                        round(row.get("Adj Close"), 2)
                        if pd.notna(row.get("Adj Close"))
                        else None
                    ),
                    "PRICE_VOLUME": int(row["Volume"]),
                    "SOURCE_SYSTEM": "yahoo finance"
                }
                all_data.append(record)

            logger.info(f"Retrieved {len(data)} rows for {stock} -> {yf_symbol}")

        except Exception as e:
            logger.error(f"Failed {stock} -> {yf_symbol}: {e}")

    if not all_data:
        logger.error("No data retrieved")
        return None

    return pd.DataFrame(all_data)


# --- Main execution ---
if __name__ == "__main__":
    end_date = datetime.now().date()
    df = fetch_stock_data(symbols, start_date, end_date, ticker_mapping)

    if df is not None and not df.empty:
        df = df[
            ~df.apply(
                lambda r: (r["SYMBOL"], r["PRICE_DATE"]) in existing_set,
                axis=1
            )
        ]

        if df.empty:
            logger.info("No new rows to insert after filtering existing prices.")
        else:
            logger.info(f"Total new rows to insert: {len(df)}")
            success, nchunks, nrows, _ = write_pandas(
                ctx,
                df,
                RAW_STOCK_PRICES_TABLE,
                schema=SCHEMA
            )
            if success:
                logger.info(f"Loaded {nrows} rows into {SCHEMA}.{RAW_STOCK_PRICES_TABLE}")
            else:
                logger.error("Failed to load data into Snowflake")
    else:
        logger.warning("No data to load.")

    cs.close()
    ctx.close()
