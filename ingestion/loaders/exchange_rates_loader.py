import yfinance as yf
from datetime import datetime, timedelta
import logging
import pandas as pd

from ingestion.snowflake_connection import get_connection  # shared connector

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Table ---
RAW_EXCHANGE_TABLE = "RAW_EXCHANGE_RATES"


import yfinance as yf
from datetime import datetime, timedelta
import logging
import pandas as pd

from ingestion.snowflake_connection import get_connection  # shared connector

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Table ---
RAW_EXCHANGE_TABLE = "RAW_EXCHANGE_RATES"


def load_exchange_rates():
    """Fetch daily exchange rates from Yahoo Finance and load into Snowflake."""
    ctx = get_connection()
    cs = ctx.cursor()

    # --- Last loaded date ---
    cs.execute(f"SELECT MAX(RATE_DATE) FROM RAW.{RAW_EXCHANGE_TABLE}")
    max_date_result = cs.fetchone()[0]

    if max_date_result is None:
        start_date = datetime(2024, 1, 1).date()
        logger.info("Table is empty. Fetching from 2024-01-01")
    else:
        start_date = max_date_result  # re-fetch last date too, in case it was partial
        logger.info(f"Last loaded date: {max_date_result}. Fetching from: {start_date}")

    end_date = datetime.now().date()
    logger.info(f"Date range: {start_date} to {end_date}")

    # --- Currency pairs ---
    currency_pairs = [
        ("USD", "EUR", "USDEUR=X"),
        ("BRL", "EUR", "BRLEUR=X"),
        ("GBP", "EUR", "GBPEUR=X")
    ]

    all_data = []

    for from_cur, to_cur, yf_ticker in currency_pairs:
        try:
            logger.info(f"Fetching {yf_ticker} from {start_date} to {end_date}")
            ticker = yf.Ticker(yf_ticker)
            data = ticker.history(start=start_date, end=end_date, interval="1d")

            if data.empty:
                logger.warning(f"No data for {yf_ticker}")
                continue

            for dt, row in data.iterrows():
                all_data.append({
                    "CURRENCY_FROM": from_cur,
                    "CURRENCY_TO": to_cur,
                    "RATE_DATE": dt.date(),
                    "EXCHANGE_RATE": round(row["Close"], 6),
                    "SOURCE_SYSTEM": "yahoo finance"
                })

            logger.info(f"Retrieved {len(data)} rows for {from_cur}/{to_cur}")

        except Exception as e:
            logger.error(f"Failed fetching {yf_ticker}: {e}")

    # --- Delete date range before inserting (idempotent) ---
    if all_data:
        logger.info(f"Deleting existing data from {start_date} to {end_date} before reload")
        cs.execute(
            f"DELETE FROM RAW.{RAW_EXCHANGE_TABLE} WHERE RATE_DATE BETWEEN %s AND %s",
            (start_date, end_date)
        )

        df = pd.DataFrame(all_data).reset_index(drop=True)
        from snowflake.connector.pandas_tools import write_pandas
        success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_EXCHANGE_TABLE, schema="RAW")
        if success:
            logger.info(f"✅ Loaded {nrows} rows into {RAW_EXCHANGE_TABLE}")
        else:
            logger.error("❌ Failed to load data into Snowflake")
    else:
        logger.warning("No new exchange rate data to load.")

    cs.close()
    ctx.close()