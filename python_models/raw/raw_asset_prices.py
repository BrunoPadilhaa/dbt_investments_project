import yfinance as yf
from datetime import datetime, timedelta, date
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
from dotenv import load_dotenv

# --- Load environment variables from .env file ---
load_dotenv('python_env.env')  # Specify your .env file name

# --- Snowflake connection info ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'
RAW_ASSET_PRICES_TABLE = 'RAW_ASSET_PRICES'
RAW_ASSET_SEED_TABLE = 'RAW_ASSET_SEED'

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

# --- Fetch last loaded price date PER ASSET ---
cs.execute(
    f"""
    SELECT ASSET_CODE, COALESCE(MAX(PRICE_DATE), TO_DATE('2024-01-01')) AS MAX_DATE
    FROM {SCHEMA}.{RAW_ASSET_PRICES_TABLE}
    GROUP BY ASSET_CODE
    """
)
per_asset_max_date = {row[0]: row[1] for row in cs.fetchall()}

logger.info(f"Per-asset max dates: {per_asset_max_date}")

# --- Fetch existing asset_code/date pairs to avoid duplicates ---
global_start = min(per_asset_max_date.values()) if per_asset_max_date else date(2024, 1, 1)

cs.execute(
    f"""
    SELECT ASSET_CODE, PRICE_DATE
    FROM {SCHEMA}.{RAW_ASSET_PRICES_TABLE}
    WHERE PRICE_DATE >= '{global_start}'
    """
)
existing_rows = cs.fetchall()
existing_set = set((row[0], row[1]) for row in existing_rows)

# --- Load asset mapping from RAW_ASSET_SEED ---
asset_map_query = f"""
    SELECT 
        ASSET_CODE,
        CASE 
            WHEN TRIM(CODE_SUFFIX) IS NOT NULL AND TRIM(CODE_SUFFIX) != '' 
            THEN TRIM(ASSET_CODE) || '.' || TRIM(CODE_SUFFIX)
            ELSE TRIM(ASSET_CODE)
        END AS YF_ASSET_CODE
    FROM {SCHEMA}.{RAW_ASSET_SEED_TABLE}
"""
asset_map_df = pd.read_sql(asset_map_query, ctx)

# --- Strip any residual whitespace from ticker symbols (e.g. non-breaking spaces) ---
asset_map_df["ASSET_CODE"] = asset_map_df["ASSET_CODE"].str.strip().str.replace(r'\s+', '', regex=True)
asset_map_df["YF_ASSET_CODE"] = asset_map_df["YF_ASSET_CODE"].str.strip().str.replace(r'\s+', '', regex=True)

logger.info(f"Asset mapping after cleaning:\n{asset_map_df.to_string(index=False)}")

asset_mapping = dict(zip(asset_map_df["ASSET_CODE"], asset_map_df["YF_ASSET_CODE"]))
asset_codes = asset_map_df["ASSET_CODE"].tolist()

logger.info(f"Loaded {len(asset_mapping)} assets from {RAW_ASSET_SEED_TABLE}")
logger.info(f"Assets to fetch: {asset_codes}")


def fetch_asset_price_data(assets, per_asset_max_date, end_date, asset_map):
    """Fetch asset price data from Yahoo Finance for given asset codes."""
    all_data = []
    default_start = date(2024, 1, 1)

    for asset_code in assets:
        yf_asset_code = asset_map.get(asset_code, asset_code)
        try:
            # --- Per-asset start date ---
            max_date = per_asset_max_date.get(asset_code, default_start)
            if isinstance(max_date, datetime):
                max_date = max_date.date()
            start_date = max_date + timedelta(days=1)

            logger.info(f"Fetching {asset_code} -> {yf_asset_code} from {start_date}")
            ticker = yf.Ticker(yf_asset_code)

            # --- Fetch currency once per asset ---
            currency = ticker.info.get("currency")

            data = ticker.history(
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=False
            )

            if data.empty:
                logger.warning(f"No data for {asset_code} (YF: {yf_asset_code})")
                continue

            for dt, row in data.iterrows():
                record = {
                    "ASSET_CODE": asset_code,
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
                    "CURRENCY": currency,
                    "SOURCE_SYSTEM": "yahoo finance"
                }
                all_data.append(record)

            logger.info(
                f"Retrieved {len(data)} rows for {asset_code} -> {yf_asset_code} ({currency})"
            )

        except Exception as e:
            logger.error(f"Failed {asset_code} -> {yf_asset_code}: {e}")

    if not all_data:
        logger.error("No data retrieved")
        return None

    return pd.DataFrame(all_data)


# --- Main execution ---
if __name__ == "__main__":
    end_date = datetime.now().date()
    df = fetch_asset_price_data(asset_codes, per_asset_max_date, end_date, asset_mapping)

    if df is not None and not df.empty:
        df = df[
            ~df.apply(
                lambda r: (r["ASSET_CODE"], r["PRICE_DATE"]) in existing_set,
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
                RAW_ASSET_PRICES_TABLE,
                schema=SCHEMA
            )
            if success:
                logger.info(f"Loaded {nrows} rows into {SCHEMA}.{RAW_ASSET_PRICES_TABLE}")
            else:
                logger.error("Failed to load data into Snowflake")
    else:
        logger.warning("No data to load.")

    cs.close()
    ctx.close()