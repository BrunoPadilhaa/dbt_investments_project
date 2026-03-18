import yfinance as yf
from datetime import datetime, timedelta, date
import logging
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from ingestion.snowflake_connection import get_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

RAW_ASSET_PRICES_TABLE = "RAW_ASSET_PRICES"
RAW_ASSET_SEED_TABLE = "RAW_ASSET_SEED"


def load_asset_prices(ctx):

    cs = ctx.cursor()

    # --- Last loaded date per asset ---
    cs.execute(
        f"""
        SELECT ASSET_CODE, COALESCE(MAX(PRICE_DATE), TO_DATE('2024-01-01')) AS MAX_DATE
        FROM RAW.{RAW_ASSET_PRICES_TABLE}
        GROUP BY ASSET_CODE
        """
    )

    per_asset_max_date = {row[0]: row[1] for row in cs.fetchall()}
    logger.info(f"Per-asset max dates: {per_asset_max_date}")

    # --- Existing rows to avoid duplicates ---
    global_start = min(per_asset_max_date.values()) if per_asset_max_date else date(2024, 1, 1)

    cs.execute(
        f"""
        SELECT ASSET_CODE, PRICE_DATE
        FROM RAW.{RAW_ASSET_PRICES_TABLE}
        WHERE PRICE_DATE >= '{global_start}'
        """
    )

    existing_rows = cs.fetchall()
    existing_set = set((row[0], row[1]) for row in existing_rows)

    # --- Load mapping from seed ---
    asset_map_query = f"""
        SELECT 
            TRIM(ASSET_CODE) AS ASSET_CODE,
            TRIM(ASSET_CODE_YAHOO_FINANCE) AS YF_ASSET_CODE
        FROM RAW.{RAW_ASSET_SEED_TABLE}
        WHERE ASSET_CODE_YAHOO_FINANCE IS NOT NULL
    """

    asset_map_df = pd.read_sql(asset_map_query, ctx)

    asset_map_df["ASSET_CODE"] = asset_map_df["ASSET_CODE"].str.replace(r"\s+", "", regex=True)
    asset_map_df["YF_ASSET_CODE"] = asset_map_df["YF_ASSET_CODE"].str.replace(r"\s+", "", regex=True)

    asset_mapping = dict(zip(asset_map_df["ASSET_CODE"], asset_map_df["YF_ASSET_CODE"]))
    asset_codes = asset_map_df["ASSET_CODE"].tolist()

    logger.info(f"Loaded {len(asset_mapping)} assets from {RAW_ASSET_SEED_TABLE}")

    all_data = []

    default_start = date(2024, 1, 1)
    end_date = datetime.now().date()

    for asset_code in asset_codes:

        yf_asset_code = asset_mapping.get(asset_code)

        try:

            max_date = per_asset_max_date.get(asset_code, default_start)

            if isinstance(max_date, datetime):
                max_date = max_date.date()

            start_date = max_date + timedelta(days=1)

            logger.info(f"Fetching {asset_code} -> {yf_asset_code} from {start_date}")

            ticker = yf.Ticker(yf_asset_code)
            currency = ticker.info.get("currency")

            data = ticker.history(
                start=start_date,
                end=end_date,
                interval="1d",
                auto_adjust=False
            )

            if data.empty:
                logger.warning(f"No data for {asset_code} ({yf_asset_code})")
                continue

            for dt, row in data.iterrows():

                record = {
                    "ASSET_CODE": asset_code,
                    "PRICE_DATE": dt.date(),
                    "PRICE_OPEN": round(row["Open"], 2),
                    "PRICE_HIGH": round(row["High"], 2),
                    "PRICE_LOW": round(row["Low"], 2),
                    "PRICE_CLOSE": round(row["Close"], 2),
                    "PRICE_ADJ_CLOSE": round(row.get("Adj Close"), 2) if pd.notna(row.get("Adj Close")) else None,
                    "PRICE_VOLUME": int(row["Volume"]),
                    "CURRENCY": currency,
                    "SOURCE_SYSTEM": "yahoo_finance"
                }

                all_data.append(record)

            logger.info(f"Retrieved {len(data)} rows for {asset_code} ({yf_asset_code})")

        except Exception as e:
            logger.error(f"Failed {asset_code} -> {yf_asset_code}: {e}")

    if not all_data:
        logger.warning("No new data retrieved for any asset.")
        return

    df = pd.DataFrame(all_data)

    # --- Remove duplicates ---
    df = df[~df.apply(lambda r: (r["ASSET_CODE"], r["PRICE_DATE"]) in existing_set, axis=1)]

    if df.empty:
        logger.info("No new rows to insert after filtering existing prices.")
        return

    logger.info(f"Total new rows to insert: {len(df)}")

    success, nchunks, nrows, _ = write_pandas(ctx, df, RAW_ASSET_PRICES_TABLE, schema="RAW")

    if success:
        logger.info(f"Loaded {nrows} rows into {RAW_ASSET_PRICES_TABLE}")
    else:
        logger.error("Failed to load data into Snowflake")

    cs.close()
    
if __name__ == "__main__":
    ctx = get_connection()
    load_asset_prices(ctx)