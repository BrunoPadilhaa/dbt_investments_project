import yfinance as yf
from datetime import datetime
import logging
import pandas as pd
from typing import Dict

from ingestion.snowflake_connection import get_connection

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Tables ---
RAW_ASSET_DETAILS_TABLE = 'RAW_ASSET_DETAILS'
RAW_ASSET_SEED_TABLE = 'RAW_ASSET_SEED'


def load_asset_info(asset_code: str, yf_asset_code: str) -> Dict:
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_system = "yahoo_finance"

    record = {
        "ASSET_CODE": asset_code,
        "ASSET_CODE_SYSTEM": yf_asset_code,
        "SOURCE_SYSTEM": source_system,
        "LOAD_TS": now_ts
    }

    try:
        info = yf.Ticker(yf_asset_code).info
        country = info.get("country") or info.get("domicile") or info.get("fundCountry")

        record.update({
            "SHORTNAME": info.get("shortName"),
            "LONGNAME": info.get("longName"),
            "QUOTETYPE": info.get("quoteType"),
            "SECTOR": info.get("sector"),
            "INDUSTRY": info.get("industry"),
            "CURRENCY": info.get("currency"),
            "EXCHANGE": info.get("exchange"),
            "COUNTRY": country
        })

        logger.info(f"Fetched info for {asset_code} -> {yf_asset_code}")

    except Exception as e:
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE", "COUNTRY"]:
            record[col] = None

        logger.error(f"Failed fetching info for {asset_code}: {e}")

    return record


def fetch_assets_from_seed():

    logger.info("=" * 60)
    logger.info("Asset Details Update (Seed-Driven)")
    logger.info("=" * 60)

    ctx = get_connection()
    cs = ctx.cursor()

    try:

        logger.info(f"Truncating {RAW_ASSET_DETAILS_TABLE}")
        cs.execute(f"TRUNCATE TABLE IF EXISTS RAW.{RAW_ASSET_DETAILS_TABLE}")

        cs.execute(f"""
            SELECT DISTINCT
                ASSET_CODE,
                ASSET_CODE_SYSTEM
            FROM RAW.{RAW_ASSET_SEED_TABLE}
            WHERE ASSET_CODE_SYSTEM IS NOT NULL
        """)

        rows = cs.fetchall()
        logger.info(f"Found {len(rows)} assets to process")

        all_data = []

        for asset_code, sys_asset_code in rows:

            record = load_asset_info(asset_code, sys_asset_code)
            all_data.append(record)

        if all_data:

            df = pd.DataFrame(all_data)

            from snowflake.connector.pandas_tools import write_pandas

            success, _, nrows, _ = write_pandas(
                ctx,
                df,
                RAW_ASSET_DETAILS_TABLE,
                schema="RAW"
            )

            if success:
                logger.info(f"Loaded {nrows} rows into {RAW_ASSET_DETAILS_TABLE}")
            else:
                logger.error("Failed to load data")

    finally:

        cs.close()
        ctx.close()

        logger.info("=" * 60)
        logger.info("Script finished")
        logger.info("=" * 60)


if __name__ == "__main__":
    fetch_assets_from_seed()
