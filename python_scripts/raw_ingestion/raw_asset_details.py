import yfinance as yf
from datetime import datetime
import logging
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import requests
from typing import Optional, Dict
import pycountry
from dotenv import load_dotenv

# --- Load environment variables from .env file ---
load_dotenv('python_env.env')  # Specify your .env file name

# --- Configuration ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']

DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'

RAW_ASSET_DETAILS_TABLE = 'RAW_ASSET_DETAILS'
RAW_ASSET_SEED_TABLE = 'RAW_ASSET_SEED'

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Helper functions ---

def get_country_name(country_code: Optional[str]) -> Optional[str]:
    """Convert country code to country name using pycountry."""
    if not country_code:
        return None
    
    try:
        country = pycountry.countries.get(alpha_2=country_code.strip().upper())
        return country.name if country else None
    except Exception as e:
        logger.warning(f"⚠️ Could not resolve country code '{country_code}': {e}")
        return None

def is_valid_asset_code(symbol: str) -> bool:
    """Check if the asset code is valid on Yahoo Finance."""
    try:
        info = yf.Ticker(symbol).info
        return bool(info and (info.get("regularMarketPrice") or info.get("shortName")))
    except Exception:
        return False

def search_yahoo_api(symbol: str) -> Optional[str]:
    """Search Yahoo Finance API for asset symbol."""
    url = "https://query2.finance.yahoo.com/v1/finance/search"
    params = {"q": symbol, "quotesCount": 5, "newsCount": 0}

    try:
        response = requests.get(url, params=params, timeout=10)
        data = response.json()

        for quote in data.get("quotes", []):
            yf_symbol = quote.get("symbol")
            if yf_symbol and is_valid_asset_code(yf_symbol):
                logger.info(f"✅ Found via API: {symbol} -> {yf_symbol}")
                return yf_symbol

    except Exception as e:
        logger.warning(f"Yahoo API search failed for {symbol}: {e}")

    return None

def find_yahoo_asset_code(yahoo_candidate: str) -> Optional[str]:
    """Find and validate Yahoo Finance asset code."""
    if not yahoo_candidate:
        return None

    if is_valid_asset_code(yahoo_candidate):
        logger.info(f"✅ Valid asset code: {yahoo_candidate}")
        return yahoo_candidate

    # Try API search as fallback
    result = search_yahoo_api(yahoo_candidate)
    if result:
        return result
    
    logger.warning(f"⚠️ Could not validate: {yahoo_candidate}")
    return None

def build_yahoo_asset_code(asset_code_current: str, code_suffix: str) -> str:
    """
    Build Yahoo Finance asset code from seed metadata.
    Handles US assets and existing formatted codes.
    """
    if not asset_code_current:
        return None
    
    asset_code_current = asset_code_current.strip()
    code_suffix = code_suffix.strip() if code_suffix else ""
    
    # US assets have no suffix in Yahoo
    if not code_suffix or code_suffix.upper() == "US":
        if asset_code_current.upper().endswith(".US"):
            return asset_code_current[:-3]
        return asset_code_current
    
    # Already formatted (e.g., ENEL.IT, TTE.FR)
    if "." in asset_code_current:
        logger.info(f"Asset code already formatted: {asset_code_current}")
        return asset_code_current
    
    return f"{asset_code_current}.{code_suffix}"

def fetch_asset_info(asset_code: str, yf_asset_code: Optional[str], country_code: Optional[str]) -> Dict:
    """Fetch asset information from Yahoo Finance."""
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert country code to country name
    country_name = get_country_name(country_code)

    source_system = "yahoo_finance"

    record = {
        "ASSET_CODE": asset_code,
        "YF_ASSET_CODE": yf_asset_code,
        "COUNTRY": country_name,
        "SOURCE_SYSTEM": source_system,
        "LOAD_TS": now_ts
    }

    if not yf_asset_code:
        logger.error(f"❌ UNRESOLVED YAHOO ASSET CODE FOR {asset_code}")
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE"]:
            record[col] = None
        return record

    try:
        info = yf.Ticker(yf_asset_code).info

        record.update({
            "SHORTNAME": info.get("shortName"),
            "LONGNAME": info.get("longName"),
            "QUOTETYPE": info.get("quoteType"),
            "SECTOR": info.get("sector"),
            "INDUSTRY": info.get("industry"),
            "CURRENCY": info.get("currency"),
            "EXCHANGE": info.get("exchange")
        })

        logger.info(f"✅ Fetched info for {asset_code} -> {yf_asset_code} ({country_name})")

    except Exception as e:
        logger.error(f"❌ Failed fetching info for {asset_code}: {e}")
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE"]:
            record[col] = None

    return record

# --- Main execution ---

def main():
    logger.info("=" * 60)
    logger.info("Asset Details Update (Seed-Driven, Deterministic)")
    logger.info("=" * 60)

    ctx = snowflake.connector.connect(
        user=USER,
        password=PWD,
        account=ACCOUNT,
        warehouse=WH,
        database=DATABASE,
        schema=SCHEMA
    )
    cs = ctx.cursor()

    try:
        logger.info(f"Truncating {SCHEMA}.{RAW_ASSET_DETAILS_TABLE}")
        cs.execute(f"TRUNCATE TABLE IF EXISTS {SCHEMA}.{RAW_ASSET_DETAILS_TABLE}")

        cs.execute(f"""
            SELECT DISTINCT
                ASSET_CODE,
                ASSET_CODE_CURRENT,
                CODE_SUFFIX,
                COUNTRY_CODE
            FROM {SCHEMA}.{RAW_ASSET_SEED_TABLE}
            WHERE ASSET_CODE_CURRENT IS NOT NULL
        """)

        rows = cs.fetchall()
        logger.info(f"Found {len(rows)} assets to process")

        all_data = []

        for asset_code, asset_code_current, code_suffix, country_code in rows:
            yahoo_candidate = build_yahoo_asset_code(asset_code_current, code_suffix)
            yf_asset_code = find_yahoo_asset_code(yahoo_candidate)

            record = fetch_asset_info(
                asset_code=asset_code,
                yf_asset_code=yf_asset_code,
                country_code=country_code
            )

            all_data.append(record)

        if all_data:
            df = pd.DataFrame(all_data)

            logger.info(f"Loading {len(df)} rows into {SCHEMA}.{RAW_ASSET_DETAILS_TABLE}")
            success, _, nrows, _ = write_pandas(
                ctx,
                df,
                RAW_ASSET_DETAILS_TABLE,
                schema=SCHEMA
            )

            if success:
                logger.info(f"✅ Loaded {nrows} rows into {RAW_ASSET_DETAILS_TABLE}")
            else:
                logger.error("❌ Failed to load data")

    finally:
        cs.close()
        ctx.close()
        logger.info("=" * 60)
        logger.info("Script finished")
        logger.info("=" * 60)


if __name__ == "__main__":
    main()