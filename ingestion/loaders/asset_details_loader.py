import yfinance as yf
from datetime import datetime
import logging
import pandas as pd
from typing import Optional, Dict
import requests

from ingestion.snowflake_connection import get_connection  # <- use shared connector

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Tables ---
RAW_ASSET_DETAILS_TABLE = 'RAW_ASSET_DETAILS'
RAW_ASSET_SEED_TABLE = 'RAW_ASSET_SEED'

# --- Helper functions ---

def is_valid_asset_code(symbol: str) -> bool:
    try:
        info = yf.Ticker(symbol).info
        return bool(info and (info.get("regularMarketPrice") or info.get("shortName")))
    except Exception:
        return False

def search_yahoo_api(symbol: str) -> Optional[str]:
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
    if not yahoo_candidate:
        return None
    if is_valid_asset_code(yahoo_candidate):
        logger.info(f"✅ Valid asset code: {yahoo_candidate}")
        return yahoo_candidate
    # fallback to API
    result = search_yahoo_api(yahoo_candidate)
    if result:
        return result
    logger.warning(f"⚠️ Could not validate: {yahoo_candidate}")
    return None

def build_yahoo_asset_code(asset_code_current: str, code_suffix: str) -> Optional[str]:
    if not asset_code_current:
        return None
    asset_code_current = asset_code_current.strip()
    code_suffix = code_suffix.strip() if code_suffix else ""
    if not code_suffix or code_suffix.upper() == "US":
        return asset_code_current[:-3] if asset_code_current.upper().endswith(".US") else asset_code_current
    if "." in asset_code_current:
        logger.info(f"Asset code already formatted: {asset_code_current}")
        return asset_code_current
    return f"{asset_code_current}.{code_suffix}"

def load_asset_info(asset_code: str, yf_asset_code: Optional[str]) -> Dict:
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_system = "yahoo_finance"
    record = {
        "ASSET_CODE": asset_code,
        "YF_ASSET_CODE": yf_asset_code,
        "SOURCE_SYSTEM": source_system,
        "LOAD_TS": now_ts
    }

    if not yf_asset_code:
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE", "COUNTRY"]:
            record[col] = None
        logger.error(f"❌ UNRESOLVED YAHOO ASSET CODE FOR {asset_code}")
        return record

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
        logger.info(f"✅ Fetched info for {asset_code} -> {yf_asset_code} (Country: {country}, Type: {info.get('quoteType')})")
    except Exception as e:
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE", "COUNTRY"]:
            record[col] = None
        logger.error(f"❌ Failed fetching info for {asset_code}: {e}")

    return record

# --- Main execution ---

def fetch_assets_from_seed():
    logger.info("=" * 60)
    logger.info("Asset Details Update (Seed-Driven, Deterministic)")
    logger.info("=" * 60)

    ctx = get_connection()
    cs = ctx.cursor()

    try:
        logger.info(f"Truncating {RAW_ASSET_DETAILS_TABLE}")
        cs.execute(f"TRUNCATE TABLE IF EXISTS RAW.{RAW_ASSET_DETAILS_TABLE}")

        cs.execute(f"""
            SELECT DISTINCT
                ASSET_CODE,
                ASSET_CODE_CURRENT,
                CODE_SUFFIX
            FROM RAW.{RAW_ASSET_SEED_TABLE}
            WHERE ASSET_CODE_CURRENT IS NOT NULL
        """)
        rows = cs.fetchall()
        logger.info(f"Found {len(rows)} assets to process")

        all_data = []
        for asset_code, asset_code_current, code_suffix in rows:
            yahoo_candidate = build_yahoo_asset_code(asset_code_current, code_suffix)
            yf_asset_code = find_yahoo_asset_code(yahoo_candidate)
            record = load_asset_info(asset_code, yf_asset_code)
            all_data.append(record)

        if all_data:
            df = pd.DataFrame(all_data)
            from snowflake.connector.pandas_tools import write_pandas
            success, _, nrows, _ = write_pandas(ctx, df, RAW_ASSET_DETAILS_TABLE, schema="RAW")
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
    fetch_assets_from_seed()