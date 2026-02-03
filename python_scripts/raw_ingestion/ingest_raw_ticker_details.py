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

RAW_TICKER_DETAILS_TABLE = 'RAW_TICKER_DETAILS'
RAW_TICKER_SEED_TABLE = 'RAW_TICKER_SEED'

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

def is_valid_ticker(symbol: str) -> bool:
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
            if yf_symbol and is_valid_ticker(yf_symbol):
                logger.info(f"✅ Found via API: {symbol} -> {yf_symbol}")
                return yf_symbol

    except Exception as e:
        logger.warning(f"Yahoo API search failed for {symbol}: {e}")

    return None

def find_yahoo_ticker(yahoo_candidate: str) -> Optional[str]:
    if not yahoo_candidate:
        return None

    if is_valid_ticker(yahoo_candidate):
        logger.info(f"✅ Valid ticker: {yahoo_candidate}")
        return yahoo_candidate

    # Try API search as fallback
    result = search_yahoo_api(yahoo_candidate)
    if result:
        return result
    
    logger.warning(f"⚠️ Could not validate: {yahoo_candidate}")
    return None

def build_yahoo_ticker(current_ticker: str, exchange_suffix: str) -> str:
    """
    Build Yahoo Finance ticker from seed metadata.
    Handles US tickers and existing formatted tickers.
    """
    if not current_ticker:
        return None
    
    current_ticker = current_ticker.strip()
    exchange_suffix = exchange_suffix.strip() if exchange_suffix else ""
    
    # US tickers have no suffix in Yahoo
    if not exchange_suffix or exchange_suffix.upper() == "US":
        if current_ticker.upper().endswith(".US"):
            return current_ticker[:-3]
        return current_ticker
    
    # Already formatted (e.g., ENEL.IT, TTE.FR)
    if "." in current_ticker:
        logger.info(f"Ticker already formatted: {current_ticker}")
        return current_ticker
    
    return f"{current_ticker}.{exchange_suffix}"

def fetch_ticker_info(original_symbol: str, yf_symbol: Optional[str], country_code: Optional[str]) -> Dict:
    now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert country code to country name
    country_name = get_country_name(country_code)

    record = {
        "TICKER": original_symbol,
        "YF_TICKER": yf_symbol,
        "COUNTRY": country_name,
        "LOAD_TS": now_ts
    }

    if not yf_symbol:
        logger.error(f"❌ UNRESOLVED YAHOO TICKER FOR {original_symbol}")
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE"]:
            record[col] = None
        return record

    try:
        info = yf.Ticker(yf_symbol).info

        record.update({
            "SHORTNAME": info.get("shortName"),
            "LONGNAME": info.get("longName"),
            "QUOTETYPE": info.get("quoteType"),
            "SECTOR": info.get("sector"),
            "INDUSTRY": info.get("industry"),
            "CURRENCY": info.get("currency"),
            "EXCHANGE": info.get("exchange")
        })

        logger.info(f"✅ Fetched info for {original_symbol} -> {yf_symbol} ({country_name})")

    except Exception as e:
        logger.error(f"❌ Failed fetching info for {original_symbol}: {e}")
        for col in ["SHORTNAME", "LONGNAME", "QUOTETYPE", "SECTOR", "INDUSTRY", "CURRENCY", "EXCHANGE"]:
            record[col] = None

    return record

# --- Main execution ---

def main():
    logger.info("=" * 60)
    logger.info("Ticker Details Update (Seed-Driven, Deterministic)")
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
        logger.info(f"Truncating {SCHEMA}.{RAW_TICKER_DETAILS_TABLE}")
        cs.execute(f"TRUNCATE TABLE IF EXISTS {SCHEMA}.{RAW_TICKER_DETAILS_TABLE}")

        cs.execute(f"""
            SELECT DISTINCT
                SYMBOL,
                CURRENT_TICKER,
                EXCHANGE_SUFFIX,
                COUNTRY_CODE
            FROM {SCHEMA}.{RAW_TICKER_SEED_TABLE}
            WHERE CURRENT_TICKER IS NOT NULL
        """)

        rows = cs.fetchall()
        logger.info(f"Found {len(rows)} tickers to process")

        all_data = []

        for original_symbol, current_ticker, exchange_suffix, country_code in rows:
            yahoo_candidate = build_yahoo_ticker(current_ticker, exchange_suffix)
            yf_symbol = find_yahoo_ticker(yahoo_candidate)

            record = fetch_ticker_info(
                original_symbol=original_symbol,
                yf_symbol=yf_symbol,
                country_code=country_code
            )

            all_data.append(record)

        if all_data:
            df = pd.DataFrame(all_data)

            logger.info(f"Loading {len(df)} rows into {SCHEMA}.{RAW_TICKER_DETAILS_TABLE}")
            success, _, nrows, _ = write_pandas(
                ctx,
                df,
                RAW_TICKER_DETAILS_TABLE,
                schema=SCHEMA
            )

            if success:
                logger.info(f"✅ Loaded {nrows} rows into {RAW_TICKER_DETAILS_TABLE}")
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