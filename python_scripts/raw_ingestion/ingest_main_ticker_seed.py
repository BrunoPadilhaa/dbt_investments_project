from datetime import datetime
import pandas as pd
import snowflake.connector
import os
import logging
from pathlib import Path

# --- Snowflake connection info ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'
RAW_TRADES_XTB_TABLE = 'RAW_TRANSACTIONS_XTB'
RAW_TRADES_CLEAR_TABLE = 'RAW_TRANSACTIONS_CLEAR'

# --- Excel/CSV file path ---
SEEDS_PATH = Path(r'C:\Users\bruno\Documents\dbt_projects\investments\seeds')
EXCEL_FILE = SEEDS_PATH / 'raw_ticker_seed.xlsx'
CSV_FILE = SEEDS_PATH / 'raw_ticker_seed.csv'

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Ensure seeds directory exists ---
SEEDS_PATH.mkdir(parents=True, exist_ok=True)

# --- Connect to Snowflake ---
ctx = snowflake.connector.connect(
    user=USER,
    password=PWD,
    account=ACCOUNT,
    warehouse=WH,
    database=DATABASE,
    schema=SCHEMA
)

# --- Fetch distinct symbols from BOTH tables ---
query = f"""
SELECT DISTINCT SYMBOL
FROM (
    -- Tickers from XTB
    SELECT DISTINCT SYMBOL
    FROM {SCHEMA}.{RAW_TRADES_XTB_TABLE} 
    WHERE SYMBOL IS NOT NULL
    
    UNION
    
    -- Tickers from CLEAR
    SELECT DISTINCT SPLIT_PART("Produto", '-', 1) AS SYMBOL
    FROM {SCHEMA}.{RAW_TRADES_CLEAR_TABLE}
    WHERE "Produto" IS NOT NULL
)
ORDER BY SYMBOL
"""

logger.info("Fetching distinct tickers from both XTB and CLEAR tables...")
df_new_tickers = pd.read_sql(query, ctx)

# Clean the symbols - strip whitespace and convert to uppercase for comparison
df_new_tickers['SYMBOL'] = df_new_tickers['SYMBOL'].str.strip().str.upper()
df_new_tickers = df_new_tickers.drop_duplicates(subset=['SYMBOL'])

logger.info(f"Found {len(df_new_tickers)} distinct tickers across both sources")

# --- Load existing Excel file if it exists ---
if EXCEL_FILE.exists():
    logger.info(f"Loading existing Excel file: {EXCEL_FILE}")
    df_existing = pd.read_excel(EXCEL_FILE)
    
    # Clean existing symbols too
    df_existing['SYMBOL'] = df_existing['SYMBOL'].str.strip().str.upper()
    df_existing = df_existing.drop_duplicates(subset=['SYMBOL'])
    
    logger.info(f"Existing file has {len(df_existing)} rows and {len(df_existing.columns)} columns")
    logger.info(f"Existing columns: {list(df_existing.columns)}")
    
    # Find only NEW tickers not in the existing file (case-insensitive comparison)
    existing_symbols = set(df_existing['SYMBOL'].values)
    new_symbols = set(df_new_tickers['SYMBOL'].values)
    tickers_to_add = new_symbols - existing_symbols
    
    if tickers_to_add:
        logger.info(f"Found {len(tickers_to_add)} new tickers to add: {sorted(tickers_to_add)}")
        
        # Create DataFrame with new tickers only
        df_to_add = pd.DataFrame({'SYMBOL': sorted(list(tickers_to_add))})
        
        # Add empty values for ALL existing columns (preserves your manual fields)
        for col in df_existing.columns:
            if col not in df_to_add.columns:
                df_to_add[col] = None
        
        # Reorder columns to match existing file
        df_to_add = df_to_add[df_existing.columns]
        
        # Append to existing data (preserves all your manual enrichment)
        df_combined = pd.concat([df_existing, df_to_add], ignore_index=True)
        df_combined = df_combined.sort_values('SYMBOL').reset_index(drop=True)
        
        # Save to both Excel and CSV
        df_combined.to_excel(EXCEL_FILE, index=False)
        df_combined.to_csv(CSV_FILE, index=False)
        logger.info(f"✅ Added {len(tickers_to_add)} new tickers to {EXCEL_FILE} and {CSV_FILE}")
        logger.info(f"✅ Preserved all {len(df_existing.columns)} existing columns with your manual data")
    else:
        logger.info("✅ No new tickers to add - files are up to date")
        logger.info("✅ Your manual enrichment data is safe")
        
        # Even if no new tickers, save the cleaned version
        df_existing = df_existing.sort_values('SYMBOL').reset_index(drop=True)
        df_existing.to_excel(EXCEL_FILE, index=False)
        df_existing.to_csv(CSV_FILE, index=False)
        logger.info("✅ Cleaned and saved existing data (removed duplicates/whitespace)")
else:
    # First run - create files with all tickers and suggested columns
    logger.info(f"Creating new files in: {SEEDS_PATH}")
    
    df_export = df_new_tickers.copy()
    
    # Add suggested columns for classification
    df_export['ASSET_CLASS'] = None  # Stock, ETF, REIT, etc.
    df_export['STYLE'] = None  # Growth, Value, Blend, Defensive
    df_export['SECTOR_TYPE'] = None  # Technology, Healthcare, Real Estate, etc.
    df_export['GEOGRAPHY'] = None  # US, International, Emerging Markets
    df_export['NOTES'] = None  # Your custom notes
    
    # Save to both Excel (for easy editing) and CSV (for dbt)
    df_export.to_excel(EXCEL_FILE, index=False)
    df_export.to_csv(CSV_FILE, index=False)
    logger.info(f"✅ Created {EXCEL_FILE} and {CSV_FILE} with {len(df_export)} tickers")
    logger.info(f"✅ Added columns: ASSET_CLASS, STYLE, SECTOR_TYPE, GEOGRAPHY, NOTES")

# --- Cleanup ---
ctx.close()
logger.info(f"Script completed successfully. Files saved to: {SEEDS_PATH}")