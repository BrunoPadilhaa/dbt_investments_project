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
RAW_TRADES_TABLE = 'RAW_TRANSACTIONS_XTB'

# --- Excel/CSV file path ---
SEEDS_PATH = Path(r'C:\Users\bruno\Documents\dbt_projects\investments\seeds')
EXCEL_FILE = SEEDS_PATH / 'tickers_seed.xlsx'
CSV_FILE = SEEDS_PATH / 'tickers_seed.csv'

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

# --- Fetch distinct symbols from RAW_TRANSACTIONS_XTB ---
query = f"""
SELECT DISTINCT 
    SYMBOL
FROM {SCHEMA}.{RAW_TRADES_TABLE} 
WHERE SYMBOL IS NOT NULL
ORDER BY SYMBOL
"""

logger.info("Fetching distinct tickers from Snowflake...")
df_new_tickers = pd.read_sql(query, ctx)
logger.info(f"Found {len(df_new_tickers)} distinct tickers in RAW_TRANSACTIONS_XTB")

# --- Load existing Excel file if it exists ---
if EXCEL_FILE.exists():
    logger.info(f"Loading existing Excel file: {EXCEL_FILE}")
    df_existing = pd.read_excel(EXCEL_FILE)
    
    # Find only NEW tickers not in the existing file
    existing_symbols = set(df_existing['SYMBOL'].values)
    new_symbols = set(df_new_tickers['SYMBOL'].values)
    tickers_to_add = new_symbols - existing_symbols
    
    if tickers_to_add:
        logger.info(f"Found {len(tickers_to_add)} new tickers to add: {sorted(tickers_to_add)}")
        
        # Create DataFrame with new tickers only
        df_to_add = pd.DataFrame({'SYMBOL': sorted(list(tickers_to_add))})
        
        # Add placeholder columns for manual enrichment (customize as needed)
        df_to_add['YF_SUFFIX'] = None
        df_to_add['ASSET_TYPE'] = None
        df_to_add['COUNTRY'] = None
        df_to_add['NOTES'] = None
        
        # Append to existing data
        df_combined = pd.concat([df_existing, df_to_add], ignore_index=True)
        df_combined = df_combined.sort_values('SYMBOL').reset_index(drop=True)
        
        # Save to both Excel and CSV
        df_combined.to_excel(EXCEL_FILE, index=False)
        df_combined.to_csv(CSV_FILE, index=False)
        logger.info(f"✅ Added {len(tickers_to_add)} new tickers to {EXCEL_FILE} and {CSV_FILE}")
    else:
        logger.info("✅ No new tickers to add - files are up to date")
else:
    # First run - create files with all tickers
    logger.info(f"Creating new files in: {SEEDS_PATH}")
    
    df_export = df_new_tickers.copy()
    
    # Add placeholder columns for manual enrichment
    df_export['YF_SUFFIX'] = None
    df_export['ASSET_TYPE'] = None
    df_export['COUNTRY'] = None
    df_export['NOTES'] = None
    
    # Save to both Excel (for easy editing) and CSV (for dbt)
    df_export.to_excel(EXCEL_FILE, index=False)
    df_export.to_csv(CSV_FILE, index=False)
    logger.info(f"✅ Created {EXCEL_FILE} and {CSV_FILE} with {len(df_export)} tickers")

# --- Cleanup ---
ctx.close()
logger.info(f"Script completed successfully. Files saved to: {SEEDS_PATH}")