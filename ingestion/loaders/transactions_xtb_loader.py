import pandas as pd
from datetime import datetime
import logging
import os

from snowflake.connector.pandas_tools import write_pandas
from ingestion.snowflake_connection import get_connection

# --- Logging setup ---
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Table ---
RAW_TABLE = "RAW_TRANSACTIONS_XTB"

# --- Folder containing Excel files ---
DATA_PATH = r"C:\Users\bruno\Documents\dbt_projects\dataset\Portugal\xtb"


def load_xtb_transactions(ctx):
    """Load XTB Portugal Excel transaction files into Snowflake."""

    cs = ctx.cursor()

    # --- Truncate table before loading to ensure clean state ---
    logger.info(f"Truncating table {RAW_TABLE}...")
    cs.execute(f"TRUNCATE TABLE RAW.{RAW_TABLE}")
    logger.info(f"Table {RAW_TABLE} truncated successfully")

    all_dfs = []

    # --- Walk through all Excel files in the data path ---
    for root, dirs, files in os.walk(DATA_PATH):
        for filename in files:
            if filename.lower().endswith(".xlsx"):
                full_path = os.path.join(root, filename)
                try:
                    logger.info(f"Processing file: {filename}")

                    df = pd.read_excel(
                        full_path,
                        sheet_name="CASH OPERATION HISTORY",
                        skiprows=10,
                        dtype=str
                    )

                    # --- Drop empty/unnamed columns and rows ---
                    df = df.drop(columns=['Unnamed: 0', 'Unnamed: 7'], errors='ignore')
                    df = df.dropna(how='all')

                    # --- Add tracking columns ---
                    df['SOURCE_FILE'] = filename
                    df['SOURCE_SYSTEM'] = 'xtb_portugal'
                    df['LOAD_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # --- Rename columns to match Snowflake table schema ---
                    df = df.rename(columns={
                        'ID': 'ID',
                        'Type': 'TYPE',
                        'Time': 'TIME',
                        'Comment': 'COMMENT',
                        'Symbol': 'SYMBOL',
                        'Amount': 'AMOUNT'
                    })

                    # --- Keep only relevant columns ---
                    df = df[['ID', 'TYPE', 'TIME', 'COMMENT', 'SYMBOL',
                             'AMOUNT', 'SOURCE_FILE', 'SOURCE_SYSTEM', 'LOAD_TS']]

                    all_dfs.append(df)
                    logger.info(f"Loaded {len(df)} rows from {filename}")

                except Exception as e:
                    logger.error(f"Error processing {filename}: {str(e)}")
                    continue

    if not all_dfs:
        logger.warning("No Excel files found to load!")
        return

    # --- Combine all files into a single DataFrame ---
    combined_df = pd.concat(all_dfs, ignore_index=True).reset_index(drop=True)
    logger.info(f"Rows before dedup: {combined_df.shape[0]}")

    # --- Deduplicate by ID ---
    combined_df = combined_df.drop_duplicates(subset=['ID'], keep='first')
    logger.info(f"Rows after dedup: {combined_df.shape[0]}")

    # --- Write to Snowflake ---
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx,
        df=combined_df,
        table_name=RAW_TABLE,
        schema="RAW"
    )

    if success:
        logger.info(f"✅ Successfully loaded {nrows} rows into {RAW_TABLE}")
    else:
        logger.error("❌ Failed to load data into Snowflake")

    cs.close()


if __name__ == "__main__":
    ctx = get_connection()
    load_xtb_transactions(ctx)
    ctx.close()