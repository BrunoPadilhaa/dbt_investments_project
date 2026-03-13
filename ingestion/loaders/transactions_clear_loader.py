import pandas as pd
from datetime import datetime
import logging
import os
import warnings

from snowflake.connector.pandas_tools import write_pandas
from ingestion.snowflake_connection import get_connection  # shared connector

# --- Logging setup ---
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Suppress openpyxl warnings about default styles
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- Table ---
RAW_TABLE = "RAW_TRANSACTIONS_CLEAR"

# --- Folder containing Excel files ---
DATA_PATH = r"C:\Users\bruno\Documents\dbt_projects\dataset\brazil"

# --- Expected columns ---
EXPECTED_COLS = [
    'Entrada/Saída', 'Data', 'Movimentação', 'Produto', 
    'Instituição', 'Quantidade', 'Preço unitário', 'Valor da Operação'
]


def load_clear_transactions():
    """Load Excel transaction files into Snowflake."""
    ctx = get_connection()
    cs = ctx.cursor()

    # --- TRUNCATE TABLE BEFORE LOADING ---
    logger.info(f"Truncating table {RAW_TABLE}...")
    cs.execute(f"TRUNCATE TABLE RAW.{RAW_TABLE}")
    logger.info(f"Table {RAW_TABLE} truncated successfully")

    all_dfs = []

    # --- Walk through all subfolders and Excel files ---
    for root, dirs, files in os.walk(DATA_PATH):
        for filename in files:
            if filename.lower().endswith((".xlsx", ".xls")):
                full_path = os.path.join(root, filename)
                logger.info(f"Processing file: {filename}")

                try:
                    df = pd.read_excel(full_path, sheet_name=0, dtype=str)
                    df = df.dropna(how='all').dropna(axis=1, how='all')
                    unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
                    df = df.drop(columns=unnamed_cols, errors='ignore')

                    if not all(col in df.columns for col in EXPECTED_COLS):
                        logger.warning(f"Skipping {filename}: Missing expected columns")
                        continue

                    # Add tracking columns
                    df['SOURCE_FILE'] = filename
                    df['SOURCE_SYSTEM'] = 'xp_investimentos'
                    df['LOAD_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    all_dfs.append(df)
                    logger.info(f"Loaded {len(df)} rows from {filename}")

                except Exception as e:
                    logger.error(f"Error processing {filename}: {str(e)}")
                    continue

    # --- Combine and upload to Snowflake ---
    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True).reset_index(drop=True)
        logger.info(f"Total rows to load: {combined_df.shape[0]}")

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
    else:
        logger.warning("No Excel files found or processed!")

    cs.close()
    ctx.close()