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
RAW_TABLE = "RAW_TRADES_B3"

# --- Folder containing Excel files ---
DATA_PATH = r"C:\Users\bruno\Documents\dbt_projects\dataset\Brazil\buys_and_sell_data"

# --- Trades sheet ---
SHEET = "Negociação"


def load_b3_trades(ctx):
    """Load B3 Brazil buy/sell trade history Excel files into Snowflake."""

    cs = ctx.cursor()

    # --- Truncate table before loading to ensure clean state ---
    logger.info(f"Truncating table {RAW_TABLE}...")
    cs.execute(f"TRUNCATE TABLE RAW.{RAW_TABLE}")
    logger.info(f"Table {RAW_TABLE} truncated successfully")

    all_dfs = []

    # --- Walk through all Excel files in the data path ---
    for root, dirs, files in os.walk(DATA_PATH):
        for filename in files:
            # --- Skip Excel lock files (~$file.xlsx) created while a file is open ---
            if filename.lower().endswith(".xlsx") and not filename.startswith("~$"):
                full_path = os.path.join(root, filename)
                try:
                    logger.info(f"Processing file: {filename}")

                    df = pd.read_excel(full_path, sheet_name=SHEET, dtype=str)

                    # --- Drop empty rows ---
                    df = df.dropna(how='all')

                    # --- Add tracking columns ---
                    df['SOURCE_FILE'] = filename
                    df['SOURCE_SYSTEM'] = 'b3_brazil'
                    df['LOAD_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    # --- Rename columns to match Snowflake table schema ---
                    df = df.rename(columns={
                        'Data do Negócio': 'TRADE_DATE',
                        'Tipo de Movimentação': 'TRANSACTION_TYPE',
                        'Mercado': 'MARKET',
                        'Prazo/Vencimento': 'MATURITY_DATE',
                        'Instituição': 'INSTITUTION',
                        'Código de Negociação': 'ASSET_CODE',
                        'Quantidade': 'QUANTITY',
                        'Preço': 'PRICE',
                        'Valor': 'AMOUNT'
                    })

                    # --- Keep only relevant columns ---
                    df = df[['TRADE_DATE', 'TRANSACTION_TYPE', 'MARKET', 'MATURITY_DATE',
                             'INSTITUTION', 'ASSET_CODE', 'QUANTITY', 'PRICE', 'AMOUNT',
                             'SOURCE_FILE', 'SOURCE_SYSTEM', 'LOAD_TS']]

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
    logger.info(f"Total rows to load: {combined_df.shape[0]}")

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
    load_b3_trades(ctx)
    ctx.close()
