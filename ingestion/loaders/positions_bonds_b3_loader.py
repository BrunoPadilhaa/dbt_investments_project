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
RAW_TABLE = "RAW_POSITIONS_BONDS_B3"

# --- Folder containing Excel files ---
DATA_PATH = r"C:\Users\bruno\Documents\dbt_projects\dataset\Brazil\month_reports"

# --- Position sheets to append ---
SHEETS = ["Posição - Tesouro Direto"]


def load_b3_bond_positions(ctx):
    """Load B3 Brazil monthly consolidated report position sheets into Snowflake."""

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

                    available_sheets = pd.ExcelFile(full_path).sheet_names

                    for sheet in SHEETS:
                        if sheet not in available_sheets:
                            logger.warning(f"Sheet '{sheet}' not found in {filename}, skipping")
                            continue

                        df = pd.read_excel(full_path, sheet_name=sheet, dtype=str)

                        # --- Drop empty rows and the footer total rows (no Produto) ---
                        df = df.dropna(subset=['Produto'])

                        # --- Add tracking columns ---
                        df['SOURCE_FILE'] = filename
                        df['SOURCE_SYSTEM'] = 'b3_brazil'
                        df['LOAD_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                        # --- Rename columns to match Snowflake table schema ---
                        df = df.rename(columns={
                            'Produto': 'PRODUCT',
                            'Vencimento': 'MATURITY_DATE',
                            'Quantidade Disponível': 'AVAILABLE_QUANTITY',
                            'Valor Aplicado': 'INVESTED_VALUE_IN_BRL',
                            'Valor Atualizado': 'MARKET_VALUE_IN_BRL'
                        })

                        # --- Keep only relevant columns ---
                        df = df[['PRODUCT', 'MATURITY_DATE', 'AVAILABLE_QUANTITY',
                                 'INVESTED_VALUE_IN_BRL', 'MARKET_VALUE_IN_BRL',
                                 'SOURCE_FILE', 'SOURCE_SYSTEM', 'LOAD_TS']]

                        all_dfs.append(df)
                        logger.info(f"Loaded {len(df)} rows from {filename} [{sheet}]")

                except Exception as e:
                    logger.error(f"Error processing {filename}: {str(e)}")
                    continue

    if not all_dfs:
        logger.warning("No Excel files found to load!")
        return

    # --- Combine all sheets/files into a single DataFrame ---
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
    load_b3_bond_positions(ctx)
    ctx.close()
