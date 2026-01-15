import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os
import warnings

# Suppress openpyxl warnings about default styles
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# --- Snowflake connection info ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'
RAW_TABLE = 'RAW_TRANSACTIONS_CLEAR'

ctx = snowflake.connector.connect(
    user=USER,
    password=PWD,
    account=ACCOUNT,
    warehouse=WH,
    database=DATABASE,
    schema=SCHEMA
)
cs = ctx.cursor()

# --- TRUNCATE TABLE BEFORE LOADING ---
print(f"Truncating table {RAW_TABLE}...")
cs.execute(f"TRUNCATE TABLE {DATABASE}.{SCHEMA}.{RAW_TABLE}")
print(f"Table {RAW_TABLE} truncated successfully")

# --- Folder containing Excel files ---
file_path = r"C:\Users\bruno\Documents\dbt_projects\dataset\brazil"

# --- Fetch all Excel files ---
all_dfs = []

# --- Walk through all subfolders and Excel files ---
for root, dirs, files in os.walk(file_path):
    for filename in files:
        if filename.lower().endswith((".xlsx", ".xls")):
            full_path = os.path.join(root, filename)
            
            print(f"Processing file: {filename}")
            
            try:
                # Read Excel file - adjust sheet_name if needed (None = first sheet)
                df = pd.read_excel(
                    full_path,
                    sheet_name=0,  # First sheet - adjust if needed
                    dtype=str  # Read all as string initially
                )
                
                # Drop fully empty rows and columns
                df = df.dropna(how='all')
                df = df.dropna(axis=1, how='all')
                
                # Drop any unnamed columns (like 'Unnamed: 0', 'Unnamed: 1', etc.)
                unnamed_cols = [col for col in df.columns if 'Unnamed' in str(col)]
                df = df.drop(columns=unnamed_cols, errors='ignore')
                
                print(f"  -> Columns found: {list(df.columns)}")
                
                # Skip files that don't have the expected columns
                expected_cols = ['Entrada/Saída', 'Data', 'Movimentação', 'Produto', 
                                'Instituição', 'Quantidade', 'Preço unitário', 'Valor da Operação']
                if not all(col in df.columns for col in expected_cols):
                    print(f"  -> Skipping {filename}: Missing expected columns")
                    continue
                
                # Add tracking columns
                df['SOURCE_FILE'] = filename
                df['SOURCE_SYSTEM'] = 'xp_investimentos'
                df['LOAD_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                all_dfs.append(df)
                print(f"  -> Loaded {len(df)} rows from {filename}")
                
            except Exception as e:
                print(f"  -> Error processing {filename}: {str(e)}")
                continue

# --- Combine and upload to Snowflake ---
if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nTotal rows to load: {combined_df.shape[0]}")
    
    # Reset index to avoid pandas warning
    combined_df = combined_df.reset_index(drop=True)
    
    # Upload to Snowflake
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx,
        df=combined_df,
        table_name=RAW_TABLE,
        schema=SCHEMA,
        database=DATABASE
    )
    
    if success:
        print(f"\n✓ Successfully loaded {nrows} rows into {RAW_TABLE}")
    else:
        print("\n✗ Failed to load data into Snowflake")
else:
    print("\nNo Excel files found or processed!")

# Close connection
cs.close()
ctx.close()