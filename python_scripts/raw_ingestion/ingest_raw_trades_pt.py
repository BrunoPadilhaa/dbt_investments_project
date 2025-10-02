import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from datetime import datetime
import os

# --- Snowflake connection info ---
USER = os.environ['SNOWFLAKE_USER']
PWD = os.environ['SNOWFLAKE_PWD']
WH = os.environ['SNOWFLAKE_WH']
ACCOUNT = os.environ['SNOWFLAKE_ACCOUNT']
DATABASE = 'INVESTMENTS'
SCHEMA = 'RAW'
RAW_TABLE = 'RAW_TRADES_PT'

ctx = snowflake.connector.connect(
    user=USER,
    password=PWD,
    account=ACCOUNT,
    warehouse=WH,
    database=DATABASE,
    schema=SCHEMA
)
cs = ctx.cursor()

# --- Folder containing Excel files ---
file_path = r"C:\Users\bruno\Documents\dbt_projects\raw_trades"

# --- Fetch already loaded files from Snowflake ---
cs.execute(f"SELECT DISTINCT source_file FROM {SCHEMA}.{RAW_TABLE}")
loaded_files = set(row[0] for row in cs.fetchall())

all_dfs = []

# --- Walk through all subfolders and Excel files ---
for root, dirs, files in os.walk(file_path):
    for filename in files:
        if filename.lower().endswith(".xlsx") and filename not in loaded_files:
            full_path = os.path.join(root, filename)
            
            # Read only the relevant sheet, skip first 10 rows, all as string
            df = pd.read_excel(
                full_path,
                sheet_name="CASH OPERATION HISTORY",
                skiprows=10,
                dtype=str
            )
            
            # Drop unwanted empty or null columns
            df = df.drop(columns=['Unnamed: 0', 'Unnamed: 7'], errors='ignore')
            df = df.dropna(how='all')  # remove fully empty rows
            
            # Add tracking columns
            df['SOURCE_FILE'] = filename
            df['SOURCE_SYSTEM'] = 'xtb_portugal'  # explicitly set system
            df['LOAD_TS'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Rename columns to match Snowflake RAW_TRADES table
            df = df.rename(columns={
                'ID': 'ID',
                'Type': 'TYPE',
                'Time': 'TIME',
                'Comment': 'COMMENT',
                'Symbol': 'SYMBOL',
                'Amount': 'AMOUNT'
            })
            
            # Keep only columns that exist in RAW_TRADES
            df = df[['ID', 'TYPE', 'TIME', 'COMMENT',
                     'SYMBOL', 'AMOUNT', 'SOURCE_FILE', 'SOURCE_SYSTEM', 'LOAD_TS']]
            
            all_dfs.append(df)

# --- Combine and upload to Snowflake ---
if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print("Rows before dedup:", combined_df.shape[0])
    
    # Remove duplicates by ID, keep first occurrence
    combined_df = combined_df.drop_duplicates(subset=['ID'], keep='first')
    
    print("Rows after dedup:", combined_df.shape[0])
    
    success, nchunks, nrows, _ = write_pandas(
        conn=ctx,
        df=combined_df,
        table_name=RAW_TABLE,
        schema=SCHEMA,
        database=DATABASE
    )

    if success:
        print(f"Successfully loaded {nrows} rows into {RAW_TABLE}")
        
    else:
        print("Failed to load data into Snowflake")
else:
    print("No new Excel files to load!")
