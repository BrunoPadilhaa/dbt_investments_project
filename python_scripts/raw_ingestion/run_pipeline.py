"""
Simple pipeline orchestrator - runs all raw data loaders in sequence.
"""

import subprocess
import sys
from pathlib import Path

# Script directory
SCRIPT_DIR = Path(__file__).parent

# Loaders in order
LOADERS = [
    "ingest_raw_trades_pt.py",
    "ingest_stock_country_mapping.py",
    "ingest_raw_symbol.py",
    "ingest_raw_stock_prices.py",
    "ingest_raw_exchange_rates.py",
]

def run_script(script_name):
    """Run a Python script and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {script_name}")
    print('='*60)
    
    result = subprocess.run(
        ["python", SCRIPT_DIR / script_name],
        cwd=SCRIPT_DIR
    )
    
    return result.returncode == 0

def main():
    print("🚀 Starting pipeline...")
    
    # Run each loader
    for script in LOADERS:
        if not run_script(script):
            print(f"\n❌ {script} failed. Stopping pipeline.")
            sys.exit(1)
    
    # Run dbt
    print(f"\n{'='*60}")
    print("Running dbt...")
    print('='*60)
    
    result = subprocess.run(["dbt", "run"])
    
    if result.returncode == 0:
        print("\n✅ Pipeline completed successfully!")
    else:
        print("\n❌ dbt failed")
        sys.exit(1)

if __name__ == "__main__":
    main()