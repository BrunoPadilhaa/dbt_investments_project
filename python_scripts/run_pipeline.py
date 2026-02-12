"""
Simple pipeline orchestrator - runs all raw data loaders in sequence.
"""
import os
import subprocess
import sys
import warnings
from pathlib import Path

# Suppress pandas warnings globally
warnings.filterwarnings('ignore', category=UserWarning)

# Script directory
SCRIPT_DIR = Path(__file__).parent / "raw_ingestion"

# Loaders in order
LOADERS = [
    "raw_exchange_rates.py",
    "raw_stock_prices.py",
    "ingest_raw_transactions_clear.py",
    "raw_transactions_xtb.py",
    "raw_ticker_details.py",
    "country_mapping.py"

]

def run_script(script_name):
    """Run a Python script and return success status."""
    print(f"\n{'='*60}")
    print(f"▶️  Running: {script_name}")
    print('='*60)
    
    # Use sys.executable to run with the SAME Python as this script
    result = subprocess.run(
        [sys.executable, SCRIPT_DIR / script_name],
        cwd=SCRIPT_DIR,
        env={**os.environ, 'PYTHONWARNINGS': 'ignore'}  # Suppress warnings in subprocess too
    )
    
    if result.returncode == 0:
        print(f"✅ {script_name} completed")
    else:
        print(f"❌ {script_name} failed")
    
    return result.returncode == 0

def main():
    print("\n" + "="*60)
    print("🚀 STARTING DATA PIPELINE")
    print("="*60)
    print(f"Python: {sys.executable}\n")
    
    success_count = 0
    
    # Run each loader
    for i, script in enumerate(LOADERS, 1):
        print(f"\n[{i}/{len(LOADERS)}]", end=" ")
        if not run_script(script):
            print(f"\n❌ Pipeline stopped due to failure in {script}")
            sys.exit(1)
        success_count += 1
    
    # Run dbt
    print(f"\n{'='*60}")
    print("🔧 Running dbt transformations...")
    print('='*60)
    
    result = subprocess.run(["dbt", "build"])
    
    # Summary
    print("\n" + "="*60)
    if result.returncode == 0:
        print("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        print(f"   • {success_count}/{len(LOADERS)} loaders completed")
        print(f"   • dbt transformations completed")
    else:
        print("❌ PIPELINE FAILED - dbt transformations failed")
    print("="*60 + "\n")
    
    sys.exit(0 if result.returncode == 0 else 1)

if __name__ == "__main__":
    main()