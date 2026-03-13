"""
Pipeline orchestrator - runs all raw data loaders in sequence.
"""
import sys
import logging
from pathlib import Path
from ingestion.snowflake_connection import get_connection
import subprocess
import inspect


# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- Import loaders ---
from ingestion.loaders.exchange_rates_loader import load_exchange_rates
from ingestion.loaders.asset_prices_loader import load_asset_prices
from ingestion.loaders.transactions_clear_loader import load_clear_transactions
from ingestion.loaders.transactions_xtb_loader import load_xtb_transactions
from ingestion.loaders.asset_details_loader import fetch_assets_from_seed


# --- Loader list ---
LOADERS = [
    ("Exchange Rates", load_exchange_rates),
    ("Asset Prices", load_asset_prices),
    ("Transactions XP", load_clear_transactions),
    ("Transactions XTB", load_xtb_transactions),
   ("Asset Details", fetch_assets_from_seed)
]


def run_loader(name, func, ctx):
    """Run a single loader function, passing ctx if it accepts it."""
    logger.info(f"▶️  Running loader: {name}")
    try:
        sig = inspect.signature(func)
        if "ctx" in sig.parameters:
            func(ctx)
        else:
            func()
        logger.info(f"✅ {name} loader completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ {name} loader failed: {e}", exc_info=True)
        return False


def main():
    logger.info("="*60)
    logger.info("🚀 STARTING DATA PIPELINE")
    logger.info("="*60)

    # --- Create Snowflake connection ---
    ctx = get_connection()
    logger.info("Connected to Snowflake")

    # --- Run loaders ---
    for i, (name, func) in enumerate(LOADERS, 1):
        logger.info(f"[{i}/{len(LOADERS)}]")
        if not run_loader(name, func, ctx):
            ctx.close()
            logger.error("Pipeline stopped due to loader failure")
            sys.exit(1)

    # --- Close Snowflake connection ---
    ctx.close()
    logger.info("Snowflake connection closed")

    # --- Run dbt ---
    logger.info("="*60)
    logger.info("🔧 Running dbt transformations...")
    logger.info("="*60)

    result = subprocess.run(["dbt", "build"])
    if result.returncode == 0:
        logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY!")
        sys.exit(0)
    else:
        logger.error("❌ PIPELINE FAILED - dbt transformations failed")
        sys.exit(1)


if __name__ == "__main__":
    main()