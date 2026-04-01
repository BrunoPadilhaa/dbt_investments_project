"""
Pipeline orchestrator - runs all raw data loaders in sequence.
"""
import sys
import logging
import inspect
import subprocess

from ingestion.snowflake_connection import get_connection
from ingestion.loaders.exchange_rates_loader import load_exchange_rates
from ingestion.loaders.asset_prices_loader import load_asset_prices
from ingestion.loaders.transactions_clear_loader import load_clear_transactions
from ingestion.loaders.transactions_xtb_loader import load_xtb_transactions
from ingestion.loaders.asset_details_loader import fetch_assets_from_seed


# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# --- Loader registry ---
# Add or remove loaders here. Order matters — loaders run sequentially.
LOADERS = [
    ("Exchange Rates",   load_exchange_rates),
    ("Asset Prices",     load_asset_prices),
    ("Transactions XP",  load_clear_transactions),
    ("Transactions XTB", load_xtb_transactions),
    ("Asset Details",    fetch_assets_from_seed),
]


def run_dbt_seed():
    """
    Runs `dbt seed` to populate seed tables in Snowflake.
    Must run before loaders, as some loaders (e.g. fetch_assets_from_seed)
    read directly from seed tables.
    """
    logger.info("="*60)
    logger.info("🌱 Running dbt seeds...")
    logger.info("="*60)

    result = subprocess.run(["dbt", "seed"])

    if result.returncode != 0:
        logger.error("❌ dbt seed failed")
        sys.exit(1)

    logger.info("✅ dbt seed completed successfully")


def run_dbt_run():
    """
    Runs `dbt run` to execute all dbt models.
    Runs separately from seeds to avoid the race condition in `dbt build`
    where seeds and models execute concurrently — causing models to join
    against empty seed tables.
    """
    logger.info("="*60)
    logger.info("🔧 Running dbt models...")
    logger.info("="*60)

    result = subprocess.run(["dbt", "run"])

    if result.returncode != 0:
        logger.error("❌ dbt run failed")
        sys.exit(1)

    logger.info("✅ dbt run completed successfully")


def run_dbt_test():
    """
    Runs `dbt test` to validate all models after they are built.
    """
    logger.info("="*60)
    logger.info("🧪 Running dbt tests...")
    logger.info("="*60)

    result = subprocess.run(["dbt", "test"])

    if result.returncode != 0:
        logger.error("❌ dbt test failed")
        sys.exit(1)

    logger.info("✅ dbt test completed successfully")


def run_loader(name, func, ctx):
    """
    Runs a single loader function.
    Automatically passes the Snowflake connection (ctx) if the function accepts it.
    Returns True on success, False on failure.
    """
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


def run_loaders(ctx):
    """
    Iterates through all registered loaders and runs them in sequence.
    Stops the pipeline immediately if any loader fails.
    """
    total = len(LOADERS)

    for i, (name, func) in enumerate(LOADERS, 1):
        logger.info(f"[{i}/{total}] Starting: {name}")

        if not run_loader(name, func, ctx):
            ctx.close()
            logger.error("🛑 Pipeline stopped due to loader failure")
            sys.exit(1)


def main():
    logger.info("="*60)
    logger.info("🚀 STARTING DATA PIPELINE")
    logger.info("="*60)

    # Step 1: Populate seed tables first so loaders can reference them
    # and so dbt models can join against them without race conditions
    run_dbt_seed()

    # Step 2: Connect to Snowflake
    ctx = get_connection()
    logger.info("✅ Connected to Snowflake")

    # Step 3: Run all raw data loaders sequentially
    run_loaders(ctx)

    # Step 4: Close Snowflake connection before running dbt
    ctx.close()
    logger.info("✅ Snowflake connection closed")

    # Step 5: Run dbt models — seeds are already loaded from Step 1
    # Using dbt run (not dbt build) to avoid the race condition where
    # build reruns seeds and models concurrently
    run_dbt_run()

    # Step 6: Run dbt tests to validate the data
    run_dbt_test()

    logger.info("="*60)
    logger.info("🏁 PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("="*60)
    sys.exit(0)


if __name__ == "__main__":
    main()