"""
tiki_disaster_recovery.py
=========================
Emergency recovery script — rebuilds Silver and Gold layers from scratch
using raw data stored in the Bronze Iceberg table.

Use this when Silver/Gold data is corrupted due to:
  - A code bug that wrote incorrect values for several days
  - An accidental DROP TABLE on Silver/Gold
  - A schema migration that broke existing data

HOW IT WORKS
------------
Replays Bronze data day-by-day in chronological order, re-running the exact
same Silver logic (SCD Type 1 + SCD Type 4) per day so that price change
history is reconstructed correctly.

USAGE
-----
# Dry-run: preview what would happen without touching any data
python tiki_disaster_recovery.py --dry-run

# Full recovery (all dates found in Bronze)
python tiki_disaster_recovery.py

# Recover only a specific date range
python tiki_disaster_recovery.py --from-date 2026-06-01 --to-date 2026-06-15

# Recover Silver only (skip Gold rebuild — faster)
python tiki_disaster_recovery.py --skip-gold

WARNING
-------
This script DROPS Silver and Gold tables before rebuilding.
Always run with --dry-run first to confirm the date range is correct.
"""

import argparse
import os
import sys
import time
from datetime import date, datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.utils import setup_logger

logger = setup_logger(__name__)


BRONZE_TABLE  = "local_catalog.tiki_bronze.products_raw"
SILVER_ACTIVE = "local_catalog.tiki_silver.products"
SILVER_HIST   = "local_catalog.tiki_silver.price_history"
GOLD_TABLES   = [
    "local_catalog.tiki_gold.brand_performance",
    "local_catalog.tiki_gold.price_trend",
    "local_catalog.tiki_gold.discount_analysis",
    "local_catalog.tiki_gold.top_products",
    "local_catalog.tiki_gold.daily_summary",
]



# Validate Bronze exists and has data
def validate_bronze(spark) -> list[str]:
    """
    Returns a sorted list of all distinct crawl_date strings found in Bronze.
    Exits with an error if Bronze is missing or empty.
    """
    if not spark.catalog.tableExists(BRONZE_TABLE):
        logger.error("Bronze table '%s' does not exist. Nothing to recover from.", BRONZE_TABLE)
        sys.exit(1)

    rows = spark.sql(
        f"SELECT DISTINCT CAST(crawl_date AS STRING) AS d FROM {BRONZE_TABLE} ORDER BY d ASC"
    ).collect()

    if not rows:
        logger.error("Bronze table exists but contains no data. Cannot recover.")
        sys.exit(1)

    dates = [r["d"] for r in rows]
    logger.info("Found %d distinct crawl_date(s) in Bronze: %s → %s", len(dates), dates[0], dates[-1])
    return dates


# Filter date range based on CLI args
def filter_dates(dates: list[str], from_date: str | None, to_date: str | None) -> list[str]:
    """
    Filters the list of dates to those within [from_date, to_date] (inclusive).
    If either bound is None, it defaults to the first/last date in the list.
    """
    start = from_date or dates[0]
    end   = to_date   or dates[-1]

    filtered = [d for d in dates if start <= d <= end]

    if not filtered:
        logger.error(
            "No crawl_dates found in Bronze between %s and %s. Check your date range.", start, end
        )
        sys.exit(1)

    logger.info(
        "Recovery date range: %s → %s (%d day(s) to process)",
        filtered[0], filtered[-1], len(filtered)
    )
    return filtered


# Drop Silver and Gold tables to start fresh
def drop_silver_and_gold(spark, skip_gold: bool, dry_run: bool):
    tables_to_drop = [SILVER_ACTIVE, SILVER_HIST]
    if not skip_gold:
        tables_to_drop.extend(GOLD_TABLES)

    logger.info("=" * 60)
    logger.info("DROPPING existing Silver%s tables...", "" if skip_gold else " + Gold")
    for tbl in tables_to_drop:
        if spark.catalog.tableExists(tbl):
            if dry_run:
                logger.info("[DRY-RUN] Would DROP TABLE: %s", tbl)
            else:
                spark.sql(f"DROP TABLE IF EXISTS {tbl}")
                logger.info("Dropped: %s", tbl)
        else:
            logger.info("Table does not exist (skip): %s", tbl)
    logger.info("=" * 60)


# Replay Silver day-by-day
def replay_silver_for_date(spark, crawl_date: str, dry_run: bool):
    """
    Reads the Bronze snapshot for a single crawl_date, then re-runs Silver
    loading logic (history + active) exactly as the live pipeline does.
    Reuses functions from tiki_load_iceberg to guarantee logic parity.
    """
    # Import here to avoid Spark session conflicts at module load time
    from jobs.tiki_load_iceberg import load_silver_active, load_silver_history
    from pyspark.sql.functions import col, lit, current_timestamp

    logger.info("--- Replaying Silver for crawl_date: %s ---", crawl_date)

    df_day = spark.sql(f"""
        SELECT * FROM {BRONZE_TABLE}
        WHERE CAST(crawl_date AS STRING) = '{crawl_date}'
    """).dropDuplicates(["id"])

    row_count = df_day.count()
    logger.info("  Bronze snapshot: %d products", row_count)

    if row_count == 0:
        logger.warning("  No data found for %s in Bronze — skipping.", crawl_date)
        return

    # Ensure loaded_at and source_file columns exist (Bronze may not have them)
    if "loaded_at" not in df_day.columns:
        df_day = df_day.withColumn("loaded_at", current_timestamp())
    if "source_file" not in df_day.columns:
        df_day = df_day.withColumn("source_file", lit(f"recovery_{crawl_date}"))

    if dry_run:
        logger.info("  [DRY-RUN] Would run load_silver_history() + load_silver_active() for %s", crawl_date)
        return

    load_silver_history(spark, df_day)
    load_silver_active(spark, df_day)
    logger.info("  Silver replay complete for %s.", crawl_date)


# Rebuild Gold layer
def rebuild_gold(spark, dry_run: bool):
    from jobs.tiki_gold import run_gold_pipeline

    logger.info("=" * 60)
    logger.info("Rebuilding Gold layer from recovered Silver ...")
    logger.info("=" * 60)

    if dry_run:
        logger.info("[DRY-RUN] Would call run_gold_pipeline()")
        return

    run_gold_pipeline()
    logger.info("Gold rebuild complete.")


# Main entry point
def run_recovery(from_date: str | None, to_date: str | None, skip_gold: bool, dry_run: bool):
    from pyspark.sql import SparkSession

    logger.info("=" * 60)
    logger.info("TIKI LAKEHOUSE — DISASTER RECOVERY")
    if dry_run:
        logger.info("*** DRY-RUN MODE — No data will be written ***")
    logger.info("=" * 60)

    spark = SparkSession.builder.appName("Tiki_Disaster_Recovery").getOrCreate()

    try:
        t_start = time.time()

        all_dates = validate_bronze(spark)

        dates_to_replay = filter_dates(all_dates, from_date, to_date)

        drop_silver_and_gold(spark, skip_gold, dry_run)

        logger.info("Starting day-by-day Silver replay (%d day(s))...", len(dates_to_replay))
        for i, d in enumerate(dates_to_replay, start=1):
            logger.info("[%d/%d] Processing: %s", i, len(dates_to_replay), d)
            replay_silver_for_date(spark, d, dry_run)

        if not skip_gold:
            rebuild_gold(spark, dry_run)
        else:
            logger.info("Skipping Gold rebuild (--skip-gold flag set).")

        elapsed = time.time() - t_start
        logger.info("=" * 60)
        logger.info("RECOVERY %s", "SIMULATION COMPLETE" if dry_run else "COMPLETE")
        logger.info("  Days replayed : %d", len(dates_to_replay))
        logger.info("  Date range    : %s → %s", dates_to_replay[0], dates_to_replay[-1])
        logger.info("  Gold rebuilt  : %s", "No (--skip-gold)" if skip_gold else "Yes")
        logger.info("  Elapsed time  : %.1f seconds", elapsed)
        logger.info("=" * 60)

    except Exception as exc:
        logger.error("Recovery failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Tiki Lakehouse — Disaster Recovery\n"
            "Drops Silver/Gold and replays Bronze day-by-day to rebuild them."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--from-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="Start date for recovery (inclusive). Defaults to earliest date in Bronze.",
    )
    parser.add_argument(
        "--to-date",
        metavar="YYYY-MM-DD",
        default=None,
        help="End date for recovery (inclusive). Defaults to latest date in Bronze.",
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip Gold layer rebuild after Silver recovery (faster, use when only Silver is needed).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview recovery plan without writing any data. Always run this first!",
    )

    args = parser.parse_args()
    run_recovery(
        from_date=args.from_date,
        to_date=args.to_date,
        skip_gold=args.skip_gold,
        dry_run=args.dry_run,
    )
