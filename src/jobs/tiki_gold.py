"""
tiki_gold.py
============
Gold layer Spark job — reads from Silver Iceberg tables, computes 4 business
aggregate tables and writes to:
  1. Iceberg Gold tables (local_catalog.tiki_gold.*)  → historical storage
  2. Reporting PostgreSQL (reporting_db)               → Superset dashboard

Gold tables:
  - brand_performance   : Top brands by sales volume, rating, product count
  - price_trend         : Average price by category + day (time series)
  - discount_analysis   : Discount rate analysis by category and brand
  - top_products        : Top 100 products by quantity_sold & rating
"""

import argparse
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.utils import setup_logger

logger = setup_logger(__name__)

# ---------------------------------------------------------------------------
# Config — read from environment (injected by Docker / Airflow)
# ---------------------------------------------------------------------------
REPORTING_DB_HOST = os.environ.get("REPORTING_DB_HOST", "reporting-postgres")
REPORTING_DB_PORT = os.environ.get("REPORTING_DB_PORT", "5432")
REPORTING_DB_NAME = os.environ.get("REPORTING_DB_NAME", "reporting")
REPORTING_DB_USER = os.environ.get("REPORTING_DB_USER", "reporting")
REPORTING_DB_PASSWORD = os.environ.get("REPORTING_DB_PASSWORD", "reporting123")

JDBC_URL = (
    f"jdbc:postgresql://{REPORTING_DB_HOST}:{REPORTING_DB_PORT}/{REPORTING_DB_NAME}"
)
JDBC_PROPS = {
    "user": REPORTING_DB_USER,
    "password": REPORTING_DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}


# ---------------------------------------------------------------------------
# Helper — writes DataFrame to both Iceberg and Postgres
# ---------------------------------------------------------------------------
def save_gold_table(spark, df, iceberg_table: str, pg_table: str, mode: str = "overwrite"):
    """Writes df to Iceberg Gold table and Reporting Postgres table."""

    # 1. Iceberg
    logger.info("Writing Iceberg Gold table: %s", iceberg_table)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki_gold")

    # Workaround for Iceberg Schema Mismatch error when data types change
    if mode == "overwrite":
        logger.info("Dropping existing Iceberg table to avoid schema conflicts...")
        spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")

    df.write.format("iceberg").mode(mode).saveAsTable(iceberg_table)
    logger.info("Iceberg write complete: %s", iceberg_table)

    # 2. Reporting Postgres (for Superset)
    logger.info("Writing Reporting Postgres table: %s", pg_table)
    df.write.jdbc(
        url=JDBC_URL,
        table=pg_table,
        mode=mode,
        properties=JDBC_PROPS,
    )
    logger.info("Postgres write complete: %s", pg_table)


# ---------------------------------------------------------------------------
# Gold Table 1 — Brand Performance
# Business question: "Which brand is leading in each category on Tiki?"
# ---------------------------------------------------------------------------
def compute_brand_performance(spark):
    logger.info("Computing gold.brand_performance ...")
    df = spark.sql("""
        SELECT
            category_id,
            category_name,
            brand_name,
            COUNT(DISTINCT id)              AS total_products,
            SUM(quantity_sold)              AS total_quantity_sold,
            ROUND(AVG(rating_average), 2)   AS avg_rating,
            ROUND(AVG(review_count), 0)     AS avg_reviews,
            ROUND(AVG(price), 0)            AS avg_price,
            ROUND(AVG(discount_rate), 1)    AS avg_discount_rate,
            MAX(crawl_date)                 AS last_updated
        FROM local_catalog.tiki_silver.products
        WHERE brand_name IS NOT NULL
          AND brand_name != 'No Brand'
          AND TRIM(brand_name) != ''
          AND quantity_sold > 0
        GROUP BY category_id, category_name, brand_name
        ORDER BY total_quantity_sold DESC
    """)
    save_gold_table(
        spark, df,
        "local_catalog.tiki_gold.brand_performance",
        "brand_performance",
    )
    return df


# ---------------------------------------------------------------------------
# Gold Table 2 — Price Volatility & Trend (time series)
# Business question: "How volatile are prices and how aggressive are promotions each day?"
# ---------------------------------------------------------------------------
def compute_price_trend(spark):
    logger.info("Computing gold.price_trend ...")
    df = spark.sql("""
        SELECT
            crawl_date,
            category_id,
            category_name,
            COUNT(id)                       AS total_price_changes,
            ROUND(AVG(price), 0)            AS avg_new_price,
            ROUND(MIN(price), 0)            AS min_new_price,
            ROUND(MAX(price), 0)            AS max_new_price,
            ROUND(AVG(discount_rate), 1)    AS avg_new_discount_rate
        FROM local_catalog.tiki_silver.price_history
        GROUP BY crawl_date, category_id, category_name
        ORDER BY crawl_date DESC, category_id
    """)
    save_gold_table(
        spark, df,
        "local_catalog.tiki_gold.price_trend",
        "price_trend",
    )
    return df


# ---------------------------------------------------------------------------
# Gold Table 3 — Discount Analysis
# Business question: "Which category has the best discount programs?"
# ---------------------------------------------------------------------------
def compute_discount_analysis(spark):
    logger.info("Computing gold.discount_analysis ...")
    df = spark.sql("""
        SELECT
            category_id,
            category_name,
            brand_name,
            COUNT(DISTINCT id)              AS product_count,
            ROUND(AVG(discount_rate), 1)    AS avg_discount_rate,
            MAX(discount_rate)              AS max_discount_rate,
            SUM(CASE WHEN discount_rate >= 30 THEN 1 ELSE 0 END) AS products_30pct_off,
            SUM(CASE WHEN discount_rate >= 50 THEN 1 ELSE 0 END) AS products_50pct_off,
            ROUND(AVG(price), 0)            AS avg_current_price,
            ROUND(AVG(original_price), 0)   AS avg_original_price,
            MAX(crawl_date)                 AS last_updated
        FROM local_catalog.tiki_silver.products
        WHERE discount_rate > 0
        GROUP BY category_id, category_name, brand_name
        ORDER BY avg_discount_rate DESC
    """)
    save_gold_table(
        spark, df,
        "local_catalog.tiki_gold.discount_analysis",
        "discount_analysis",
    )
    return df


# ---------------------------------------------------------------------------
# Gold Table 4 — Top Products
# Business question: "Top 100 products worth buying in Tiki's core categories?"
# ---------------------------------------------------------------------------
def compute_top_products(spark):
    logger.info("Computing gold.top_products ...")
    df = spark.sql("""
        SELECT
            id,
            name,
            brand_name,
            category_id,
            category_name,
            ROUND(price / 1000, 1)          AS price_k,
            ROUND(original_price / 1000, 1) AS original_price_k,
            discount_rate,
            rating_average,
            review_count,
            quantity_sold,
            url_key,
            thumbnail_url,
            crawl_date                      AS last_updated,
            ROUND(
                (quantity_sold * 0.5)
                + (rating_average * review_count * 0.3)
                + (discount_rate * 10 * 0.2),
            1) AS popularity_score
        FROM local_catalog.tiki_silver.products
        WHERE review_count >= 10
          AND rating_average >= 3.5
        ORDER BY popularity_score DESC
        LIMIT 100
    """)
    save_gold_table(
        spark, df,
        "local_catalog.tiki_gold.top_products",
        "top_products",
    )
    return df


# ---------------------------------------------------------------------------
# Gold Table 5 — Daily Summary (overview cho Superset home dashboard)
# ---------------------------------------------------------------------------
def compute_daily_summary(spark):
    logger.info("Computing gold.daily_summary ...")
    df = spark.sql("""
        SELECT
            crawl_date,
            COUNT(DISTINCT id)              AS total_products,
            COUNT(DISTINCT brand_name)      AS total_brands,
            COUNT(DISTINCT category_id)     AS total_categories,
            ROUND(AVG(price), 0)            AS avg_price,
            ROUND(AVG(discount_rate), 1)    AS avg_discount_rate,
            SUM(quantity_sold)              AS total_quantity_sold,
            SUM(CASE WHEN discount_rate >= 50 THEN 1 ELSE 0 END) AS flash_sale_products
        FROM local_catalog.tiki_bronze.products_raw
        GROUP BY crawl_date
        ORDER BY crawl_date DESC
    """)
    save_gold_table(
        spark, df,
        "local_catalog.tiki_gold.daily_summary",
        "daily_summary",
    )
    return df


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
def run_gold_pipeline():
    from pyspark.sql import SparkSession

    logger.info("=" * 60)
    logger.info("Starting Tiki GOLD layer pipeline")
    logger.info("Target Postgres: %s / %s", REPORTING_DB_HOST, REPORTING_DB_NAME)
    logger.info("=" * 60)

    spark = SparkSession.builder.appName("Tiki_Gold_Pipeline").getOrCreate()

    try:
        # Check if Silver tables exist
        if not spark.catalog.tableExists("local_catalog.tiki_silver.products"):
            logger.error(
                "Silver table 'local_catalog.tiki_silver.products' does not exist. "
                "Run Bronze/Silver pipeline first."
            )
            sys.exit(1)

        compute_brand_performance(spark)
        compute_price_trend(spark)
        compute_discount_analysis(spark)
        compute_top_products(spark)
        compute_daily_summary(spark)

        logger.info("=" * 60)
        logger.info("Gold pipeline completed successfully!")
        logger.info("Superset dashboard will auto-refresh upon querying Postgres.")
        logger.info("=" * 60)

    except Exception as exc:
        logger.error("Gold pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tiki Gold Layer Pipeline")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run — only log, do not write data",
    )
    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN mode — no data written")
    else:
        run_gold_pipeline()
