"""
tiki_gold.py
============
Gold layer Spark job — reads from Silver Iceberg tables, computes One Big Table (OBT)
and 6 Business Data Marts. Writes to:
  1. Iceberg Gold tables (local_catalog.tiki_gold.*)  → historical storage
  2. Reporting PostgreSQL (reporting_db)               → Superset dashboard
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
# Config
# ---------------------------------------------------------------------------
REPORTING_DB_HOST = os.environ.get("REPORTING_DB_HOST", "reporting-postgres")
REPORTING_DB_PORT = os.environ.get("REPORTING_DB_PORT", "5432")
REPORTING_DB_NAME = os.environ.get("REPORTING_DB_NAME", "reporting")
REPORTING_DB_USER = os.environ.get("REPORTING_DB_USER", "reporting")
REPORTING_DB_PASSWORD = os.environ.get("REPORTING_DB_PASSWORD", "reporting123")

JDBC_URL = f"jdbc:postgresql://{REPORTING_DB_HOST}:{REPORTING_DB_PORT}/{REPORTING_DB_NAME}"
JDBC_PROPS = {
    "user": REPORTING_DB_USER,
    "password": REPORTING_DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def save_gold_table(spark, df, iceberg_table: str, pg_table: str, mode: str = "overwrite"):
    from pyspark.sql.functions import expr
    
    # Superset Timezone Hack: Convert Date to Timestamp and add 7 hours (ICT offset).
    # When Superset renders this in the browser, it subtracts 7 hours, landing perfectly on 00:00 of the correct day!
    if "crawl_date" in df.columns:
        df = df.withColumn("crawl_date", expr("CAST(crawl_date AS TIMESTAMP) + INTERVAL 7 HOURS"))

    # 1. Iceberg
    logger.info("Writing Iceberg Gold table: %s", iceberg_table)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki_gold")
    if mode == "overwrite":
        spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
    df.write.format("iceberg").mode(mode).saveAsTable(iceberg_table)

    # 2. Postgres
    logger.info("Writing Reporting Postgres table: %s", pg_table)
    df.write.jdbc(url=JDBC_URL, table=pg_table, mode=mode, properties=JDBC_PROPS)


# ===========================================================================
# 1. ONE BIG TABLE (OBT) - CORE FOUNDATION
# ===========================================================================
def build_obt_product_daily_snapshot(spark):
    logger.info("Building obt_product_daily_snapshot...")
    df = spark.sql("""
        WITH ranked_history AS (
            SELECT
                *,
                ROW_NUMBER() OVER(PARTITION BY id, crawl_date ORDER BY loaded_at DESC) as rn
            FROM local_catalog.tiki_silver.price_history
            WHERE crawl_date IS NOT NULL
        ),
        daily_latest AS (
            SELECT * FROM ranked_history WHERE rn = 1
        ),
        delta_calc AS (
            SELECT
                h.*,
                LAG(h.quantity_sold) OVER(PARTITION BY h.id ORDER BY h.crawl_date ASC) as prev_qty
            FROM daily_latest h
        )
        SELECT
            d.crawl_date,
            d.id AS product_id,
            p.name AS product_name,
            d.brand_name,
            d.category_id,
            p.category_name,
            d.price,
            d.original_price,
            d.discount_rate,
            d.rating_average,
            d.review_count,
            p.url_key,
            p.thumbnail_url,
            d.quantity_sold AS lifetime_sold,
            GREATEST(d.quantity_sold - COALESCE(d.prev_qty, d.quantity_sold), 0) AS daily_quantity_sold
        FROM delta_calc d
        LEFT JOIN local_catalog.tiki_silver.products p ON d.id = p.id
    """)
    save_gold_table(spark, df, "local_catalog.tiki_gold.obt_product_daily_snapshot", "obt_product_daily_snapshot")


# ===========================================================================
# 2. BUSINESS DATA MARTS (JOIN-FREE)
# ===========================================================================

def build_mart_daily_summary(spark):
    logger.info("Building mart_daily_summary...")
    df = spark.sql("""
        SELECT
            crawl_date,
            COUNT(DISTINCT product_id) AS total_products,
            COUNT(DISTINCT brand_name) AS total_brands,
            COUNT(DISTINCT category_name) AS total_categories,
            ROUND(AVG(price), 0) AS avg_price,
            ROUND(AVG(discount_rate), 1) AS avg_discount_rate,
            SUM(daily_quantity_sold) AS total_quantity_sold,
            SUM(CASE WHEN discount_rate >= 50 THEN 1 ELSE 0 END) AS flash_sale_products,
            SUM(price * daily_quantity_sold) AS total_revenue
        FROM local_catalog.tiki_gold.obt_product_daily_snapshot
        GROUP BY 1
        ORDER BY crawl_date DESC
    """)
    save_gold_table(spark, df, "local_catalog.tiki_gold.mart_daily_summary", "mart_daily_summary")

def build_mart_brand_market_share(spark):
    logger.info("Building mart_brand_market_share...")
    df = spark.sql("""
        SELECT
            crawl_date,
            category_name,
            brand_name,
            COUNT(DISTINCT product_id) AS total_products,
            SUM(daily_quantity_sold) AS total_volume_sold,
            SUM(price * daily_quantity_sold) AS total_revenue,
            ROUND(AVG(rating_average), 2) AS avg_rating
        FROM local_catalog.tiki_gold.obt_product_daily_snapshot
        WHERE brand_name IS NOT NULL AND brand_name != 'No Brand'
        GROUP BY 1, 2, 3
    """)
    save_gold_table(spark, df, "local_catalog.tiki_gold.mart_brand_market_share", "mart_brand_market_share")

def build_mart_product_ranking(spark):
    logger.info("Building mart_product_ranking...")
    df = spark.sql("""
        WITH daily_scored AS (
            SELECT
                crawl_date,
                product_id,
                product_name,
                brand_name,
                category_name,
                thumbnail_url,
                url_key,
                price,
                discount_rate,
                rating_average,
                review_count,
                daily_quantity_sold,
                ROUND(
                    (daily_quantity_sold * 0.5)
                    + (rating_average * review_count * 0.3)
                    + (discount_rate * 10 * 0.2),
                1) AS popularity_score,
                (price * daily_quantity_sold) AS daily_revenue
            FROM local_catalog.tiki_gold.obt_product_daily_snapshot
        ),
        ranked_daily AS (
            SELECT *,
                   ROW_NUMBER() OVER(PARTITION BY crawl_date ORDER BY daily_quantity_sold DESC, daily_revenue DESC) as rank
            FROM daily_scored
        )
        SELECT * FROM ranked_daily WHERE rank <= 100
    """)
    save_gold_table(spark, df, "local_catalog.tiki_gold.mart_product_ranking", "mart_product_ranking")

def build_mart_price_volatility(spark):
    logger.info("Building mart_price_volatility...")
    # Reads from price_history directly to get all events, not just the daily snapshot
    df = spark.sql("""
        SELECT
            p.category_name,
            h.crawl_date,
            COUNT(h.id) AS total_price_events,
            ROUND(AVG(h.price), 0) AS avg_price,
            ROUND(MIN(h.price), 0) AS min_price,
            ROUND(MAX(h.price), 0) AS max_price,
            ROUND(AVG(h.discount_rate), 1) AS avg_discount_rate
        FROM local_catalog.tiki_silver.price_history h
        LEFT JOIN local_catalog.tiki_silver.products p ON h.id = p.id
        WHERE h.crawl_date IS NOT NULL
        GROUP BY 1, 2
        ORDER BY h.crawl_date DESC, p.category_name
    """)
    save_gold_table(spark, df, "local_catalog.tiki_gold.mart_price_volatility", "mart_price_volatility")

def build_mart_marketing_campaign_roi(spark):
    logger.info("Building mart_marketing_campaign_roi...")
    df = spark.sql("""
        SELECT
            crawl_date,
            category_name,
            CASE
                WHEN discount_rate = 0 THEN '1. No Discount (0%)'
                WHEN discount_rate > 0 AND discount_rate <= 15 THEN '2. Light Sale (1-15%)'
                WHEN discount_rate > 15 AND discount_rate <= 30 THEN '3. Regular Sale (16-30%)'
                WHEN discount_rate > 30 AND discount_rate <= 50 THEN '4. Deep Sale (31-50%)'
                ELSE '5. Clearance (>50%)'
            END AS campaign_type,
            COUNT(DISTINCT product_id) AS total_products,
            ROUND(AVG(daily_quantity_sold), 0) AS avg_quantity_sold_per_item,
            SUM(daily_quantity_sold) AS total_volume_sold,
            SUM(price * daily_quantity_sold) AS total_revenue
        FROM local_catalog.tiki_gold.obt_product_daily_snapshot
        GROUP BY 1, 2, 3
    """)
    save_gold_table(spark, df, "local_catalog.tiki_gold.mart_marketing_campaign_roi", "mart_marketing_campaign_roi")


def run_gold_pipeline():
    from pyspark.sql import SparkSession

    logger.info("=" * 60)
    logger.info("Starting Tiki GOLD layer pipeline (Pure OBT Architecture)")
    logger.info("=" * 60)

    spark = SparkSession.builder \
        .appName("Tiki_Gold_Pipeline_OBT") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .getOrCreate()

    try:
        # Check if Silver tables exist
        if not spark.catalog.tableExists("local_catalog.tiki_silver.products"):
            logger.error("Silver table 'products' does not exist. Run Bronze/Silver first.")
            sys.exit(1)

        # 1. Build Foundation OBT
        build_obt_product_daily_snapshot(spark)

        # 2. Build Marts
        build_mart_daily_summary(spark)
        build_mart_brand_market_share(spark)
        build_mart_product_ranking(spark)
        build_mart_price_volatility(spark)
        build_mart_marketing_campaign_roi(spark)

        logger.info("=" * 60)
        logger.info("Gold pipeline completed successfully (OBT mode)!")
        logger.info("=" * 60)

    except Exception as exc:
        logger.error("Gold pipeline failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tiki Gold Layer Pipeline")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.dry_run:
        logger.info("DRY RUN mode — no data written")
    else:
        run_gold_pipeline()
