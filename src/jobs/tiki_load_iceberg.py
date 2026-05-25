import argparse
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))

if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.utils import setup_logger


logger = setup_logger(__name__)


def build_product_schema():
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("id", LongType(), True),
            StructField("sku", StringType(), True),
            StructField("name", StringType(), True),
            StructField("url_key", StringType(), True),
            StructField("price", LongType(), True),
            StructField("original_price", LongType(), True),
            StructField("discount", LongType(), True),
            StructField("discount_rate", IntegerType(), True),
            StructField("brand_name", StringType(), True),
            StructField("rating_average", DoubleType(), True),
            StructField("review_count", IntegerType(), True),
            StructField("thumbnail_url", StringType(), True),
            StructField("category_id", IntegerType(), True),
            StructField("quantity_sold", IntegerType(), True),
        ]
    )


def load_bronze(spark, df):
    logger.info("Step 1: Loading raw data to Bronze Iceberg table")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki_bronze")
    table_name = "local_catalog.tiki_bronze.products_raw"

    if spark.catalog.tableExists(table_name):
        logger.info("Table %s exists, appending Bronze rows", table_name)
        df.write.format("iceberg").mode("append").saveAsTable(table_name)
        logger.info("Bronze append completed")
    else:
        logger.info("Table %s does not exist, creating Bronze table", table_name)
        df.write.format("iceberg").partitionBy("crawl_date").saveAsTable(table_name)
        logger.info("Initial Bronze table created")


# SCD type 4
def load_silver_history(spark, df_new):
    logger.info("Step 2: Loading price history to Silver Iceberg table")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki")
    history_table = "local_catalog.tiki.price_history"
    active_table = "local_catalog.tiki.products"

    from pyspark.sql.functions import col

    if spark.catalog.tableExists(history_table) and spark.catalog.tableExists(active_table):
        logger.info("Comparing new crawl with active products to detect price updates")
        df_active = spark.read.table(active_table)

        # Select only key columns to compare
        df_active_subset = df_active.select(
            col("id").alias("curr_id"),
            col("price").alias("curr_price"),
            col("original_price").alias("curr_original_price"),
            col("discount").alias("curr_discount"),
            col("discount_rate").alias("curr_discount_rate"),
        )

        # Left join new crawl data with active table to detect changes
        df_diff = df_new.join(df_active_subset, df_new.id == df_active_subset.curr_id, "left")

        # CDC new product OR price/discount has changed
        df_filtered = df_diff.filter(
            col("curr_id").isNull()
            | (col("price") != col("curr_price"))
            | (col("original_price") != col("curr_original_price"))
            | (col("discount") != col("curr_discount"))
            | (col("discount_rate") != col("curr_discount_rate"))
        ).select(df_new.columns)

        change_count = df_filtered.count()
        logger.info("Detected %d rows with price changes or new products", change_count)

        if change_count > 0:
            df_filtered.write.format("iceberg").mode("append").saveAsTable(history_table)
            logger.info("Successfully appended %d changed rows to history", change_count)
        else:
            logger.info("No price changes detected. Nothing appended to history table.")
    else:
        logger.info("History or Active table does not exist yet. Performing initial history load")
        df_new.write.format("iceberg").mode("append").saveAsTable(history_table)
        logger.info("Initial load to history completed")


def load_silver_active(spark, df_new):
    logger.info("Step 3: Updating Silver Active table (SCD Type 1)")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki")
    active_table = "local_catalog.tiki.products"

    if spark.catalog.tableExists(active_table):
        logger.info("Active table %s exists. Performing MERGE INTO", active_table)

        df_new.createOrReplaceTempView("du_lieu_hom_nay")

        merge_query = f"""
            MERGE INTO {active_table} AS t
            USING du_lieu_hom_nay AS s
            ON t.id = s.id
            WHEN MATCHED THEN
                UPDATE SET
                    t.sku = s.sku,
                    t.name = s.name,
                    t.url_key = s.url_key,
                    t.price = s.price,
                    t.original_price = s.original_price,
                    t.discount = s.discount,
                    t.discount_rate = s.discount_rate,
                    t.brand_name = s.brand_name,
                    t.rating_average = s.rating_average,
                    t.review_count = s.review_count,
                    t.thumbnail_url = s.thumbnail_url,
                    t.category_id = s.category_id,
                    t.quantity_sold = s.quantity_sold,
                    t.crawl_date = s.crawl_date,
                    t.loaded_at = s.loaded_at,
                    t.source_file = s.source_file
            WHEN NOT MATCHED THEN
                INSERT *
        """
        spark.sql(merge_query)
        logger.info("MERGE INTO active table completed successfully")
    else:
        logger.info("Table %s does not exist, creating Active table", active_table)
        df_new.write.format("iceberg").saveAsTable(active_table)
        logger.info("Initial Active table created")


def run_pipeline(raw_filepath):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_date, current_timestamp, lit

    logger.info("Starting Tiki Medallion pipeline for: %s", raw_filepath)

    if not os.path.exists(raw_filepath):
        logger.error("File does not exist: %s", raw_filepath)
        sys.exit(1)

    spark = SparkSession.builder.appName("Tiki_Medallion_Pipeline").getOrCreate()

    try:
        df_new = spark.read.option("multiline", "true").schema(build_product_schema()).json(raw_filepath)
        df_new = (
            df_new.withColumn("crawl_date", current_date())
            .withColumn("loaded_at", current_timestamp())
            .withColumn("source_file", lit(os.path.basename(raw_filepath)))
        )

        # Bronze Raw table
        load_bronze(spark, df_new)

        # Silver History table
        load_silver_history(spark, df_new)

        # Write/Merge into Silver Active table
        load_silver_active(spark, df_new)

        logger.info("Pipeline executed successfully!")
    except Exception as exc:
        logger.error("Error writing to Iceberg: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


def load_to_iceberg(raw_filepath):
    run_pipeline(raw_filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_file", required=True, help="Path to raw JSON file")
    args = parser.parse_args()

    run_pipeline(args.raw_file)

