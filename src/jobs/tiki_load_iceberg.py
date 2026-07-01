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
    from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType, BooleanType

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
            StructField("category_name", StringType(), True),
            StructField("quantity_sold", IntegerType(), True),
            StructField("is_active", BooleanType(), True),
        ]
    )


def clean_silver_data(df):
    from pyspark.sql.functions import col, when, trim

    logger.info("Step 1.5: Cleaning data (Handling nulls and trimming strings)")
    
    # 1. Drop rows with null IDs
    df_clean = df.filter(col("id").isNotNull())
    
    # 2. Fill nulls for numeric columns
    df_clean = df_clean.withColumn("price", when(col("price").isNull(), 0).otherwise(col("price")))
    df_clean = df_clean.withColumn("discount_rate", when(col("discount_rate").isNull(), 0).otherwise(col("discount_rate")))
    df_clean = df_clean.withColumn("quantity_sold", when(col("quantity_sold").isNull(), 0).otherwise(col("quantity_sold")))
    
    # 3. Trim whitespace from text columns
    df_clean = df_clean.withColumn("name", trim(col("name")))
    df_clean = df_clean.withColumn("brand_name", trim(col("brand_name")))
    
    return df_clean


def load_bronze(spark, df, is_streaming=False):
    logger.info("Step 1: Loading raw data to Bronze Iceberg table")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki_bronze")
    table_name = "local_catalog.tiki_bronze.products_raw"

    if spark.catalog.tableExists(table_name):
        if is_streaming:
            logger.info("Table %s exists, appending to Bronze partition for streaming", table_name)
            df.writeTo(table_name).append()
        else:
            logger.info("Table %s exists, overwriting Bronze partition for idempotency", table_name)
            df.writeTo(table_name).overwritePartitions()
        logger.info("Bronze write completed")
    else:
        logger.info("Table %s does not exist, creating Bronze table", table_name)
        df.write.format("iceberg").partitionBy("crawl_date").saveAsTable(table_name)
        logger.info("Initial Bronze table created")


# SCD type 4
def load_silver_history(spark, df_new):
    logger.info("Step 2: Loading price history to Silver Iceberg table")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki_silver")
    history_table = "local_catalog.tiki_silver.price_history"
    active_table = "local_catalog.tiki_silver.products"

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


def load_silver_active(spark, df_new, is_full_snapshot=False):
    logger.info("Step 3: Updating Silver Active table (SCD Type 1)")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki_silver")
    active_table = "local_catalog.tiki_silver.products"

    if spark.catalog.tableExists(active_table):
        logger.info("Active table %s exists. Performing MERGE INTO", active_table)

        df_new.createOrReplaceTempView("today_data")

        when_not_matched_by_source = ""
        if is_full_snapshot:
            # Get distinct categories in this batch to scope the soft delete
            cat_rows = df_new.select("category_id").distinct().collect()
            cat_ids = [str(r.category_id) for r in cat_rows if r.category_id is not None]
            if cat_ids:
                cat_list = ", ".join(cat_ids)
                when_not_matched_by_source = f"WHEN NOT MATCHED BY SOURCE AND t.category_id IN ({cat_list}) THEN UPDATE SET t.is_active = false"

        merge_query = f"""
            MERGE INTO {active_table} AS t
            USING today_data AS s
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
                    t.category_name = s.category_name,
                    t.quantity_sold = s.quantity_sold,
                    t.crawl_date = s.crawl_date,
                    t.loaded_at = s.loaded_at,
                    t.source_file = s.source_file,
                    t.is_active = s.is_active
            WHEN NOT MATCHED THEN
                INSERT *
            {when_not_matched_by_source}
        """
        spark.sql(merge_query)
        logger.info("MERGE INTO active table completed successfully")
    else:
        logger.info("Table %s does not exist, creating Active table", active_table)
        df_new.write.format("iceberg").saveAsTable(active_table)
        logger.info("Initial Active table created")


def run_pipeline(raw_filepath, layer="all"):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_date, current_timestamp, lit, regexp_extract, col, to_date, date_format

    if not os.path.exists(raw_filepath):
        logger.error("File does not exist: %s", raw_filepath)
        sys.exit(1)

    spark = SparkSession.builder.appName("Tiki_Medallion_Pipeline").getOrCreate()

    try:
        # Step 1: Read raw JSON regardless of layer to extract crawl_date properly
        # Reading as JSON lines (NDJSON) prevents OOM issues for large files
        df_new = spark.read.schema(build_product_schema()).json(raw_filepath)
        df_new = (
            df_new.withColumn("source_file", lit(os.path.basename(raw_filepath)))
            .withColumn("loaded_at", current_timestamp())
            .withColumn("is_active", lit(True))
            # Extract date from filename (e.g., tiki_products_raw_2026-06-11.json -> 2026-06-11)
            .withColumn("crawl_date", to_date(regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1)))
        ).dropDuplicates(["id"])

        if layer in ["all", "bronze"]:
            logger.info(f"Executing Bronze Layer load for {raw_filepath}")
            load_bronze(spark, df_new)
            
        if layer in ["all", "silver"]:
            logger.info(f"Executing Silver Layer load for {raw_filepath}")
            
            # Extract crawl_date string to filter Bronze table
            date_row = df_new.select(date_format("crawl_date", "yyyy-MM-dd").alias("cdate")).first()
            if not date_row or not date_row["cdate"]:
                logger.error("Could not extract crawl_date from JSON filename.")
                sys.exit(1)
                
            cdate_str = date_row["cdate"]
            logger.info(f"Reading from Bronze table for crawl_date: {cdate_str}")
            
            # Read from Bronze layer for the specific crawl_date
            df_bronze = spark.sql(f"SELECT * FROM local_catalog.tiki_bronze.products_raw WHERE crawl_date = date('{cdate_str}')")
            
            # Clean data
            df_clean = clean_silver_data(df_bronze)
            
            # Silver History table
            load_silver_history(spark, df_clean)

            # Write/Merge into Silver Active table (Batch Mode = Full Snapshot)
            load_silver_active(spark, df_clean, is_full_snapshot=True)

        logger.info("Pipeline executed successfully!")
    except Exception as exc:
        logger.error("Error writing to Iceberg: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


def load_to_iceberg(raw_filepath, layer="all"):
    run_pipeline(raw_filepath, layer)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_file", required=True, help="Path to raw JSON file")
    parser.add_argument("--layer", choices=["all", "bronze", "silver"], default="all", help="Which medallion layer to execute")
    args = parser.parse_args()

    run_pipeline(args.raw_file, args.layer)

