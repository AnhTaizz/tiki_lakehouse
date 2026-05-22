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


def load_bronze_to_iceberg(raw_filepath):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_date, current_timestamp, lit

    logger.info("Start loading raw data to Bronze Iceberg table")

    if not os.path.exists(raw_filepath):
        logger.error("File does not exist: %s", raw_filepath)
        sys.exit(1)

    spark = SparkSession.builder.appName("Tiki_Load_Bronze_Iceberg").getOrCreate()

    try:
        df = spark.read.option("multiline", "true").schema(build_product_schema()).json(raw_filepath)
        df = (
            df.withColumn("crawl_date", current_date())
            .withColumn("loaded_at", current_timestamp())
            .withColumn("source_file", lit(os.path.basename(raw_filepath)))
        )

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
    except Exception as exc:
        logger.error("Error writing to Iceberg: %s", exc)
        sys.exit(1)
    finally:
        spark.stop()


def load_to_iceberg(raw_filepath):
    load_bronze_to_iceberg(raw_filepath)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_file", required=True, help="Path to raw JSON file")
    args = parser.parse_args()

    load_bronze_to_iceberg(args.raw_file)
