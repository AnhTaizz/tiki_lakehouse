
import os
import sys
import argparse

current_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.abspath(os.path.join(current_dir, '..'))

from libs.utils import setup_logger
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

logger = setup_logger(__name__)

def load_to_iceberg(raw_filepath):
    logger.info("Bắt đầu load dữ liệu và Lakehouse (Iceberg)")

    if not os.path.exists(raw_filepath):
        logger.error(f"File không tồn tại: {raw_filepath}")
        sys.exit(1)

    spark = SparkSession.builder.appName("Tiki_Load_Iceberg").getOrCreate()

    schema = StructType([
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
        StructField("quantity_sold", IntegerType(), True)
    ])

    df = spark.read.option("multiline", "true").schema(schema).json(raw_filepath)

    from pyspark.sql.functions import current_date
    df = df.withColumn("crawl_date", current_date())
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki")
    table_name = "local_catalog.tiki.products"

    try:
        df.createOrReplaceTempView("new_crawl_dataframe")
        logger.info("Kiểm tra biến động để lưu history...")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local_catalog.tiki.price_history(
                id BIGINT,
                price BIGINT,
                crawl_date DATE
                ) USING iceberg PARTITIONED BY (crawl_date)
        """)    

        if spark.catalog.tableExists(table_name):
            spark.sql(f"""
                INSERT INTO local_catalog.tiki.price_history
                SELECT s.id, s.price, s.crawl_date
                FROM new_crawl_dataframe s
                LEFT JOIN {table_name} t ON s.id = t.id
                WHERE t.id IS NULL OR s.price != t.price
            """)
        else:
            spark.sql(f"""
                INSERT INTO local_catalog.tiki.price_history
                SELECT id, price, crawl_date FROM new_crawl_dataframe
            """)
        logger.info("Đã update lịch sử giá")

        if spark.catalog.tableExists(table_name):
            logger.info(f"Bảng {table_name} đã tồn tại, tiến hành Upsert")
            merge_query = f"""
                MERGE INTO {table_name} AS target
                USING new_crawl_dataframe AS source
                ON target.id = source.id 
                WHEN MATCHED THEN 
                    UPDATE SET 
                        target.price = source.price, 
                        target.original_price = source.original_price,
                        target.discount = source.discount,
                        target.discount_rate = source.discount_rate,
                        target.rating_average = source.rating_average,
                        target.review_count = source.review_count,
                        target.quantity_sold = source.quantity_sold,
                        target.crawl_date = source.crawl_date
                WHEN NOT MATCHED THEN 
                    INSERT *
            """
            spark.sql(merge_query)
            logger.info("Upsert bằng Merge into thành công")
        else:
            logger.info(f"Bảng {table_name} CHƯA TỒN TẠI, tạo bảng lần đầu với PARTITION")
            df.write.format("iceberg").partitionBy("crawl_date").saveAsTable(table_name)
            logger.info("Tạo bảng lần đầu bằng PATITION thành công")
    except Exception as e:
        logger.error(f"Lỗi ghi vào Iceberg: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--raw_file', required=True, help='Đường dẫn tới file JSON thô')
    args = parser.parse_args()

    load_to_iceberg(args.raw_file)
            
