# filepath: /workspace/jobs/spark_crawler.py
import os
import sys
import json
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.abspath(os.path.join(current_dir, '..'))
if workspace_dir not in sys.path:
    sys.path.append(workspace_dir)

from libs.tiki_category import load_categories_from_api, load_categories_from_file, get_leaf_categories
from libs.tiki_product import fetch_products_by_category
from libs.utils import setup_logger, save_to_json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

logger = setup_logger(__name__)

"""Extract - Cào dữ liệu từ API"""
def crawl_tiki_data(target_category_id=1520):

    logger.info(f"=== BẮT ĐẦU CRAWL NGÀNH HÀNG ID {target_category_id} ===")
    
    raw_cats = load_categories_from_api(category_id=target_category_id)
    if not raw_cats:
        logger.warning("API danh mục lỗi. Chuyển sang đọc từ file local backup...")
        backup_path = os.path.join(workspace_dir, 'data', 'tiki_category.json')
        raw_cats = load_categories_from_file(backup_path)
        
    leaf_cats = get_leaf_categories(raw_cats)
    logger.info(f"Đã bóc tách được {len(leaf_cats)} danh mục lá.")
    
    all_products = []
    
    for idx, cat in enumerate(leaf_cats):
        logger.info(f"[{idx+1}/{len(leaf_cats)}] Đang xử lý: {cat['name']}...")
        products = fetch_products_by_category(cat['id'], cat['url_key'])
        all_products.extend(products)
        
    # Lưu file thô (Raw/Bronze Layer) theo ngày
    today_str = datetime.now().strftime("%Y-%m-%d")
    raw_filename = f"tiki_skincare_raw_{today_str}.json"
    raw_filepath = os.path.join(workspace_dir, 'data', raw_filename)
    
    save_to_json(all_products, raw_filepath)
    logger.info(f"Đã cào xong {len(all_products)} sản phẩm. Lưu raw tại: {raw_filepath}")
    
    return raw_filepath

"""Load - Đọc JSON và đẩy vào Iceberg bằng Spark (Kèm CDC và Partition)"""
def load_to_iceberg(raw_filepath):
    logger.info("=== BẮT ĐẦU ĐẨY DỮ LIỆU VÀO LAKEHOUSE (ICEBERG) ===")
    
    spark = SparkSession.builder \
        .appName("Tiki_Crawler_To_Iceberg") \
        .getOrCreate()
        
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
        # ĐĂNG KÝ BẢNG TẠM TRÊN SPARK
        df.createOrReplaceTempView("dataframe_moi_cao_ve")

        # CDC, SCD type 4
        logger.info("Đang kiểm tra biến động giá để lưu lịch sử...")
        
        spark.sql("""
            CREATE TABLE IF NOT EXISTS local_catalog.tiki.price_history (
                id BIGINT,
                price BIGINT,
                crawl_date DATE
            ) USING iceberg PARTITIONED BY (crawl_date)
        """)
        
        if spark.catalog.tableExists(table_name):
            spark.sql(f"""
                INSERT INTO local_catalog.tiki.price_history
                SELECT s.id, s.price, s.crawl_date
                FROM dataframe_moi_cao_ve s
                LEFT JOIN {table_name} t ON s.id = t.id
                WHERE t.id IS NULL OR s.price != t.price
            """)
        else:
            spark.sql("""
                INSERT INTO local_catalog.tiki.price_history
                SELECT id, price, crawl_date FROM dataframe_moi_cao_ve
            """)
        logger.info("Đã cập nhật xong bảng lịch sử giá (price_history).")

        # Upsert : SCD type 1
        if spark.catalog.tableExists(table_name):
            logger.info(f"Bảng {table_name} ĐÃ TỒN TẠI. Tiến hành UPSERT (MERGE INTO)...")
            
            merge_query = f"""
                MERGE INTO {table_name} AS target
                USING dataframe_moi_cao_ve AS source
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
            logger.info("ĐÃ UPSERT BẰNG MERGE INTO THÀNH CÔNG!")
            
        else:
            logger.info(f"Bảng {table_name} CHƯA TỒN TẠI. Tiến hành tạo bảng lần đầu với PARTITION...")
            df.write \
                .format("iceberg") \
                .partitionBy("crawl_date") \
                .saveAsTable(table_name)
            logger.info("TẠO BẢNG LẦN ĐẦU KÈM PARTITION THÀNH CÔNG!")
            
    except Exception as e:
        logger.error(f"Lỗi khi ghi vào Iceberg: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    saved_raw_file = crawl_tiki_data(target_category_id=1520)
    load_to_iceberg(saved_raw_file)