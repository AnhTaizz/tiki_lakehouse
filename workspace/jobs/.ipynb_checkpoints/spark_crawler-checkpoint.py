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

# Import Spark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

logger = setup_logger(__name__)

"""Extract - Cào dữ liệu từ API"""
def crawl_tiki_data(target_category_id=1520):
    logger.info(f"=== BẮT ĐẦU CRAWL NGÀNH HÀNG ID {target_category_id} ===")
    
    # Lấy cây danh mục
    raw_cats = load_categories_from_api(category_id=target_category_id)
    if not raw_cats:
        logger.warning("API danh mục lỗi. Chuyển sang đọc từ file local backup...")
        backup_path = os.path.join(workspace_dir, 'data', 'lam_dep_suc_khoe_category.json')
        raw_cats = load_categories_from_file(backup_path)
        
    leaf_cats = get_leaf_categories(raw_cats)
    logger.info(f"Đã bóc tách được {len(leaf_cats)} danh mục lá.")
    
    all_products = []
    
    # MẸO: Nếu test, đổi thành leaf_cats[:2] để chạy 2 danh mục thôi cho nhanh.
    # Chạy thật thì để nguyên leaf_cats
    for idx, cat in enumerate(leaf_cats[:2]):
        logger.info(f"[{idx+1}/{len(leaf_cats[:2])}] Đang xử lý: {cat['name']}...")
        products = fetch_products_by_category(cat['id'], cat['url_key'])
        all_products.extend(products)
        
    # Lưu file thô (Raw/Bronze Layer) theo ngày
    today_str = datetime.now().strftime("%Y-%m-%d")
    raw_filename = f"tiki_skincare_raw_{today_str}.json"
    raw_filepath = os.path.join(workspace_dir, 'data', raw_filename)
    
    save_to_json(all_products, raw_filepath)
    logger.info(f"Đã cào xong {len(all_products)} sản phẩm. Lưu raw tại: {raw_filepath}")
    
    return raw_filepath
    
"""Load - Đọc JSON và đẩy vào Iceberg bằng Spark"""
def load_to_iceberg(raw_filepath):
    logger.info("=== BẮT ĐẦU ĐẨY DỮ LIỆU VÀO LAKEHOUSE (ICEBERG) ===")
    
    spark = SparkSession.builder \
        .appName("Tiki_Crawler_To_Iceberg") \
        .getOrCreate()
        
    # Định nghĩa Schema
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
    
    # Đọc file JSON
    df = spark.read.schema(schema).json(raw_filepath)
    
    # Thêm cột ngày crawl để dễ phân tích lịch sử giá (Time Travel / SCD Type 2)
    from pyspark.sql.functions import current_date
    df = df.withColumn("crawl_date", current_date())
    
    df.show(5, truncate=False)

    spark.sql("CREATE NAMESPACE IF NOT EXISTS local_catalog.tiki")
    # Định nghĩa tên bảng
    table_name = "local_catalog.tiki.products"
    
    try:
        logger.info(f"Đang ghi dữ liệu vào bảng Iceberg: {table_name}...")
        # Ghi đè (append) dữ liệu mới vào bảng Iceberg
        df.write \
            .format("iceberg") \
            .mode("append") \
            .save(table_name)
        logger.info("GHI ICEBERG THÀNH CÔNG!")
    except Exception as e:
        logger.error(f"Lỗi khi ghi vào Iceberg: {e}")
    finally:
        # Giải phóng tài nguyên Spark sau khi hoàn thành
        spark.stop()

if __name__ == "__main__":
    saved_raw_file = crawl_tiki_data(target_category_id=1520)
    load_to_iceberg(saved_raw_file)