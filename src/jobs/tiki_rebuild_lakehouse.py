import os
import glob
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, regexp_extract, col, to_date

# Import functions from tiki_load_iceberg
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

from tiki_load_iceberg import load_bronze, load_silver_history, load_silver_active, build_product_schema

def rebuild():
    print("Starting Tiki Lakehouse Rebuild...")
    spark = SparkSession.builder.appName("Rebuild_Tiki_Lakehouse").getOrCreate()
    
    # 1. Xóa toàn bộ bảng cũ
    tables_to_drop = [
        "local_catalog.tiki_bronze.products_raw",
        "local_catalog.tiki_silver.price_history",
        "local_catalog.tiki_silver.products"
    ]
    for tbl in tables_to_drop:
        print(f"Dropping {tbl}...")
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
        
    # 2. Lấy danh sách file raw JSON và sắp xếp theo ngày (Chỉ lấy tháng 6)
    data_dir = "/home/jovyan/work/data"
    files = glob.glob(os.path.join(data_dir, "tiki_beauty_health_raw_2026-06-*.json"))
    files.sort() # Sắp xếp tăng dần theo thời gian
    
    schema = build_product_schema()
    
    # 3. Load tuần tự từng file vào Iceberg
    for f in files:
        print(f"Processing {os.path.basename(f)}...")
        df_new = spark.read.option("multiline", "true").schema(schema).json(f)
        df_new = (
            df_new.withColumn("source_file", lit(os.path.basename(f)))
            .withColumn("loaded_at", current_timestamp())
            .withColumn("crawl_date", to_date(regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1)))
        )
        
        load_bronze(spark, df_new)
        load_silver_history(spark, df_new)
        load_silver_active(spark, df_new)
        
    print("Rebuild Silver & Bronze Completed!")
    print("Next step: Run transform_gold to update Superset dashboards.")
    spark.stop()

if __name__ == "__main__":
    rebuild()
