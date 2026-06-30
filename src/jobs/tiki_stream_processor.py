import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.utils import setup_logger
from jobs.tiki_load_iceberg import build_product_schema, load_bronze, load_silver_history, load_silver_active

logger = setup_logger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = "tiki.stream.products"
CHECKPOINT_LOCATION = "/home/jovyan/work/data/checkpoints/tiki_stream"

def build_streaming_schema():
    from pyspark.sql.types import StringType
    schema = build_product_schema()
    # Schema gốc không có crawl_date (vì batch lấy từ tên file)
    # Nhưng bên streaming ta lấy crawl_date từ trong file JSON (Simulator)
    schema.add("crawl_date", StringType(), True)
    schema.add("_event_type", StringType(), True)
    return schema

def process_micro_batch(df_batch, epoch_id):
    logger.info("Processing Micro-Batch %s (Spark Structured Streaming)", epoch_id)

    if df_batch.isEmpty():
        logger.info("Micro-batch is empty. Waiting for events...")
        return

    from pyspark.sql.functions import lit, current_timestamp, to_date, col

    # 1. Thêm các cột metadata như mô hình Batch
    df_new = (
        df_batch
        .withColumn("source_file", lit("kafka_realtime_stream"))
        .withColumn("loaded_at", current_timestamp())
        .withColumn("crawl_date", to_date(col("crawl_date")))
    ).dropDuplicates(["id"])

    count = df_new.count()
    logger.info("Received %d unique products in this micro-batch.", count)

    # 2. Tái sử dụng lại 100% logic Medallion Architecture của code cũ!
    spark = df_batch.sparkSession

    try:
        # Iceberg schema không có cột _event_type (cột này chỉ dùng cho luồng Streaming -> Postgres)
        # Nên ta phải drop nó ra trước khi nạp vào Iceberg để tránh lỗi TOO_MANY_DATA_COLUMNS
        df_iceberg = df_new.drop("_event_type")

        load_bronze(spark, df_iceberg)
        load_silver_history(spark, df_iceberg)
        load_silver_active(spark, df_iceberg)

        # 3. SPEED LAYER (Ghi thẳng vào Postgres để vẽ biểu đồ Real-time trên Superset)
        reporting_db_host = os.environ.get("REPORTING_DB_HOST", "reporting-postgres")
        jdbc_url = f"jdbc:postgresql://{reporting_db_host}:5432/reporting"
        jdbc_props = {
            "user": "reporting",
            "password": "reporting123",
            "driver": "org.postgresql.Driver",
        }

        # Trích xuất các trường dữ liệu quan trọng để Superset đọc cho nhẹ
        df_realtime = df_new.select(
            "id", "name", "price", "discount_rate",
            "quantity_sold", "loaded_at", "_event_type"
        )

        df_realtime.write.jdbc(
            url=jdbc_url,
            table="realtime_events",
            mode="append",
            properties=jdbc_props,
        )

        logger.info("Micro-Batch %s successfully written to Iceberg (Storage) & Postgres (Speed Layer).", epoch_id)
    except Exception as e:
        logger.error("Error processing Micro-Batch %s: %s", epoch_id, e, exc_info=True)
        # Ném lỗi để Spark Streaming biết batch này failed và sẽ retry
        raise e

def start_streaming():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col

    # Cấu hình Spark Session hỗ trợ Iceberg
    spark = SparkSession.builder.appName("Tiki_Realtime_Processor").getOrCreate()

    logger.info("Connecting to Kafka: %s, Topic: %s", KAFKA_BROKER, KAFKA_TOPIC)

    # Khởi tạo Kafka ReadStream
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Giải mã JSON
    schema = build_streaming_schema()
    df_parsed = df_kafka.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Kích hoạt ForeachBatch để đổ dữ liệu vào Iceberg
    query = df_parsed.writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="10 seconds") \
        .start()

    logger.info("TIKI STREAM PROCESSOR STARTED! Waiting for events...")
    query.awaitTermination()

if __name__ == "__main__":
    start_streaming()
