import json
import logging
import os
import random
import time
from datetime import datetime

from kafka import KafkaProducer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC  = "tiki.raw.products"
SOURCE_FILE  = "tiki_products_raw_2026-06-28.json"

def get_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"),
            retries=3,
        )
        return producer
    except Exception as e:
        logger.error("Failed to connect to Kafka at %s: %s", KAFKA_BROKER, e)
        return None

def load_source_data():
    """Đọc file JSON lớn nhất vào RAM làm dữ liệu nguồn."""
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    filepath = os.path.join(data_dir, SOURCE_FILE)

    if not os.path.exists(filepath):
        logger.error("Source file not found: %s", filepath)
        return []

    logger.info("Loading massive data source: %s ... (This may take a few seconds)", SOURCE_FILE)
    with open(filepath, "r", encoding="utf-8") as f:
        products = json.load(f)

    logger.info("Loaded %d products into RAM.", len(products))
    return products

def mutate_product(product):
    """
    Sự kiện Event Engine: Xáo trộn dữ liệu ngẫu nhiên
    để tạo sự kiện thay đổi giá (SCD Type 4) và số lượng bán.
    """
    import copy
    p = copy.deepcopy(product)

    # Cập nhật thời gian thực
    p["crawl_date"] = datetime.now().strftime("%Y-%m-%d")

    dice = random.random()

    if dice < 0.10:
        # Sự kiện Flash Sale (10% cơ hội): Giảm giá 5-10%
        discount_percent = random.uniform(0.05, 0.10)
        current_price = p.get("price", 0)
        if current_price > 0:
            new_price = int(current_price * (1 - discount_percent))
            # Tính lại discount rate
            original_price = p.get("original_price", current_price)
            p["price"] = new_price
            p["discount"] = original_price - new_price
            p["discount_rate"] = int((p["discount"] / original_price) * 100) if original_price > 0 else 0
            p["_event_type"] = "FLASH_SALE"

    elif dice < 0.40:
        # Sự kiện Có người mua hàng (30% cơ hội): Tăng quantity_sold
        sold_increase = random.randint(1, 5)
        current_sold = p.get("quantity_sold")
        # Đôi khi tiki trả về quantity_sold dạng dictionary, phải check cẩn thận
        if isinstance(current_sold, dict):
            current_sold = current_sold.get("value", 0)
        elif not isinstance(current_sold, (int, float)):
            current_sold = 0

        p["quantity_sold"] = int(current_sold) + sold_increase
        p["_event_type"] = "PURCHASE"

    else:
        # Bình thường (60% cơ hội): Dữ liệu không đổi (chỉ là luồng quét cập nhật state)
        p["_event_type"] = "PING"

    return p

def run_simulator():
    products = load_source_data()
    if not products:
        return

    # Chỉ lấy danh mục Làm Đẹp - Sức Khỏe (ID: 1520) để mô phỏng (khoảng 11k sản phẩm)
    # Điều này giúp dung lượng mỗi mẻ chạy vừa phải, đúng chuẩn ETL Batch.
    target_products = [p for p in products if p.get("category_id") == 1520]
    logger.info("Found %d products for category 1520 (Làm Đẹp - Sức Khỏe)", len(target_products))

    producer = get_kafka_producer()
    if not producer:
        return

    logger.info("🚀 TIKI BATCH SIMULATOR STARTED!")
    logger.info("Pushing %d simulated events to topic: %s", len(target_products), KAFKA_TOPIC)

    events_sent = 0
    flash_sales = 0
    purchases = 0

    try:
        for base_product in target_products:
            if not base_product.get("id"):
                continue

            mutated_product = mutate_product(base_product)

            if mutated_product["_event_type"] == "FLASH_SALE":
                flash_sales += 1
            elif mutated_product["_event_type"] == "PURCHASE":
                purchases += 1

            producer.send(
                KAFKA_TOPIC,
                key=str(mutated_product["id"]).encode("utf-8"),
                value=mutated_product
            )
            events_sent += 1

        producer.flush()
        logger.info("✅ Batch completed! Sent %d events.", events_sent)
        logger.info("📊 Stats: %d Flash Sales, %d Purchases generated.", flash_sales, purchases)

    except Exception as e:
        logger.error("❌ Simulator failed: %s", e)
        sys.exit(1)
    finally:
        if producer:
            producer.close()

if __name__ == "__main__":
    run_simulator()
