import os
import sys
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
project_dir = os.path.abspath(os.path.join(src_dir, ".."))

if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.tiki_category import load_categories_from_api, load_categories_from_file, get_leaf_categories
from common.tiki_product import fetch_products_by_category
from common.utils import save_to_json, setup_logger


logger = setup_logger(__name__)


def crawl_tiki_data(target_category_id=1520):
    logger.info("Start crawling category id %s", target_category_id)

    raw_categories = load_categories_from_api(category_id=target_category_id)
    if not raw_categories:
        logger.warning("Category API failed, using local backup file")
        backup_path = os.path.join(project_dir, "data", "tiki_category.json")
        raw_categories = load_categories_from_file(backup_path)

    leaf_categories = get_leaf_categories(raw_categories)
    logger.info("Found %s leaf categories", len(leaf_categories))

    all_products = []

    def process_category(category, index, total):
        logger.info("[%s/%s] Processing: %s", index + 1, total, category["name"])
        try:
            products = fetch_products_by_category(category["id"], category["url_key"])
            for p in products:
                p["category_name"] = category["name"]
            return products
        except Exception as e:
            logger.error("Error fetching category %s: %s", category["name"], e)
            return []

    # Dùng ThreadPoolExecutor để cào song song 10 ngành hàng cùng lúc
    logger.info("Starting concurrent extraction with 10 workers...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(process_category, cat, idx, len(leaf_categories))
            for idx, cat in enumerate(leaf_categories)
        ]

        for future in as_completed(futures):
            all_products.extend(future.result())

    today = datetime.now().strftime("%Y-%m-%d")
    raw_filename = f"tiki_beauty_health_raw_{today}.json"
    raw_filepath = os.path.join(project_dir, "data", raw_filename)

    save_to_json(all_products, raw_filepath)
    logger.info("Finished crawling %s products. Saved to %s", len(all_products), raw_filepath)

    print(raw_filepath)
    return raw_filepath


if __name__ == "__main__":
    crawl_tiki_data()
