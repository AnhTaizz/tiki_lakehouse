import os
import sys
from datetime import datetime

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
    for index, category in enumerate(leaf_categories):
        logger.info("[%s/%s] Processing: %s", index + 1, len(leaf_categories), category["name"])
        products = fetch_products_by_category(category["id"], category["url_key"])
        all_products.extend(products)

    today = datetime.now().strftime("%Y-%m-%d")
    raw_filename = f"tiki_beauty_health_raw_{today}.json"
    raw_filepath = os.path.join(project_dir, "data", raw_filename)

    save_to_json(all_products, raw_filepath)
    logger.info("Finished crawling %s products. Saved to %s", len(all_products), raw_filepath)

    print(raw_filepath)
    return raw_filepath


if __name__ == "__main__":
    crawl_tiki_data()
