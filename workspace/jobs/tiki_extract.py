import json
import os
import sys
from datetime import datetime

current_dir = os.path.dirname(os.path.abspath(__file__))
workspace_dir = os.path.abspath(os.path.join(current_dir, '..'))

from libs.tiki_category import load_categories_from_api, load_categories_from_file, get_leaf_categories
from libs.tiki_product import fetch_products_by_category
from libs.utils import setup_logger, save_to_json

logger = setup_logger(__name__)

def crawl_tiki_data(target_category_id=1520):
    logger.info(f"Crawl ngành hàng có id là {target_category_id}")

    raw_cats = load_categories_from_api(category_id = target_category_id)
    if not raw_cats:
        logger.warning("API category lỗi, chuyển sang đọc file local Backup")
        backup_path = os.path.join(workspace_dir, 'data', 'tiki_category.json')
        raw_cats = load_categories_from_file(backup_path)

    leaf_cats = get_leaf_categories(raw_cats)
    logger.info(f"Đã bóc tách được {len(leaf_cats)} danh mục lá")

    all_products = []

    for idx, cat in enumerate(leaf_cats):
        logger.info(f"[{idx+1}/{len(leaf_cats)}] Đang xử lý: {cat['name']}...")
        products = fetch_products_by_category(cat['id'], cat['url_key'])
        all_products.extend(products)

    today_str = datetime.now().strftime("%Y-%m-%d")
    raw_filename = f"tiki_beauty_health_raw_{today_str}.json"
    raw_filepath = os.path.join(workspace_dir, 'data', raw_filename)

    save_to_json(all_products, raw_filepath)
    logger.info(f"Đã crawl xong {len(all_products)} sản phẩm. Lưu tại {raw_filepath}")

    print(raw_filepath)

if __name__ == "__main__":
    crawl_tiki_data()
        