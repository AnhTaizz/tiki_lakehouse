import os
import sys
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.http_client import HttpClient
from common.utils import setup_logger


logger = setup_logger(__name__)


import concurrent.futures

def fetch_products_by_category(category_id, url_key, max_pages=None):
    client = HttpClient()
    api_url = "https://tiki.vn/api/personalish/v1/blocks/listings"

    all_products = []
    seen_ids = set()

    def fetch_page(page):
        params = {
            "limit": 40,
            "include": "advertisement",
            "aggregations": 2,
            "version": "home-persionalized",
            "category": category_id,
            "urlKey": url_key,
            "page": page,
        }

        logger.info("Category %s (%s) - fetching page %s", category_id, url_key, page)
        data = client.get(api_url, params=params)

        if not data:
            logger.error("API failed for category %s, page %s", category_id, page)
            return [], 1

        items = data.get("data", [])
        page_products = []

        for item in items:
            quantity_sold = item.get("quantity_sold")
            sold_value = 0
            if isinstance(quantity_sold, dict):
                sold_value = quantity_sold.get("value", 0)
            elif isinstance(quantity_sold, int):
                sold_value = quantity_sold

            page_products.append(
                {
                    "id": item.get("id"),
                    "sku": item.get("sku"),
                    "name": item.get("name"),
                    "url_key": item.get("url_key"),
                    "price": item.get("price", 0),
                    "original_price": item.get("original_price") or item.get("list_price", 0),
                    "discount": item.get("discount", 0),
                    "discount_rate": item.get("discount_rate", 0),
                    "brand_name": item.get("brand_name") or "No Brand",
                    "rating_average": item.get("rating_average", 0),
                    "review_count": item.get("review_count", 0),
                    "thumbnail_url": item.get("thumbnail_url", ""),
                    "category_id": category_id,
                    "quantity_sold": sold_value,
                }
            )

        paging = data.get("paging", {})
        last_page = paging.get("last_page", 1)

        import random
        time.sleep(0.01)  # small delay
        return page_products, last_page

    # 1. Fetch page 1 sequentially to get last_page
    first_page_products, last_page = fetch_page(1)
    for p in first_page_products:
        if p["id"] not in seen_ids:
            seen_ids.add(p["id"])
            all_products.append(p)

    if max_pages and last_page > max_pages:
        logger.info("Capping last_page from %s to max_pages=%s", last_page, max_pages)
        last_page = max_pages

    # 2. Fetch remaining pages concurrently
    if last_page > 1:
        logger.info("Category %s has %s pages. Starting concurrent fetch for %s pages...", category_id, last_page, last_page - 1)
        # Tối ưu hóa: Dùng 15 luồng để lật trang cùng lúc
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            future_to_page = {
                executor.submit(fetch_page, page): page
                for page in range(2, last_page + 1)
            }

            for future in concurrent.futures.as_completed(future_to_page):
                page = future_to_page[future]
                try:
                    page_products, _ = future.result()
                    for p in page_products:
                        if p["id"] not in seen_ids:
                            seen_ids.add(p["id"])
                            all_products.append(p)
                except Exception as exc:
                    logger.error("Page %s generated an exception: %s", page, exc)

    logger.info("FINISHED Category %s (%s) - Extracted a total of %d products.", category_id, url_key, len(all_products))
    return all_products
