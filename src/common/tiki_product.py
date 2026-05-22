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


def fetch_products_by_category(category_id, url_key, max_pages=None):
    client = HttpClient()
    api_url = "https://tiki.vn/api/personalish/v1/blocks/listings"

    all_products = []
    seen_ids = set()

    current_page = 1
    last_page = 1

    while True:
        if max_pages and current_page > max_pages:
            logger.info("Reached max_pages=%s. Stop crawling this category.", max_pages)
            break

        params = {
            "limit": 40,
            "include": "advertisement",
            "aggregations": 2,
            "version": "home-persionalized",
            "category": category_id,
            "urlKey": url_key,
            "page": current_page,
        }

        logger.info(
            "Category %s (%s) - fetching page %s/%s",
            category_id,
            url_key,
            current_page,
            last_page,
        )

        data = client.get(api_url, params=params)

        if not data:
            logger.error(
                "API failed for category %s, page %s. Stop this category.",
                category_id,
                current_page,
            )
            break

        items = data.get("data", [])
        if not items:
            logger.info("No more products on page %s. Finished category %s.", current_page, category_id)
            break

        for item in items:
            product_id = item.get("id")
            if product_id in seen_ids:
                continue
            seen_ids.add(product_id)

            quantity_sold = item.get("quantity_sold")
            sold_value = 0
            if isinstance(quantity_sold, dict):
                sold_value = quantity_sold.get("value", 0)
            elif isinstance(quantity_sold, int):
                sold_value = quantity_sold

            all_products.append(
                {
                    "id": product_id,
                    "sku": item.get("sku"),
                    "name": item.get("name"),
                    "url_key": item.get("url_key"),
                    "price": item.get("price", 0),
                    "original_price": item.get("original_price") or item.get("list_price", 0),
                    "discount": item.get("discount", 0),
                    "discount_rate": item.get("discount_rate", 0),
                    "brand_name": item.get("brand_name", "No Brand"),
                    "rating_average": item.get("rating_average", 0),
                    "review_count": item.get("review_count", 0),
                    "thumbnail_url": item.get("thumbnail_url", ""),
                    "category_id": category_id,
                    "quantity_sold": sold_value,
                }
            )

        paging = data.get("paging", {})
        last_page = paging.get("last_page", 1)

        if current_page >= last_page:
            logger.info("Reached last page (%s). Finished category %s.", last_page, category_id)
            break

        current_page += 1
        time.sleep(2)

    return all_products
