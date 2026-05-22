import json
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
if src_dir not in sys.path:
    sys.path.append(src_dir)

from common.http_client import HttpClient
from common.utils import setup_logger


logger = setup_logger(__name__)


def load_categories_from_file(filepath):
    with open(filepath, "r", encoding="utf-8") as file:
        raw_data = json.load(file)
    return raw_data.get("data", [])


def load_categories_from_api(category_id=None):
    client = HttpClient()

    base_url = "https://tiki.vn/api/v2/categories"
    params = {"include": "children"}
    if category_id:
        params["parent_id"] = category_id

    label = category_id if category_id else "root"
    logger.info("Loading category tree from Tiki API (ID: %s)", label)
    response_data = client.get(base_url, params=params)

    if response_data:
        return response_data.get("data", [])

    logger.error("Could not load categories from API.")
    return []


def get_leaf_categories(categories):
    leaf_nodes = []
    for category in categories:
        if category.get("is_leaf") is True or not category.get("children"):
            leaf_nodes.append(
                {
                    "id": category.get("id"),
                    "name": category.get("name"),
                    "url_key": category.get("url_key"),
                }
            )
        else:
            leaf_nodes.extend(get_leaf_categories(category["children"]))
    return leaf_nodes
