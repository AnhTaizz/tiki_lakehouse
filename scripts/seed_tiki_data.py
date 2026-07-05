import os
import json
import time
import random
import logging
import requests
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CATEGORIES = {
    8322: "NhÃ  SÃ¡ch Tiki",
    1883: "NhÃ  Cá»­a - Äá»i Sá»‘ng",
    1789: "Äiá»‡n Thoáº¡i - MÃ¡y TÃ­nh Báº£ng",
    2549: "Äá»“ ChÆ¡i - Máº¹ & BÃ©",
    1815: "Thiáº¿t Bá»‹ Sá»‘ - Phá»¥ Kiá»‡n Sá»‘",
    1882: "Äiá»‡n Gia Dá»¥ng",
    1520: "LÃ m Äáº¹p - Sá»©c Khá»e",
    8594: "Ã” TÃ´ - Xe MÃ¡y - Xe Äáº¡p",
    931: "Thá»i trang ná»¯",
    4384: "BÃ¡ch HÃ³a Online",
    1975: "Thá»ƒ Thao - DÃ£ Ngoáº¡i",
    915: "Thá»i trang nam",
    17166: "Cross Border - HÃ ng Quá»‘c Táº¿",
    1846: "Laptop - MÃ¡y Vi TÃ­nh - Linh kiá»‡n",
    1686: "GiÃ y - DÃ©p nam",
    4221: "Äiá»‡n Tá»­ - Äiá»‡n Láº¡nh",
    1703: "GiÃ y - DÃ©p ná»¯",
    1801: "MÃ¡y áº¢nh - MÃ¡y Quay Phim",
    27498: "Phá»¥ kiá»‡n thá»i trang",
    44792: "NGON",
    8371: "Äá»“ng há»“ vÃ  Trang sá»©c",
    6000: "Balo vÃ  Vali",
    11312: "Voucher - Dá»‹ch vá»¥",
    976: "TÃºi thá»i trang ná»¯",
    27616: "TÃºi thá»i trang nam",
    15078: "ChÄƒm sÃ³c nhÃ  cá»­a"
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15"
]

def get_leaf_categories(parent_id, retries=3):
    """
    Calls Tiki API to get the category tree and recursively extracts all leaf node IDs.
    """
    url = f"https://tiki.vn/api/v2/categories?include=children&parent_id={parent_id}"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
    }
    
    for attempt in range(retries):
        try:
            time.sleep(random.uniform(1.0, 2.0))
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json().get("data", [])
                
                def extract_leaves(nodes):
                    leaves = []
                    for node in nodes:
                        if node.get("is_leaf") is True or not node.get("children"):
                            leaves.append((node.get("id"), node.get("name")))
                        else:
                            leaves.extend(extract_leaves(node.get("children")))
                    return leaves
                
                leaf_nodes = extract_leaves(data)
                logger.info(f"Extracted {len(leaf_nodes)} leaf categories for parent {parent_id}")
                return leaf_nodes
            
            logger.warning(f"Failed to fetch category tree (HTTP {response.status_code}). Retry {attempt+1}/{retries}")
        except Exception as e:
            logger.error(f"Error fetching category tree: {e}")
            
    logger.error(f"Could not fetch leaf categories for {parent_id}. Returning root ID only.")
    return [(parent_id, "Root")]

def process_single_leaf(leaf_id, leaf_name, root_cat_id, root_cat_name, leaf_idx, total_leaves):
    """Worker function to fetch all pages for a single leaf category."""
    leaf_products = []
    page = 1
    max_retries = 3
    logger.info(f"--- [Thread {leaf_idx}/{total_leaves}] Started Crawling Leaf: {leaf_name} (ID: {leaf_id}) ---")

    while True:
        url = f"https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&category={leaf_id}&page={page}"
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://tiki.vn/",
        }

        success = False
        for attempt in range(max_retries):
            try:
                # Ngá»§ ngáº«u nhiÃªn 4.0 - 8.0 giÃ¢y Ä‘á»ƒ trÃ¡nh bá»‹ Akamai block khi crawl trá»±c tiáº¿p
                sleep_time = random.uniform(4.0, 8.0)
                time.sleep(sleep_time)

                response = requests.get(url, headers=headers, timeout=15)
                if response.status_code == 200:
                    data = response.json()
                    items = data.get("data", [])
                    
                    if not items:
                        logger.info(f"[Thread {leaf_idx}] Reached end of leaf {leaf_name} at page {page}.")
                        success = True
                        break
                    
                    # OVERRIDE with ROOT CATEGORY INFO
                    for item in items:
                        item["category_id"] = root_cat_id
                        item["category_name"] = root_cat_name
                        leaf_products.append(item)
                    
                    logger.info(f"[Thread {leaf_idx}] Fetched {len(items)} items from {leaf_name} (Page {page}).")
                    page += 1
                    success = True
                    break
                elif response.status_code == 429:
                    logger.warning(f"[Thread {leaf_idx}] Rate limited (429) on {leaf_name} page {page}. Sleeping 10s...")
                    time.sleep(10)
                else:
                    logger.warning(f"[Thread {leaf_idx}] HTTP {response.status_code} on {leaf_name} page {page}. Attempt {attempt+1}/{max_retries}")
                    
            except Exception as e:
                logger.error(f"[Thread {leaf_idx}] Error calling {url}: {e}")
                time.sleep(5)
                
        if not success:
            logger.error(f"[Thread {leaf_idx}] Failed to fetch {leaf_name} page {page} after {max_retries} attempts. ABORTING TASK.")
            raise RuntimeError("API Error or IP Blocked. Aborting.")
            
        if success and 'items' in locals() and not items:
            break
            
    return leaf_products

def crawl_category(root_cat_id, root_cat_name, output_dir):
    """Crawls all leaf categories using multiple threads and aggregates them."""
    output_file = os.path.join(output_dir, f"mock_category_{root_cat_id}.json")
    
    if os.path.exists(output_file):
        logger.info(f"File {output_file} already exists. Skipping root category {root_cat_name}.")
        return

    logger.info(f"========== Starting crawl for ROOT: {root_cat_name} (ID: {root_cat_id}) ==========")
    leaf_categories = get_leaf_categories(root_cat_id)
    all_products = []
    
    # Run 1 thread sequentially for leaf categories (Safe Mode)
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = []
        for idx, (leaf_id, leaf_name) in enumerate(leaf_categories, 1):
            futures.append(
                executor.submit(
                    process_single_leaf, 
                    leaf_id, leaf_name, root_cat_id, root_cat_name, idx, len(leaf_categories)
                )
            )
            
        for future in as_completed(futures):
            try:
                products = future.result()
                if products:
                    all_products.extend(products)
            except Exception as exc:
                logger.error(f"A leaf crawler thread generated an exception: {exc}. Crashing script to mark Airflow task as failed.")
                
                # Cancel all pending tasks to prevent them from executing
                for f in futures:
                    f.cancel()
                
                # Shutdown executor immediately
                try:
                    executor.shutdown(wait=False, cancel_futures=True)
                except TypeError:
                    # Fallback for Python < 3.9
                    executor.shutdown(wait=False)
                    
                raise exc

    # Save aggregated data for the Root Category
    if all_products:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_products, f, ensure_ascii=False, indent=2)
        logger.info(f"========== Saved TOTAL {len(all_products)} products to {output_file} ==========")
    else:
        logger.warning(f"No products found for {root_cat_name}. Skipping save.")

def main():
    parser = argparse.ArgumentParser(description="Tiki Multi-threaded Category Crawler")
    parser.add_argument("--category_ids", type=str, help="Comma-separated category IDs to crawl. If empty, crawls all 26.")
    parser.add_argument("--force_full", action="store_true", help="Bypass the confirmation prompt and force a full crawl.")
    args = parser.parse_args()

    base_dir = "/opt/airflow/data/mock_data" if os.path.exists("/opt/airflow/data") else os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "mock_data"))
    os.makedirs(base_dir, exist_ok=True)
    logger.info(f"Saving output JSON files to {base_dir}")

    # Determine which categories to process based on CLI arg
    target_cats = {}
    if args.category_ids:
        ids_to_crawl = [int(cid.strip()) for cid in args.category_ids.split(",") if cid.strip()]
        for cid in ids_to_crawl:
            if cid in CATEGORIES:
                target_cats[cid] = CATEGORIES[cid]
            else:
                logger.warning(f"Category ID {cid} is not in the predefined 26 root categories. Skipping.")
    else:
        if args.force_full:
            logger.warning("PROCEEDING WITH FULL CRAWL via --force_full flag. Godspeed!")
            target_cats = CATEGORIES
        else:
            # Prevent IP Ban for users cloning the repo
            print("\n===========================================================================")
            print("WARNING: FULL CRAWL INITIATED")
            print("You are about to crawl ALL 26 categories (approx. 180,000+ items).")
            print("This process will take roughly 8-12 hours.")
            print("There is a HIGH RISK that Tiki's Anti-Bot system will permanently BAN your IP.")
            print("===========================================================================")
            choice = input("Do you want to proceed with FULL MODE? [y/N] (Press N for Safe Demo Mode): ")
            if choice.strip().lower() == 'y':
                target_cats = CATEGORIES
                logger.warning("PROCEEDING WITH FULL CRAWL. Godspeed!")
            else:
                print("\nFalling back to SAFE DEMO MODE (2 small categories only).")
                target_cats = {
                    1789: CATEGORIES[1789],    # Phones (~150 items)
                    17166: CATEGORIES[17166]   # Cross Border (~30 items)
                }

    logger.info(f"Will crawl the following {len(target_cats)} categories: {list(target_cats.values())}")

    for cat_id, cat_name in target_cats.items():
        crawl_category(cat_id, cat_name, base_dir)
        
    logger.info("Chunk crawl completed successfully!")

if __name__ == "__main__":
    main()
