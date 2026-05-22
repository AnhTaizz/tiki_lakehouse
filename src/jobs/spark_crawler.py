from tiki_extract import crawl_tiki_data
from tiki_load_iceberg import load_to_iceberg


if __name__ == "__main__":
    saved_raw_file = crawl_tiki_data(target_category_id=1520)
    load_to_iceberg(saved_raw_file)
