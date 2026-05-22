# filepath: /workspace/libs/utils.py
import json
import logging

"""Cấu hình logger để theo dõi tiến trình chạy thay cho print"""
def setup_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger

"""Lưu dữ liệu dạng list/dict ra file JSON"""
def save_to_json(data, filepath):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)