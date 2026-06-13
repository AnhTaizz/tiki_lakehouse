"""
superset_config.py
==================
Superset configuration — đọc credentials từ environment variables.
File này được mount vào container qua docker-compose volume.

Superset KHÔNG đọc SQLALCHEMY_DATABASE_URI từ env var trực tiếp —
phải khai báo trong file Python config này.
"""

import os

# ---------------------------------------------------------------------------
# Security — bắt buộc, Superset không start nếu thiếu
# ---------------------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "tiki_superset_secret_2026")

# ---------------------------------------------------------------------------
# Internal database — nơi Superset lưu dashboards, charts, users, permissions
# Trỏ vào superset-postgres container (không phải reporting-postgres)
# ---------------------------------------------------------------------------
_pg_user = os.environ.get("SUPERSET_DB_USER", "superset")
_pg_pass = os.environ.get("SUPERSET_DB_PASSWORD", "superset123")
_pg_host = os.environ.get("SUPERSET_DB_HOST", "superset-postgres")
_pg_port = os.environ.get("SUPERSET_DB_PORT", "5432")
_pg_name = os.environ.get("SUPERSET_DB_NAME", "superset")

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{_pg_user}:{_pg_pass}@{_pg_host}:{_pg_port}/{_pg_name}"
)

# ---------------------------------------------------------------------------
# Cache — dùng SimpleCache cho local dev (không cần Redis)
# ---------------------------------------------------------------------------
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}

# ---------------------------------------------------------------------------
# Feature flags
# ---------------------------------------------------------------------------
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,
}

# ---------------------------------------------------------------------------
# Tắt CSRF cho local dev — bật lại khi deploy production
# ---------------------------------------------------------------------------
WTF_CSRF_ENABLED = False
