FROM apache/superset:latest

# Superset venv được tạo bởi uv (không có pip) — phải dùng uv pip
USER root
RUN uv pip install psycopg2-binary --python /app/.venv

USER superset
