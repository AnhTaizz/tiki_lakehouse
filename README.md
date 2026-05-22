# Tiki Data Lakehouse

Local data lakehouse project for crawling Tiki product data and loading raw rows into a Bronze Apache Iceberg table on top of MinIO, Hive Metastore, PostgreSQL, Spark, and Airflow.

## Architecture

- Ingestion: Python jobs crawl Tiki category and product APIs.
- Compute: Spark runs in `jupyter/pyspark-notebook:spark-3.5.0`.
- Storage: MinIO provides an S3-compatible warehouse.
- Catalog: Hive Metastore stores Iceberg table metadata, backed by PostgreSQL.
- Orchestration: Airflow schedules daily extract and Bronze load jobs.
- Current pipeline scope: crawl -> local raw JSON file -> Bronze Iceberg append table.

## Project Layout

- `src/common`: shared API clients and helpers.
- `src/jobs`: executable extract/load jobs.
- `dags`: Airflow DAG definitions.
- `config`: Spark and Hadoop configuration files.
- `data`: local seed files and generated raw extracts.
- `notebooks`: exploratory notebooks.

## Quick Start

1. Copy `.env.example` to `.env` and adjust local credentials if needed.
2. Start the stack:

   ```bash
   docker compose up -d --build
   ```

3. Open the services:

   - Jupyter: http://localhost:8888
   - MinIO Console: http://localhost:9001
   - Airflow: http://localhost:8081

Default local Airflow login is `admin` / `password123`. MinIO credentials come from `.env`.

## Main Commands

Run extract locally with the project source on `PYTHONPATH`:

```bash
PYTHONPATH=src python src/jobs/tiki_extract.py
```

Run Bronze load inside the Spark container:

```bash
docker exec tiki_spark_crawler /opt/conda/bin/python /home/jovyan/work/src/jobs/tiki_load_iceberg.py --raw_file /home/jovyan/work/data/<raw-file>.json
```

## Notes

- `data/tiki_category.json` and `data/lam_dep_suc_khoe_category.json` are seed/backup category files.
- Generated raw extract files are ignored by git.
- Spark uses the `local_catalog` Iceberg catalog and appends raw rows to `local_catalog.tiki_bronze.products_raw`.
- Silver/Gold transformations, upserts, and price history are intentionally deferred while the project focuses on the crawl and Airflow scheduling flow.
