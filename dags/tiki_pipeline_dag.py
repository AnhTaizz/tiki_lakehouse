import os
from datetime import datetime, timedelta
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator


local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    "owner": "anhtaizz",
    "depends_on_past": False,
    "start_date": datetime(2026, 5, 20, tzinfo=local_tz),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    "tiki_beauty_lakehouse_pipeline",
    default_args=default_args,
    description="Pipeline crawl Tiki Beauty data and load to Medallion Iceberg tables",
    schedule_interval="0 15 * * *",
    catchup=False,
    tags=["tiki", "lakehouse", "beauty"],
) as dag:
    extract_task = BashOperator(
        task_id="extract_tiki_data",
        env={
            **os.environ,
            "PYTHONPATH": "/opt/airflow/src",
        },
        bash_command="python /opt/airflow/src/jobs/tiki_extract.py",
        do_xcom_push=True,
    )

    load_medallion_task = BashOperator(
        task_id="load_medallion_iceberg",
        env={
            **os.environ,
            "PYTHONPATH": "/opt/airflow/src",
        },
        bash_command="""
            RAW_PATH="{{ ti.xcom_pull(task_ids='extract_tiki_data') }}"
            FILENAME=$(basename "$RAW_PATH")
            docker exec tiki_spark_crawler /opt/conda/bin/python /home/jovyan/work/src/jobs/tiki_load_iceberg.py --raw_file /home/jovyan/work/data/$FILENAME
        """,
    )

    extract_task >> load_medallion_task
