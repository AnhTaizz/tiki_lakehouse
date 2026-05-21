# filepath: /workspace/dags/tiki_pipeline_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner' : 'anhtaizz',
    'depends_on_past' : False,
    'start_date' : datetime(2026, 5, 20),
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=2),
}

with DAG(
    'tiki_beauty_lakehouse_pipeline',
    default_args=default_args,
    description='Pipeline crawl data from Tiki Beauty Category and add to Iceberg',
    schedule_interval='0 2 * * *',   # Auto crawl data on 2 am
    catchup=False,
    tags=['tiki', 'lakehouse', 'beauty'],
) as dag:

    # 3. Định nghĩa Task 1: Extract (Cào dữ liệu)
    # Thuộc tính do_xcom_push=True sẽ bắt dòng lệnh "print(raw_filepath)" ở cuối file
    # tiki_extract.py và lưu đường dẫn đó vào một bộ nhớ tạm gọi là XCom.

    extract_task = BashOperator(
        task_id='extract_tiki_data',
        bash_command='python /opt/airflow/workspace/jobs/tiki_extract.py',
        do_xcom_push=True
    )

    # 4. Định nghĩa Task 2: Load (Đẩy vào Iceberg)
    # Cú pháp {{ ti.xcom_pull(...) }} là Jinja Template của Airflow.
    # Nó sẽ tự động moi đường dẫn file từ Task 1 ra và điền vào tham số --raw_file

    load_task = BashOperator(
        task_id='load_to_iceberg',
        bash_command='python /opt/airflow/workspace/jobs/tiki_load_iceberg.py --raw_file {{ ti.xcom_pull(task_ids="extract_tiki_data")}}'
    )

    # 5. Thiết lập luồng chạy (Dependencies)
    # Dấu >> nghĩa là Task 1 phải chạy XONG và THÀNH CÔNG thì Task 2 mới được phép chạy.
    extract_task >> load_task