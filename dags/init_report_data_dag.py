from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='init_historical_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['init', 'bronze'],
) as dag:

    ingest_csv = BashOperator(
        task_id='ingest_historical_csv_to_bronze',
        bash_command='docker exec -e MINIO_HOST=minio:9000 -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin python_container python bronze/ingest_historical.py',
    )

    # crawl_reports = BashOperator(
    #     task_id='crawl_report_excel_to_bronze',
    #     bash_command='docker exec -e MINIO_HOST=minio:9000 -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin python_container python bronze/crawl_and_load_report_excel_files_to_bronze.py',
    # )

    [ingest_csv]
