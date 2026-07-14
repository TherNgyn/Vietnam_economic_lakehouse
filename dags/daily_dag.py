import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from datetime import datetime, timedelta
import pendulum
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
def validate_daily_bronze(**context):
    from minio import Minio
    from datetime import datetime

    client = Minio(
        os.getenv('MINIO_HOST', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False,
    )
    today = datetime.utcnow().strftime('%Y-%m-%d')
    checks = [
        ('bronze', 'daily/economics/interest_rate'),
        ('bronze', 'daily/currency'),
        ('bronze', 'daily/world_index'),
        ('bronze', 'daily/product'),
    ]
    status = {}
    for bucket, prefix in checks:
        try:
            objects = list(client.list_objects(bucket, prefix=prefix, recursive=False))
            has_today = any(today in obj.object_name for obj in objects)
            status[prefix] = 'OK' if has_today else 'missing today partition'
        except Exception as e:
            status[prefix] = f'ERROR: {str(e)[:60]}'
    context['task_instance'].xcom_push(key='bronze_status', value=status)
    return status


with DAG(
    dag_id='daily_ingestion',
    
    schedule='0 7 * * *',
    catchup=False,
    default_args={
        'owner': 'data-engineer',
        'retries': 2,
        'retry_delay': timedelta(minutes=5),
        'start_date': datetime(2025, 1, 1, tzinfo=VN_TZ),
    },
    tags=['daily', 'bronze'],
) as dag:

    interest_rate = BashOperator(
        task_id='ingest_interest_rate_to_bronze',
        bash_command='docker exec python_container python bronze/ingest_interest_rate_day.py',
    )

    yfinance = BashOperator(
        task_id='ingest_yfinance_to_bronze',
        bash_command='docker exec python_container python bronze/ingest_yfinance_day.py',
    )

    validate = PythonOperator(
        task_id='validate_daily_bronze',
        python_callable=validate_daily_bronze,
    )

    [interest_rate,  yfinance] >> validate
