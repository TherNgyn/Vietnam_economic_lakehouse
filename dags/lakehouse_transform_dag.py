import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


def check_bronze_gap(**context):
    from minio import Minio
    from datetime import datetime as dt

    client = Minio(
        os.getenv('MINIO_HOST', 'minio:9000'),
        access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
        secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
        secure=False,
    )
    today = dt.utcnow().date()
    max_gap = 0

    for prefix in ('daily/currency', 'daily/world_index', 'daily/product'):
        try:
            objects = list(client.list_objects('bronze', prefix=prefix, recursive=False))
            dates = []
            for obj in objects:
                name = obj.object_name.rstrip('/')
                if 'processing_date=' in name:
                    try:
                        dates.append(dt.strptime(name.split('processing_date=')[-1], '%Y-%m-%d').date())
                    except Exception:
                        pass
            if dates:
                max_gap = max(max_gap, (today - max(dates)).days)
            else:
                max_gap = max(max_gap, 999)
        except Exception:
            max_gap = max(max_gap, 999)

    if max_gap <= 1:
        period = 'skip'
    elif max_gap <= 7:
        period = '5d'
    elif max_gap <= 30:
        period = '30d'
    else:
        period = 'max'

    context['task_instance'].xcom_push(key='backfill_period', value=period)
    return period


def decide_backfill(**context):
    period = context['task_instance'].xcom_pull(task_ids='check_bronze_gap', key='backfill_period')
    if not period or period == 'skip':
        return 'bronze_quality_ok'
    return 'backfill_yfinance'


with DAG(
    dag_id='lakehouse_transform',
    start_date=datetime(2025, 1, 1),
    schedule='0 6 * * *',
    catchup=False,
    default_args={
        'owner': 'data-engineer',
        'retries': 1,
        'retry_delay': timedelta(minutes=10),
    },
    tags=['lakehouse', 'orchestrator'],
) as dag:

    check_gap = PythonOperator(
        task_id='check_bronze_gap',
        python_callable=check_bronze_gap,
    )

    branch = BranchPythonOperator(
        task_id='decide_backfill',
        python_callable=decide_backfill,
    )

    backfill = BashOperator(
        task_id='backfill_yfinance',
        bash_command=(
            "docker exec python_container python bronze/ingest_yfinance_day.py "
            "--period {{ ti.xcom_pull(task_ids='check_bronze_gap', key='backfill_period') }}"
        ),
    )

    quality_ok = BashOperator(
        task_id='bronze_quality_ok',
        bash_command='echo bronze OK',
        trigger_rule='none_failed_min_one_success',
    )

    check_gap >> branch >> [backfill, quality_ok]
    backfill >> quality_ok
