from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pendulum
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
MACRO_OBJECTS = [
    "historical/economics/broad_money_policy_rate.csv",
    "historical/economics/cpi.csv",
    "historical/economics/money_supply_m2.csv",
    "historical/economics/vietnam-core-inflation-rate.csv",
    "historical/economics/vietnam-interest-rate.csv",
    "historical/economics/vietnam-producer-price-inflation-qoq.csv",
]


def validate_macro_bronze(**context):
    import os
    from minio import Minio

    endpoint = os.getenv("MINIO_HOST", "minio:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")

    client = Minio(
        endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")),
        secret_key=os.getenv("MINIO_SECRET_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")),
        secure=False,
    )

    status = {}

    for object_name in MACRO_OBJECTS:
        try:
            stat = client.stat_object("bronze", object_name)
            status[object_name] = {
                "status": "OK",
                "size": stat.size,
                "last_modified": stat.last_modified.isoformat() if stat.last_modified else None,
            }
        except Exception as exc:
            status[object_name] = {
                "status": f"MISSING_OR_ERROR: {str(exc)[:120]}"
            }

    context["task_instance"].xcom_push(
        key="macro_bronze_status",
        value=status,
    )

    return status


with DAG(
    dag_id="macro_local_monthly_sync",
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 8 1 * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["macro", "bronze", "monthly", "local_sync"],
) as dag:

    sync_historical_local_to_bronze = BashOperator(
        task_id="sync_historical_local_to_bronze",
        bash_command=(
            "docker exec python_container "
            "python bronze/ingest_historical.py"
        ),
    )

    validate = PythonOperator(
        task_id="validate_macro_bronze",
        python_callable=validate_macro_bronze,
    )

    sync_historical_local_to_bronze >> validate