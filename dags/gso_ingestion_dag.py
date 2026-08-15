import os
import re
import subprocess
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup
from minio import Minio

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.date_time import DateTimeSensor
from airflow.utils.trigger_rule import TriggerRule


VN_TZ = timezone(timedelta(hours=7))
NSO_URL = "https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/"

BRONZE_BUCKET = "bronze"
REPORT_PREFIX = "economic_report_excel_files"

HISTORICAL_CRAWL_CMD = (
    "docker exec python_container "
    "python bronze/crawl_and_load_report_excel_files_to_bronze.py"
)

NEWEST_CRAWL_CMD = (
    "docker exec python_container "
    "python bronze/crawl_and_load_newest_report.py"
)


def fetch_archive_container():
    try:
        res = requests.get(NSO_URL, verify=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        container = soup.find("div", class_="archive-container")
        if container is None:
            raise AirflowFailException("Không tìm thấy archive-container.")
        return container
    except Exception as exc:
        raise AirflowFailException(f"Lỗi request hoặc parse trang NSO: {exc}") from exc


def get_next_report_time(add_days=1):
    container = fetch_archive_container()
    span = container.find("span", class_="archive-next-release")

    if span is None:
        raise AirflowFailException("Không tìm thấy archive-next-release.")

    raw_text = span.get_text(strip=True)
    match = re.search(r"(\d{2}/\d{2}/\d{4})", raw_text)

    if not match:
        raise AirflowFailException(f"Không parse được ngày công bố từ: {raw_text}")

    release_date = datetime.strptime(match.group(1), "%d/%m/%Y").replace(tzinfo=VN_TZ)
    target_time = release_date + timedelta(days=add_days)

    return target_time.astimezone(timezone.utc).isoformat()


def get_latest_reference_period():
    container = fetch_archive_container()
    text = container.get_text(" ", strip=True)

    match = re.search(r"Kỳ tham chiếu:\s*(\d{1,2})/(\d{4})", text)

    if not match:
        raise AirflowFailException(f"Không parse được kỳ tham chiếu từ nội dung: {text}")

    month = int(match.group(1))
    year = int(match.group(2))

    return year, month


def get_minio_client():
    endpoint = os.getenv("MINIO_HOST", "minio:9000")
    endpoint = endpoint.replace("http://", "").replace("https://", "").rstrip("/")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure = os.getenv("MINIO_SECURE", "false").lower() == "true"

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def bronze_report_exists(year, month):
    client = get_minio_client()
    prefix = f"{REPORT_PREFIX}/{year}/{month}/"

    try:
        for _ in client.list_objects(BRONZE_BUCKET, prefix=prefix, recursive=True):
            return True
        return False
    except Exception as exc:
        raise AirflowFailException(
            f"Lỗi kiểm tra MinIO prefix s3://{BRONZE_BUCKET}/{prefix}: {exc}"
        ) from exc


def run_cmd(cmd):
    subprocess.run(cmd, shell=True, check=True)


def choose_route(**context):
    conf = context["dag_run"].conf or {}
    return "bootstrap_process" if not conf.get("target_time") else "wait_for_report_time"


def bootstrap_process():
    run_cmd(HISTORICAL_CRAWL_CMD)
    return get_next_report_time(add_days=1)


def process_cycle(**context):
    conf = context["dag_run"].conf or {}
    target_time_str = conf.get("target_time")

    if not target_time_str:
        raise AirflowFailException("Thiếu target_time trong dag_run.conf")

    target_time = datetime.fromisoformat(target_time_str)
    web_next_time = datetime.fromisoformat(get_next_report_time(add_days=1))
    if web_next_time > target_time:
        year, month = get_latest_reference_period()

        if not bronze_report_exists(year, month):
            run_cmd(NEWEST_CRAWL_CMD)

        return get_next_report_time(add_days=1)

    return (target_time + timedelta(days=1)).isoformat()


def resolve_next_target_time(**context):
    ti = context["ti"]
    target_time = (
        ti.xcom_pull(task_ids="bootstrap_process")
        or ti.xcom_pull(task_ids="process_cycle")
    )

    if not target_time:
        raise AirflowFailException("Không xác định được target_time cho chu kỳ tiếp theo.")

    target_dt = datetime.fromisoformat(target_time)

    print(f"NEXT_TARGET_TIME_UTC={target_dt.isoformat()}")
    print(f"NEXT_TARGET_TIME_VN={target_dt.astimezone(VN_TZ).isoformat()}")

    return target_time


with DAG(
    dag_id="gso_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["gso", "bronze", "economic_report"],
) as dag:

    branch = BranchPythonOperator(
        task_id="choose_route",
        python_callable=choose_route,
    )

    bootstrap = PythonOperator(
        task_id="bootstrap_process",
        python_callable=bootstrap_process,
    )

    wait = DateTimeSensor(
        task_id="wait_for_report_time",
        target_time="{{ dag_run.conf.get('target_time') }}",
        mode="reschedule",
        poke_interval=86400,
        timeout=60 * 60 * 24 * 60,
    )

    process = PythonOperator(
        task_id="process_cycle",
        python_callable=process_cycle,
    )

    resolve = PythonOperator(
        task_id="resolve_next_target_time",
        python_callable=resolve_next_target_time,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    trigger = TriggerDagRunOperator(
        task_id="trigger_next_cycle",
        trigger_dag_id="gso_ingestion",
        trigger_run_id=(
            "gso_ingestion__"
            "{{ ti.xcom_pull(task_ids='resolve_next_target_time') "
            "| replace(':', '') "
            "| replace('-', '') "
            "| replace('+', '') "
            "| replace('.', '') }}"
        ),
        conf={
            "target_time": "{{ ti.xcom_pull(task_ids='resolve_next_target_time') }}"
        },
        reset_dag_run=False,
        skip_when_already_exists=True,
    )

    branch >> bootstrap >> resolve
    branch >> wait >> process >> resolve
    resolve >> trigger