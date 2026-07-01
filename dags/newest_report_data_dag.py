import requests
import pytz
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.date_time import DateTimeSensor

VN_TZ = timezone(timedelta(hours=7))


def get_time_of_next_report():
    url = 'https://www.nso.gov.vn/bao-cao-tinh-hinh-kinh-te-xa-hoi-hang-thang/'
    try:
        res = requests.get(url, verify=False, timeout=15)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        container = soup.find('div', class_='archive-container')
        span = container.find('span', class_='archive-next-release')
        date_str = span.get_text(strip=True).split(':', 1)[1].strip()
        next_date = datetime.strptime(date_str, '%d/%m/%Y').replace(tzinfo=VN_TZ)
        return (next_date + timedelta(days=1)).astimezone(pytz.UTC).isoformat()
    except Exception:
        return None


with DAG(
    dag_id='newest_report_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=['report', 'bronze', 'silver'],
) as dag:

    wait_for_report = DateTimeSensor(
        task_id='wait_for_report_time',
        target_time="{{ dag_run.conf['target_time'] }}",
        mode='reschedule',
        poke_interval=600,
    )

    crawl_newest = BashOperator(
        task_id='crawl_newest_report_to_bronze',
        bash_command='docker exec python_container python bronze/crawl_and_load_newest_report.py',
    )

    transform_silver = BashOperator(
        task_id='transform_newest_to_silver',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit silver/main_2.py',
    )

    get_next_time = PythonOperator(
        task_id='get_next_report_time',
        python_callable=get_time_of_next_report,
    )

    trigger_self = TriggerDagRunOperator(
        task_id='trigger_next_cycle',
        trigger_dag_id='newest_report_pipeline',
        conf={'target_time': "{{ ti.xcom_pull(task_ids='get_next_report_time') }}"},
    )

wait_for_report >> crawl_newest >> transform_silver >> get_next_time >> trigger_self
