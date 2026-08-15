from datetime import datetime, timedelta
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
with DAG(
    dag_id="start_realtime_streaming",
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 6 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["realtime", "kafka", "streaming", "start"],
) as dag:

    start_runner = BashOperator(
        task_id="start_runner_streaming_container",
        bash_command="docker start runner || docker compose up -d pipeline",
    )