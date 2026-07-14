from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="silver_daily_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 18 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["silver", "daily", "transform"],
) as dag:

    interest_rate = BashOperator(
        task_id="silver_interest_rate",
        bash_command=(
            "docker exec python_container "
            "python silver/bronze_interest_rate_silver.py"
        ),
    )

    ohlc_world = BashOperator(
        task_id="silver_ohlc_world",
        bash_command=(
            "docker exec python_container "
            "python silver/bronze_to_ohlc.py"
        ),
    )

    ohlc_vn = BashOperator(
        task_id="silver_ohlc_vietnam",
        bash_command=(
            "docker exec python_container "
            "python silver/bronze_to_vietnam_ohlc.py"
        ),
    )

    [interest_rate, ohlc_world, ohlc_vn]