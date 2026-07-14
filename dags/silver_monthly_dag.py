from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

with DAG(
    dag_id="silver_monthly_macro_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 8 10 * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["silver", "monthly", "macro", "transform"],
) as dag:

    economic = BashOperator(
        task_id="silver_economics",
        bash_command=(
            "docker exec python_container "
            "python silver/bronze_all_economics_silver.py"
        ),
    )

    # excel_reports = BashOperator(
    #     task_id="silver_excel_reports",
    #     bash_command=(
    #         "docker exec spark-master "
    #         "/opt/spark/bin/spark-submit /opt/spark/apps/silver/main.py"
    #     ),
    # )

    economic