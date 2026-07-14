from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="silver_ddl_init",    
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["silver","ddl", "init"],
) as dag:

    ddl_silver = BashOperator(
        task_id="silver_ddl",
        bash_command=(
            "docker exec spark-master "
            "/opt/spark/bin/spark-submit /opt/spark/apps/silver/ddl_silver.py"
        ),
    )
