from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
with DAG(
    dag_id='gold_pipeline',
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 19 * * 1-5",
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=10)},
    tags=['gold', 'transform', 'dbt'],
) as dag:
    dbt_run = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir . --target spark',
    )

    dbt_test = BashOperator(
        task_id='dbt_test_gold',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir . --target spark',
    )
    dbt_run >> dbt_test 

