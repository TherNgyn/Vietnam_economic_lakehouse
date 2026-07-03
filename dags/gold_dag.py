from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id='gold_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='0 8 * * *',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=10)},
    tags=['gold', 'transform', 'dbt'],
) as dag:

    # wait_silver = ExternalTaskSensor(
    #     task_id='wait_silver_pipeline',
    #     external_dag_id='silver_pipeline',
    #     external_task_id=None,
    #     mode='reschedule',
    #     timeout=3600,
    #     poke_interval=120,
    # )

    dbt_run = BashOperator(
        task_id='dbt_run_gold',
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir . --target spark',
    )

    dbt_test = BashOperator(
        task_id='dbt_test_gold',
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir . --target spark',
    )

    # redis_push = BashOperator(
    #     task_id='gold_push_to_redis',
    #     bash_command='docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/apps/gold/push_to_redis.py',
    # )

    dbt_run >> dbt_test 

