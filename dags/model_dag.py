from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

with DAG(
    dag_id='model_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='0 9 * * 1',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=10)},
    tags=['model', 'ml', 'cpi', 'forecast'],
) as dag:

    wait_gold = ExternalTaskSensor(
        task_id='wait_gold_pipeline',
        external_dag_id='gold_pipeline',
        external_task_id=None,
        mode='reschedule',
        timeout=7200,
        poke_interval=300,
    )

    train = BashOperator(
        task_id='train_cpi_model',
        bash_command='docker exec python_container python model/train_cpi.py',
    )

    predict = BashOperator(
        task_id='predict_cpi_future',
        bash_command='docker exec python_container python model/predict_cpi.py',
    )

    push_forecast_gold = BashOperator(
        task_id='push_forecast_to_gold',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit gold/build_fact_market.py --forecast-only',
    )

    push_deeplake = BashOperator(
        task_id='push_dataset_to_deeplake',
        bash_command='curl -X POST http://model-api:8000/dataset/push',
    )

    wait_gold >> train >> predict >> push_forecast_gold >> push_deeplake
