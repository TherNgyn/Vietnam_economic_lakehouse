from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='silver_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule='0 7 * * *',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    tags=['silver', 'transform'],
) as dag:

    economic = BashOperator(
        task_id='silver_economics',
        bash_command='docker exec python_container python silver/bronze_all_economics_silver.py',
    )
    interest_rate = BashOperator(
        task_id='silver_interest_rate',
        bash_command='docker exec python_container python silver/bronze_interest_rate_silver.py',
    )

    gasoline = BashOperator(
        task_id='silver_gasoline',
        bash_command='docker exec python_container python silver/bronze_gasoline_silver.py',
    )

    ohlc_world = BashOperator(
        task_id='silver_ohlc_world',
        bash_command='docker exec python_container python silver/bronze_to_ohlc.py',
    )

    ohlc_vn = BashOperator(
        task_id='silver_ohlc_vietnam',
        bash_command='docker exec python_container python silver/bronze_to_vietnam_ohlc.py',
    )

    excel_reports = BashOperator(
        task_id='silver_excel_reports',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit silver/main.py',
    )

    ddl_silver = BashOperator(
        task_id='silver_ddl',
        bash_command='docker exec spark-master /opt/spark/bin/spark-submit silver/ddl_silver.py',
    )

    ddl_silver >> [economic, interest_rate, gasoline, ohlc_world, ohlc_vn, excel_reports]
