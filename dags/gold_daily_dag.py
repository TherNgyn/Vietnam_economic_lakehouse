from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_TARGET = "spark"

DBT_DAILY_MODELS = (
        "dim_time "
        "dim_asset_class "
        "dim_market "
        "dim_asset "
        "dim_term "
        "stg_interest_rate "
        "stg_ohlc "
        "fact_interest_rate "
        "fact_ohlc"
)


with DAG(
    dag_id="gold_daily_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 19 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["gold", "daily", "dbt", "interest_rate", "ohlc"],
) as dag:

    dbt_run_gold_daily = BashOperator(
        task_id="dbt_run_gold_daily",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"--select {DBT_DAILY_MODELS} "
            f"--profiles-dir . "
        ),
    )

    dbt_test_gold_daily = BashOperator(
        task_id="dbt_test_gold_daily",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--select {DBT_DAILY_MODELS} "
            f"--profiles-dir . "
        ),
    )

    dbt_run_gold_daily >> dbt_test_gold_daily