from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


VN_TZ = pendulum.timezone("Asia/Ho_Chi_Minh")

DBT_PROJECT_DIR = "/opt/airflow/dbt"

DBT_MONTHLY_MODELS = (
     "stg_crop "
     "stg_gdp "
    "stg_macro_indicator "
     "stg_production_output "
     "stg_social_total_investment "
     "stg_trade_international "
     "fact_crop_yield "
     "fact_gdp_growth "
     "fact_international_trade "
    "fact_macro_indicator "
    "fact_production_output "
    "fact_social_total_investment"
)


with DAG(
    dag_id="gold_monthly_pipeline",
    start_date=datetime(2025, 1, 1, tzinfo=VN_TZ),
    schedule="0 10 10 * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineer",
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["gold", "monthly", "dbt", "macro"],
) as dag:

    dbt_run_gold_monthly = BashOperator(
        task_id="dbt_run_gold_monthly",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run "
            f"--select {DBT_MONTHLY_MODELS} "
            f"--profiles-dir ."
        ),
    )

    dbt_test_gold_monthly = BashOperator(
        task_id="dbt_test_gold_monthly",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test "
            f"--select {DBT_MONTHLY_MODELS} "
            f"--profiles-dir ."
        ),
    )

    dbt_run_gold_monthly >> dbt_test_gold_monthly