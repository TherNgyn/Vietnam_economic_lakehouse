from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


def task(task_id: str, pipeline_task: str, extra_args: str = ""):
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "docker exec model-runner "
            "python /app/model/run_pipeline.py "
            f"--task {pipeline_task} {extra_args}"
        ),
    )


with DAG(
    dag_id="inflation_forecast_mlflow_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["inflation", "mlflow", "varnn_rm"],
) as dag:

    create_gold_tables = task(
        "create_gold_tables",
        "create_gold_tables",
    )

    # build_gold_features = task(
    #     "build_gold_features",
    #     "build_gold_features",
    # )

    # build_model_features = task(
    #     "build_model_features",
    #     "build_model_features",
    # )

    # save_deeplake_snapshot = task(
    #     "save_deeplake_snapshot",
    #     "save_deeplake_snapshot",
    #     "--version {{ ts_nodash }}",
    # )

    # train_varnn_rm_and_log_mlflow = task(
    #     "train_varnn_rm_and_log_mlflow",
    #     "train_varnn_rm_and_log_mlflow",
    # )

    # promote_best_model_or_mark_candidate = task(
    #     "promote_best_model_or_mark_candidate",
    #     "promote_model",
    # )

    # run_inference = task(
    #     "run_inference",
    #     "run_inference",
    # )

    # write_explainability_to_gold = task(
    #     "write_explainability_to_gold",
    #     "write_explainability",
    # )

    (
        create_gold_tables
        # >> build_gold_features
        # >> build_model_features
        # >> save_deeplake_snapshot
        # >> train_varnn_rm_and_log_mlflow
        # >> promote_best_model_or_mark_candidate
        # >> run_inference
        # >> write_explainability_to_gold
    )