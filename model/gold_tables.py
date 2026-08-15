from config import (
    MODEL_GOLD_DATABASE,
    GOLD_MODEL_REGISTRY_TABLE,
    GOLD_MODEL_METRICS_TABLE,
    GOLD_PREDICTION_TABLE,
    GOLD_FEATURE_IMPORTANCE_TABLE,
)
from spark_session import get_spark


def create_gold_tables():
    spark = get_spark()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {MODEL_GOLD_DATABASE}")

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_MODEL_REGISTRY_TABLE} (
        model_id STRING,
        model_name STRING,
        model_version STRING,
        mlflow_run_id STRING,
        mlflow_model_uri STRING,
        artifact_path STRING,
        feature_table STRING,
        feature_snapshot_path STRING,
        target_col STRING,
        is_active BOOLEAN,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION 's3a://gold/model/results/ml_model_registry'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_MODEL_METRICS_TABLE} (
        model_id STRING,
        model_name STRING,
        model_version STRING,
        dataset_split STRING,
        rmse DOUBLE,
        mae DOUBLE,
        r2 DOUBLE,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION 's3a://gold/model/results/ml_model_metrics'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_PREDICTION_TABLE} (
        prediction_for_date DATE,
        model_id STRING,
        model_name STRING,
        model_version STRING,
        target_col STRING,
        predicted_value DOUBLE,
        actual_value DOUBLE,
        prediction_type STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION 's3a://gold/model/results/inflation_forecast_predictions'
    """)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {GOLD_FEATURE_IMPORTANCE_TABLE} (
        model_id STRING,
        model_name STRING,
        model_version STRING,
        feature_name STRING,
        importance_value DOUBLE,
        importance_type STRING,
        direction STRING,
        created_at TIMESTAMP
    )
    USING DELTA
    LOCATION 's3a://gold/model/results/inflation_feature_importance'
    """)


if __name__ == "__main__":
    create_gold_tables()