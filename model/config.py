import os

APP_NAME = os.getenv("APP_NAME", "Inflation-MLflow-VARNN-RM")

SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://hive:9083")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL", MINIO_ENDPOINT)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "inflation_forecast")
MLFLOW_REGISTERED_MODEL_NAME = os.getenv(
    "MLFLOW_REGISTERED_MODEL_NAME",
    "inflation_varnn_rm"
)
MLFLOW_MODEL_ALIAS = os.getenv("MLFLOW_MODEL_ALIAS", "champion")

DBT_GOLD_DATABASE = os.getenv("DBT_GOLD_DATABASE", "gold_gold")
MODEL_GOLD_DATABASE = os.getenv("MODEL_GOLD_DATABASE", "gold_model")

MODEL_BUCKET = os.getenv("MODEL_BUCKET", "gold")
MODEL_PREFIX = os.getenv("MODEL_PREFIX", "model_artifacts/inflation/varnn_rm")

DEEPLAKE_URI_PREFIX = os.getenv(
    "DEEPLAKE_URI_PREFIX",
    "s3://gold/deeplake/inflation/features"
)

DATE_COL = "date"
TARGET_COL = "cpi_mom_processed_inflation"

TRAIN_RATIO = 0.70
VALID_RATIO = 0.15
SEED = 42

START_MONTH = os.getenv("START_MONTH", "1995-01-01")
END_MONTH = os.getenv("END_MONTH", "2024-12-01")

CANDIDATE_KEEP_VARS = [
    TARGET_COL,
    "wti",
    "gasoline_world",
    "gold",
    "USDVND",
    "policy_rate",
    "interest_rate",
    "broad_money",
    "NIKKEI225",
    "gdp",
]

GOLD_MONTHLY_FEATURE_TABLE = (
    f"{MODEL_GOLD_DATABASE}.inflation_forecast_features_monthly"
)
GOLD_MODEL_FEATURE_TABLE = (
    f"{MODEL_GOLD_DATABASE}.inflation_model_features"
)
GOLD_MODEL_REGISTRY_TABLE = (
    f"{MODEL_GOLD_DATABASE}.ml_model_registry"
)
GOLD_MODEL_METRICS_TABLE = (
    f"{MODEL_GOLD_DATABASE}.ml_model_metrics"
)
GOLD_PREDICTION_TABLE = (
    f"{MODEL_GOLD_DATABASE}.inflation_forecast_predictions"
)
GOLD_FEATURE_IMPORTANCE_TABLE = (
    f"{MODEL_GOLD_DATABASE}.inflation_feature_importance"
)

GOLD_FACT_GDP_GROWTH_TABLE = f"{DBT_GOLD_DATABASE}.fact_gdp_growth"
GOLD_DIM_TIME_TABLE = f"{DBT_GOLD_DATABASE}.dim_time"