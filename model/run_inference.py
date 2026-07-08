import os
from datetime import datetime
import pandas as pd
import mlflow.pyfunc
from pyspark.sql import functions as F
from spark_session import get_spark
from config import MLFLOW_TRACKING_URI, MLFLOW_REGISTERED_MODEL_NAME, MLFLOW_MODEL_ALIAS, TARGET_COL, GOLD_MODEL_FEATURE_TABLE, GOLD_MODEL_REGISTRY_TABLE, GOLD_PREDICTION_TABLE


def run_inference():
    spark = get_spark()
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv("AWS_ENDPOINT_URL", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    mlflow.pyfunc.mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

    active = spark.table(GOLD_MODEL_REGISTRY_TABLE).where("model_name = 'varnn_rm' and is_active = true").orderBy(F.col("created_at").desc()).limit(1).toPandas()
    if active.empty:
        raise RuntimeError("No active varnn_rm model in gold.ml_model_registry")
    row = active.iloc[0]
    model_uri = f"models:/{MLFLOW_REGISTERED_MODEL_NAME}@{MLFLOW_MODEL_ALIAS}"
    model = mlflow.pyfunc.load_model(model_uri)

    features = spark.table(GOLD_MODEL_FEATURE_TABLE).toPandas()
    features["date"] = pd.to_datetime(features["date"], errors="coerce")
    latest = features.sort_values("date").tail(1).copy()
    pred = float(model.predict(latest)[0])
    prediction_for_date = (latest["date"].iloc[0] + pd.offsets.MonthBegin(1)).date()
    now = datetime.utcnow()
    out = [(prediction_for_date, row["model_id"], "varnn_rm", row["model_version"], TARGET_COL, pred, None, "future", now)]
    spark.createDataFrame(out, "prediction_for_date date, model_id string, model_name string, model_version string, target_col string, predicted_value double, actual_value double, prediction_type string, created_at timestamp").write.format("delta").mode("append").saveAsTable(GOLD_PREDICTION_TABLE)
    print(f"saved future prediction {prediction_for_date}: {pred}")


if __name__ == "__main__":
    run_inference()
