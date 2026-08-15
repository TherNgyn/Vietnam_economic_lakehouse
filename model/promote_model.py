import mlflow
from mlflow.tracking import MlflowClient

from spark_session import get_spark
from config import (
    MLFLOW_TRACKING_URI,
    MLFLOW_REGISTERED_MODEL_NAME,
    MLFLOW_MODEL_ALIAS,
    GOLD_MODEL_REGISTRY_TABLE,
)


PROMOTE_MODEL_VERSION = "4"


def promote_latest_model():
    spark = get_spark()

    try:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
        client = MlflowClient()

        selected_version = client.get_model_version(
            name=MLFLOW_REGISTERED_MODEL_NAME,
            version=PROMOTE_MODEL_VERSION,
        )

        run_id = selected_version.run_id

        client.set_registered_model_alias(
            MLFLOW_REGISTERED_MODEL_NAME,
            MLFLOW_MODEL_ALIAS,
            PROMOTE_MODEL_VERSION,
        )

        spark.sql(f"""
        UPDATE {GOLD_MODEL_REGISTRY_TABLE}
        SET is_active = false
        WHERE model_name = 'varnn_rm'
        """)

        spark.sql(f"""
        UPDATE {GOLD_MODEL_REGISTRY_TABLE}
        SET is_active = true
        WHERE mlflow_run_id = '{run_id}'
        """)

        print(
            f"promoted {MLFLOW_REGISTERED_MODEL_NAME}@{MLFLOW_MODEL_ALIAS} "
            f"version={PROMOTE_MODEL_VERSION}, run_id={run_id}"
        )

    finally:
        try:
            spark.catalog.clearCache()
        except Exception:
            pass

        try:
            spark.stop()
        except Exception:
            pass


if __name__ == "__main__":
    promote_latest_model()