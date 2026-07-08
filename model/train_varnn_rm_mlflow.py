import gc
import json
import os
import tempfile
from datetime import datetime
from math import sqrt
from pathlib import Path

import joblib
import mlflow
import mlflow.pyfunc
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import RobustScaler

from config import (
    SEED,
    TRAIN_RATIO,
    VALID_RATIO,
    TARGET_COL,
    GOLD_MODEL_FEATURE_TABLE,
    MLFLOW_TRACKING_URI,
    MLFLOW_EXPERIMENT_NAME,
    MLFLOW_REGISTERED_MODEL_NAME,
    MODEL_BUCKET,
    MODEL_PREFIX,
    DEEPLAKE_URI_PREFIX,
    GOLD_MODEL_REGISTRY_TABLE,
    GOLD_MODEL_METRICS_TABLE,
    GOLD_PREDICTION_TABLE,
)
from spark_session import get_spark
from minio_io import upload_dir
from varnn_pyfunc import VarNNRMPyFunc


def regression_metrics(y_true, y_pred):
    return {
        "rmse": float(sqrt(mean_squared_error(y_true, y_pred))),
        "mae": float(mean_absolute_error(y_true, y_pred)),
        "r2": float(r2_score(y_true, y_pred)),
    }


def train_varnn_rm():
    spark = get_spark()

    try:
        model_df = spark.table(GOLD_MODEL_FEATURE_TABLE).toPandas()
        model_df["date"] = pd.to_datetime(model_df["date"], errors="coerce")
        model_df = model_df.sort_values("date").dropna(subset=["target_next"]).copy()

        n = len(model_df)
        train_end = int(n * TRAIN_RATIO)
        valid_end = int(n * (TRAIN_RATIO + VALID_RATIO))

        train_df = model_df.iloc[:train_end].copy()
        valid_df = model_df.iloc[train_end:valid_end].copy()
        test_df = model_df.iloc[valid_end:].copy()

        if len(train_df) == 0 or len(valid_df) == 0 or len(test_df) == 0:
            raise ValueError(
                f"Invalid split sizes: "
                f"train={len(train_df)}, valid={len(valid_df)}, test={len(test_df)}"
            )

        X_train = train_df.drop(columns=["target_next", "date"])
        y_train = train_df["target_next"]

        X_valid = valid_df.drop(columns=["target_next", "date"])
        y_valid = valid_df["target_next"]

        X_test = test_df.drop(columns=["target_next", "date"])
        y_test = test_df["target_next"]

        print("Number of training features:", len(X_train.columns))
        print("Training features:")
        for i, col in enumerate(X_train.columns, 1):
            print(i, col)

        imputer = SimpleImputer(strategy="median")

        X_train_imp = pd.DataFrame(
            imputer.fit_transform(X_train),
            columns=X_train.columns,
            index=X_train.index,
        )
        X_valid_imp = pd.DataFrame(
            imputer.transform(X_valid),
            columns=X_valid.columns,
            index=X_valid.index,
        )
        X_test_imp = pd.DataFrame(
            imputer.transform(X_test),
            columns=X_test.columns,
            index=X_test.index,
        )

        x_scaler = RobustScaler()
        y_scaler = RobustScaler()

        X_train_scaled = x_scaler.fit_transform(X_train_imp)
        X_valid_scaled = x_scaler.transform(X_valid_imp)
        X_test_scaled = x_scaler.transform(X_test_imp)

        y_train_scaled = y_scaler.fit_transform(
            y_train.values.reshape(-1, 1)
        ).ravel()
        y_valid_scaled = y_scaler.transform(
            y_valid.values.reshape(-1, 1)
        ).ravel()
        y_test_scaled = y_scaler.transform(
            y_test.values.reshape(-1, 1)
        ).ravel()

        np.random.seed(SEED)

        
        base_predictor = MLPRegressor(
            hidden_layer_sizes=(8, 4),
            activation="relu",
            solver="adam",
            alpha=1e-2,
            max_iter=1500,
            random_state=SEED,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=40,
        )


        base_predictor.fit(X_train_scaled, y_train_scaled)

        train_base_preds = base_predictor.predict(X_train_scaled)
        train_residuals = y_train_scaled - train_base_preds

        train_res_lagged = np.concatenate(
            ([0], train_residuals[:-1])
        )[:, np.newaxis]

        X_train_varnn = np.hstack([X_train_scaled, train_res_lagged])

        
        varnn_rm = MLPRegressor(
            hidden_layer_sizes=(8, 4),
            activation="tanh",
            solver="adam",
            alpha=1e-2,
            learning_rate_init=5e-4,
            max_iter=1000,
            random_state=SEED,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=30,
        )


        varnn_rm.fit(X_train_varnn, y_train_scaled)

        def recursive_predict(X_scaled, y_true_scaled, initial_residual):
            predictions = []
            current_residual = initial_residual

            for i in range(len(X_scaled)):
                input_vector = np.hstack(
                    [X_scaled[i], current_residual]
                ).reshape(1, -1)

                pred_scaled = varnn_rm.predict(input_vector)[0]
                predictions.append(pred_scaled)

                current_residual = y_true_scaled[i] - pred_scaled

            return np.array(predictions)

        last_train_residual = float(train_residuals[-1])

        valid_pred_scaled = recursive_predict(
            X_valid_scaled,
            y_valid_scaled,
            last_train_residual,
        )

        last_valid_residual = float(y_valid_scaled[-1] - valid_pred_scaled[-1])

        test_pred_scaled = recursive_predict(
            X_test_scaled,
            y_test_scaled,
            last_valid_residual,
        )

        valid_pred = y_scaler.inverse_transform(
            valid_pred_scaled.reshape(-1, 1)
        ).ravel()

        test_pred = y_scaler.inverse_transform(
            test_pred_scaled.reshape(-1, 1)
        ).ravel()

        valid_metrics = regression_metrics(y_valid, valid_pred)
        test_metrics = regression_metrics(y_test, test_pred)

        version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        model_id = f"varnn_rm_{version}"

        feature_snapshot_path = f"{DEEPLAKE_URI_PREFIX}/{version}"
        artifact_prefix = f"{MODEL_PREFIX}/{version}"
        artifact_path = f"s3://{MODEL_BUCKET}/{artifact_prefix}"

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)

            joblib.dump(varnn_rm, td / "model.pkl")
            joblib.dump(base_predictor, td / "base_predictor.pkl")
            joblib.dump(imputer, td / "imputer.pkl")
            joblib.dump(x_scaler, td / "x_scaler.pkl")
            joblib.dump(y_scaler, td / "y_scaler.pkl")

            with open(td / "feature_columns.json", "w", encoding="utf-8") as f:
                json.dump(list(X_train.columns), f, ensure_ascii=False, indent=2)

            metadata = {
                "model_id": model_id,
                "model_name": "varnn_rm",
                "model_version": version,
                "target_col": TARGET_COL,
                "feature_table": GOLD_MODEL_FEATURE_TABLE,
                "feature_snapshot_path": feature_snapshot_path,
                "last_train_residual": last_train_residual,
                "last_valid_residual": last_valid_residual,
            }

            with open(td / "metadata.json", "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            with open(td / "metrics.json", "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "validation": valid_metrics,
                        "test": test_metrics,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            upload_dir(MODEL_BUCKET, artifact_prefix, str(td))

            os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv(
                "AWS_ENDPOINT_URL",
                os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            )
            os.environ["AWS_ACCESS_KEY_ID"] = os.getenv(
                "MINIO_ACCESS_KEY",
                "minioadmin",
            )
            os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv(
                "MINIO_SECRET_KEY",
                "minioadmin",
            )

            mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
            mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

            with mlflow.start_run(run_name=model_id) as run:
                mlflow.log_params(
                    {
                        "model_name": "varnn_rm",
                        "target_col": TARGET_COL,
                        "feature_table": GOLD_MODEL_FEATURE_TABLE,
                        "feature_snapshot_path": feature_snapshot_path,
                        "base_hidden_layer_sizes": "(16,8)",
                        "varnn_hidden_layer_sizes": "(16,8)",
                        "alpha": 1e-2,
                        "learning_rate_init": 5e-4,
                    }
                )

                mlflow.log_metrics(
                    {
                        "valid_rmse": valid_metrics["rmse"],
                        "valid_mae": valid_metrics["mae"],
                        "valid_r2": valid_metrics["r2"],
                        "test_rmse": test_metrics["rmse"],
                        "test_mae": test_metrics["mae"],
                        "test_r2": test_metrics["r2"],
                    }
                )

                artifacts = {
                    "model": str(td / "model.pkl"),
                    "imputer": str(td / "imputer.pkl"),
                    "x_scaler": str(td / "x_scaler.pkl"),
                    "y_scaler": str(td / "y_scaler.pkl"),
                    "feature_columns": str(td / "feature_columns.json"),
                    "metadata": str(td / "metadata.json"),
                }

                mlflow.pyfunc.log_model(
                    artifact_path="model",
                    python_model=VarNNRMPyFunc(),
                    artifacts=artifacts,
                    registered_model_name=MLFLOW_REGISTERED_MODEL_NAME,
                )

                run_id = run.info.run_id
                mlflow_model_uri = f"runs:/{run_id}/model"

        now = datetime.utcnow()

        registry = [
            (
                model_id,
                "varnn_rm",
                version,
                run_id,
                mlflow_model_uri,
                artifact_path,
                GOLD_MODEL_FEATURE_TABLE,
                feature_snapshot_path,
                TARGET_COL,
                False,
                now,
            )
        ]

        metrics_rows = [
            (
                model_id,
                "varnn_rm",
                version,
                "validation",
                valid_metrics["rmse"],
                valid_metrics["mae"],
                valid_metrics["r2"],
                now,
            ),
            (
                model_id,
                "varnn_rm",
                version,
                "test",
                test_metrics["rmse"],
                test_metrics["mae"],
                test_metrics["r2"],
                now,
            ),
        ]

        pred_rows = []

        for _, r in pd.DataFrame(
            {
                "date": valid_df["date"],
                "actual": y_valid,
                "pred": valid_pred,
            }
        ).iterrows():
            pred_rows.append(
                (
                    pd.to_datetime(r["date"]).date(),
                    model_id,
                    "varnn_rm",
                    version,
                    TARGET_COL,
                    float(r["pred"]),
                    float(r["actual"]),
                    "validation",
                    now,
                )
            )

        for _, r in pd.DataFrame(
            {
                "date": test_df["date"],
                "actual": y_test,
                "pred": test_pred,
            }
        ).iterrows():
            pred_rows.append(
                (
                    pd.to_datetime(r["date"]).date(),
                    model_id,
                    "varnn_rm",
                    version,
                    TARGET_COL,
                    float(r["pred"]),
                    float(r["actual"]),
                    "test",
                    now,
                )
            )

        registry_schema = """
        model_id string,
        model_name string,
        model_version string,
        mlflow_run_id string,
        mlflow_model_uri string,
        artifact_path string,
        feature_table string,
        feature_snapshot_path string,
        target_col string,
        is_active boolean,
        created_at timestamp
        """

        metrics_schema = """
        model_id string,
        model_name string,
        model_version string,
        dataset_split string,
        rmse double,
        mae double,
        r2 double,
        created_at timestamp
        """

        prediction_schema = """
        prediction_for_date date,
        model_id string,
        model_name string,
        model_version string,
        target_col string,
        predicted_value double,
        actual_value double,
        prediction_type string,
        created_at timestamp
        """

        spark.createDataFrame(registry, registry_schema) \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_MODEL_REGISTRY_TABLE)

        spark.createDataFrame(metrics_rows, metrics_schema) \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_MODEL_METRICS_TABLE)

        spark.createDataFrame(pred_rows, prediction_schema) \
            .write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_PREDICTION_TABLE)

        print(model_id)

        return model_id

    finally:
        cleanup_names = [
            "model_df",
            "train_df",
            "valid_df",
            "test_df",
            "X_train",
            "X_valid",
            "X_test",
            "y_train",
            "y_valid",
            "y_test",
            "X_train_imp",
            "X_valid_imp",
            "X_test_imp",
            "X_train_scaled",
            "X_valid_scaled",
            "X_test_scaled",
            "y_train_scaled",
            "y_valid_scaled",
            "y_test_scaled",
            "train_base_preds",
            "train_residuals",
            "train_res_lagged",
            "X_train_varnn",
            "valid_pred_scaled",
            "test_pred_scaled",
            "valid_pred",
            "test_pred",
            "registry",
            "metrics_rows",
            "pred_rows",
            "base_predictor",
            "varnn_rm",
            "imputer",
            "x_scaler",
            "y_scaler",
        ]

        for name in cleanup_names:
            if name in locals():
                try:
                    del locals()[name]
                except Exception:
                    pass

        try:
            spark.catalog.clearCache()
        except Exception:
            pass

        try:
            spark.stop()
        except Exception:
            pass

        gc.collect()


if __name__ == "__main__":
    train_varnn_rm()