import argparse
import gc
import json
import os
import re
import tempfile
from datetime import datetime
from math import sqrt
from pathlib import Path

import deeplake
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


DEFAULT_FEATURE_VERSION = "varnn_features_diff_all"


def get_deeplake_creds():
    return {
        "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "endpoint_url": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
    }


def get_deeplake_columns(ds):
    try:
        return list(ds.schema.keys())
    except Exception:
        pass

    try:
        return list(ds.tensors.keys())
    except Exception:
        pass

    match = re.search(r"columns=\((.*?)\),\s*length=", str(ds))
    if match:
        return [c.strip() for c in match.group(1).split(",") if c.strip()]

    raise RuntimeError("Không lấy được danh sách columns từ Deep Lake dataset")


def clean_deeplake_value(v):
    if isinstance(v, list) and len(v) == 1:
        v = v[0]

    if isinstance(v, bytes):
        return v.decode("utf-8")

    return v


def deeplake_col_to_list(ds, col):
    obj = ds[col]

    try:
        values = obj.numpy()
    except Exception:
        try:
            values = obj[:].numpy()
        except Exception:
            values = list(obj)

    if hasattr(values, "tolist"):
        values = values.tolist()

    return [clean_deeplake_value(v) for v in values]


def load_deeplake_snapshot_to_pandas(feature_version: str) -> pd.DataFrame:
    dataset_path = f"{DEEPLAKE_URI_PREFIX.rstrip('/')}/{feature_version}"

    print(f"[INFO] Loading Deep Lake feature snapshot: {dataset_path}")

    ds = deeplake.open(dataset_path, creds=get_deeplake_creds())
    columns = get_deeplake_columns(ds)

    print(f"[INFO] Snapshot rows: {len(ds)}")
    print(f"[INFO] Snapshot columns: {len(columns)}")

    pdf = pd.DataFrame({col: deeplake_col_to_list(ds, col) for col in columns})

    if "date" in pdf.columns:
        pdf["date"] = pd.to_datetime(pdf["date"], errors="coerce")

    for col in pdf.columns:
        if col != "date":
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

    pdf = pdf.replace([np.inf, -np.inf], np.nan)

    print(f"[INFO] Loaded snapshot shape: {pdf.shape}")

    if "date" in pdf.columns:
        print(f"[INFO] Snapshot date range: {pdf['date'].min()} -> {pdf['date'].max()}")

    return pdf


def regression_metrics(y_true, y_pred):
    y_true = pd.Series(y_true).reset_index(drop=True)
    y_pred = pd.Series(y_pred).reset_index(drop=True)

    y_true = pd.to_numeric(y_true, errors="coerce")
    y_pred = pd.to_numeric(y_pred, errors="coerce")

    mask = (
        y_true.notna()
        & y_pred.notna()
        & np.isfinite(y_true.to_numpy(dtype=float))
        & np.isfinite(y_pred.to_numpy(dtype=float))
    )

    dropped = int((~mask).sum())
    if dropped > 0:
        print(f"[WARN] regression_metrics dropped {dropped} rows with NaN/inf")

    y_true_clean = y_true[mask].to_numpy(dtype=float)
    y_pred_clean = y_pred[mask].to_numpy(dtype=float)

    if len(y_true_clean) == 0:
        raise ValueError("No valid rows for metrics after dropping NaN/inf")

    return {
        "rmse": float(sqrt(mean_squared_error(y_true_clean, y_pred_clean))),
        "mae": float(mean_absolute_error(y_true_clean, y_pred_clean)),
        "r2": float(r2_score(y_true_clean, y_pred_clean)),
    }


def train_varnn_rm(feature_version: str = DEFAULT_FEATURE_VERSION):
    spark = get_spark()

    try:
        model_df = load_deeplake_snapshot_to_pandas(feature_version)
        feature_snapshot_path = f"{DEEPLAKE_URI_PREFIX.rstrip('/')}/{feature_version}"

        if "date" not in model_df.columns:
            raise ValueError("Feature snapshot thiếu cột date")

        if "target_next" not in model_df.columns:
            raise ValueError("Feature snapshot thiếu cột target_next")

        before_dropna = len(model_df)

        model_df = (
            model_df
            .sort_values("date")
            .dropna()
            .copy()
        )

        print(f"[INFO] Rows before dropna: {before_dropna}")
        print(f"[INFO] Rows after dropna: {len(model_df)}")
        print(f"[INFO] Rows dropped by dropna: {before_dropna - len(model_df)}")

        if len(model_df) == 0:
            raise ValueError("Không có dữ liệu train sau khi dropna")

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
            hidden_layer_sizes=(32, 16),
            activation="relu",
            solver="adam",
            alpha=1e-2,
            max_iter=1000,
            random_state=SEED,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=50,
        )

        base_predictor.fit(X_train_scaled, y_train_scaled)

        train_base_preds = base_predictor.predict(X_train_scaled)
        train_residuals = y_train_scaled - train_base_preds

        train_res_lagged = np.concatenate(
            ([0], train_residuals[:-1])
        )[:, np.newaxis]

        X_train_varnn = np.hstack([X_train_scaled, train_res_lagged])

        varnn_rm = MLPRegressor(
            hidden_layer_sizes=(16, 8),
            activation="tanh",
            solver="adam",
            alpha=1e-2,
            learning_rate_init=5e-4,
            max_iter=2000,
            random_state=SEED,
            early_stopping=True,
            validation_fraction=0.15,
            n_iter_no_change=30,
        )

        varnn_rm.fit(X_train_varnn, y_train_scaled)

        def recursive_predict(X_scaled, y_true_scaled, initial_residual):
            predictions = []
            current_residual = float(initial_residual)

            for i in range(len(X_scaled)):
                if not np.isfinite(current_residual):
                    print(
                        f"[WARN] current_residual not finite at step {i}: "
                        f"{current_residual}"
                    )
                    current_residual = 0.0

                input_vector = np.hstack(
                    [X_scaled[i], current_residual]
                ).reshape(1, -1)

                if not np.isfinite(input_vector).all():
                    bad_count = int((~np.isfinite(input_vector)).sum())
                    raise ValueError(
                        f"Input vector contains NaN/inf at recursive step {i}, "
                        f"bad_count={bad_count}"
                    )

                pred_scaled = varnn_rm.predict(input_vector)[0]

                if not np.isfinite(pred_scaled):
                    print(f"[WARN] pred_scaled not finite at step {i}: {pred_scaled}")
                    pred_scaled = 0.0

                predictions.append(pred_scaled)
                current_residual = float(y_true_scaled[i] - pred_scaled)

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
                "feature_version": feature_version,
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
                        "feature_version": feature_version,
                        "base_hidden_layer_sizes": "(32,16)",
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
                    "base_predictor": str(td / "base_predictor.pkl"),
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

        valid_pred_df = pd.DataFrame(
            {
                "date": valid_df["date"].reset_index(drop=True),
                "actual": y_valid.reset_index(drop=True),
                "pred": pd.Series(valid_pred).reset_index(drop=True),
            }
        )

        test_pred_df = pd.DataFrame(
            {
                "date": test_df["date"].reset_index(drop=True),
                "actual": y_test.reset_index(drop=True),
                "pred": pd.Series(test_pred).reset_index(drop=True),
            }
        )

        pred_rows = []

        for _, r in valid_pred_df.iterrows():
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

        for _, r in test_pred_df.iterrows():
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
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--feature-version",
        default=DEFAULT_FEATURE_VERSION,
        help="Deep Lake feature snapshot version",
    )
    args = parser.parse_args()

    train_varnn_rm(feature_version=args.feature_version)