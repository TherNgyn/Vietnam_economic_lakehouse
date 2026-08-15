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
import keras
import mlflow
import mlflow.keras
import numpy as np
import pandas as pd
import tensorflow as tf
from keras import layers
from keras import backend as K
from keras.callbacks import EarlyStopping
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
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


DEFAULT_FEATURE_VERSION = "tcn_features"
LOOKBACK = 12


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

    pdf = pd.DataFrame(
        {
            col: deeplake_col_to_list(ds, col)
            for col in columns
        }
    )

    if "date" in pdf.columns:
        pdf["date"] = pd.to_datetime(pdf["date"], errors="coerce")

    for col in pdf.columns:
        if col != "date":
            pdf[col] = pd.to_numeric(pdf[col], errors="coerce")

    pdf = pdf.replace([np.inf, -np.inf], np.nan)

    print(f"[INFO] Loaded snapshot shape: {pdf.shape}")

    if "date" in pdf.columns:
        print(
            f"[INFO] Snapshot date range: "
            f"{pdf['date'].min()} -> {pdf['date'].max()}"
        )

    return pdf


def create_window_sequences(X_data, y_data, lookback):
    X_seq = []
    y_seq = []
    indices = []

    for i in range(lookback, len(X_data)):
        X_seq.append(X_data[i - lookback:i, :])
        y_seq.append(y_data[i])
        indices.append(i)

    return np.asarray(X_seq), np.asarray(y_seq), np.asarray(indices)


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


def validate_prediction_dates(
    valid_pred_df: pd.DataFrame,
    test_pred_df: pd.DataFrame,
) -> None:
    valid_pred_df["date"] = pd.to_datetime(
        valid_pred_df["date"],
        errors="coerce",
    )
    test_pred_df["date"] = pd.to_datetime(
        test_pred_df["date"],
        errors="coerce",
    )

    if valid_pred_df["date"].isna().any():
        raise ValueError("Validation prediction date contains NaT")

    if test_pred_df["date"].isna().any():
        raise ValueError("Test prediction date contains NaT")

    valid_min = valid_pred_df["date"].min()
    test_min = test_pred_df["date"].min()

    if valid_min.year < 1990 or test_min.year < 1990:
        raise ValueError(
            "Prediction date looks invalid. "
            f"valid_min={valid_min}, test_min={test_min}"
        )


def train_tcn(feature_version: str = DEFAULT_FEATURE_VERSION):
    spark = get_spark()

    model_df = None
    train_df = None
    valid_df = None
    test_df = None

    try:
        np.random.seed(SEED)
        tf.keras.utils.set_random_seed(SEED)

        model_df = load_deeplake_snapshot_to_pandas(feature_version)
        feature_snapshot_path = f"{DEEPLAKE_URI_PREFIX.rstrip('/')}/{feature_version}"

        if "date" not in model_df.columns:
            raise ValueError("Feature snapshot thiếu cột date")

        if "target_next" not in model_df.columns:
            raise ValueError("Feature snapshot thiếu cột target_next")

        if TARGET_COL not in model_df.columns:
            raise ValueError(
                f"Feature snapshot thiếu cột {TARGET_COL}; cần cột này để tạo naive baseline"
            )

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

        if (
            len(train_df) <= LOOKBACK
            or len(valid_df) <= LOOKBACK
            or len(test_df) <= LOOKBACK
        ):
            raise ValueError(
                f"Invalid split sizes for LOOKBACK={LOOKBACK}: "
                f"train={len(train_df)}, valid={len(valid_df)}, test={len(test_df)}"
            )

        X_train = train_df.drop(columns=["target_next", "date"])
        y_train = train_df["target_next"]

        X_valid = valid_df.drop(columns=["target_next", "date"])
        y_valid = valid_df["target_next"]

        X_test = test_df.drop(columns=["target_next", "date"])
        y_test = test_df["target_next"]

        naive_valid_raw = X_valid[TARGET_COL]
        naive_test_raw = X_test[TARGET_COL]

        print("\n==== TIME SPLIT INFORMATION ====")
        print(f"Total observations: {len(model_df)}")
        print(f"Train size: {len(train_df)}")
        print(f"Validation size: {len(valid_df)}")
        print(f"Test size: {len(test_df)}")

        print("\nTrain period:")
        print(f"From {train_df['date'].min()} to {train_df['date'].max()}")

        print("\nValidation period:")
        print(f"From {valid_df['date'].min()} to {valid_df['date'].max()}")

        print("\nTest period:")
        print(f"From {test_df['date'].min()} to {test_df['date'].max()}")

        print("\nSố biến đầu vào:", len(X_train.columns))
        print("Tên biến đầu vào:")
        print(list(X_train.columns))

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

        X_train_tcn, y_train_tcn, _ = create_window_sequences(
            X_train_scaled,
            y_train_scaled,
            LOOKBACK,
        )

        X_valid_tcn, y_valid_tcn, valid_indices = create_window_sequences(
            X_valid_scaled,
            y_valid_scaled,
            LOOKBACK,
        )

        X_test_tcn, y_test_tcn, test_indices = create_window_sequences(
            X_test_scaled,
            y_test_scaled,
            LOOKBACK,
        )

        valid_dates = valid_df["date"].iloc[valid_indices].reset_index(drop=True)
        test_dates = test_df["date"].iloc[test_indices].reset_index(drop=True)

        y_valid_actual = y_valid.iloc[valid_indices].reset_index(drop=True)
        y_test_actual = y_test.iloc[test_indices].reset_index(drop=True)

        naive_valid = naive_valid_raw.iloc[valid_indices].reset_index(drop=True)
        naive_test = naive_test_raw.iloc[test_indices].reset_index(drop=True)

        print("Kích thước Tensor Train TCN:", X_train_tcn.shape)
        print("Kích thước Tensor Valid TCN:", X_valid_tcn.shape)
        print("Kích thước Tensor Test TCN:", X_test_tcn.shape)
        print("Kích thước y_train_tcn:", y_train_tcn.shape)
        print("Kích thước y_valid_tcn:", y_valid_tcn.shape)
        print("Kích thước y_test_tcn:", y_test_tcn.shape)

        K.clear_session()
        tf.keras.utils.set_random_seed(SEED)
        np.random.seed(SEED)

        n_timesteps = X_train_tcn.shape[1]
        n_features = X_train_tcn.shape[2]

        tcn_model = keras.Sequential(
            [
                keras.Input(
                    shape=(n_timesteps, n_features),
                    name="input_sequence",
                ),
                layers.Conv1D(
                    filters=16,
                    kernel_size=2,
                    dilation_rate=1,
                    padding="causal",
                    activation="tanh",
                    kernel_regularizer=keras.regularizers.l2(1e-4),
                    name="causal_conv_1",
                ),
                layers.Conv1D(
                    filters=16,
                    kernel_size=2,
                    dilation_rate=2,
                    padding="causal",
                    activation="tanh",
                    kernel_regularizer=keras.regularizers.l2(1e-3),
                    name="causal_conv_2",
                ),
                layers.Lambda(
                    lambda x: x[:, -1, :],
                    name="last_timestep",
                ),
                layers.Dense(
                    units=8,
                    activation="tanh",
                    kernel_regularizer=keras.regularizers.l2(1e-2),
                    name="dense_hidden",
                ),
                layers.Dense(
                    units=1,
                    activation="linear",
                    kernel_regularizer=keras.regularizers.l2(1e-2),
                    name="forecast_output",
                ),
            ],
            name="window_tcn_forecaster",
        )

        tcn_model.compile(
            optimizer=keras.optimizers.Adam(
                learning_rate=5e-4,
            ),
            loss="mse",
            metrics=[
                keras.metrics.RootMeanSquaredError(name="rmse"),
                keras.metrics.MeanAbsoluteError(name="mae"),
            ],
        )

        tcn_model.summary()

        early_stopping = EarlyStopping(
            monitor="val_loss",
            patience=20,
            restore_best_weights=True,
            verbose=1,
        )

        history = tcn_model.fit(
            X_train_tcn,
            y_train_tcn,
            epochs=100,
            batch_size=32,
            validation_data=(X_valid_tcn, y_valid_tcn),
            callbacks=[early_stopping],
            shuffle=False,
            verbose=1,
        )

        valid_pred_scaled = tcn_model.predict(
            X_valid_tcn,
            verbose=0,
        ).ravel()

        test_pred_scaled = tcn_model.predict(
            X_test_tcn,
            verbose=0,
        ).ravel()

        valid_pred = y_scaler.inverse_transform(
            valid_pred_scaled.reshape(-1, 1)
        ).ravel()

        test_pred = y_scaler.inverse_transform(
            test_pred_scaled.reshape(-1, 1)
        ).ravel()

        valid_metrics = regression_metrics(y_valid_actual, valid_pred)
        test_metrics = regression_metrics(y_test_actual, test_pred)

        baseline_valid_metrics = regression_metrics(y_valid_actual, naive_valid)
        baseline_test_metrics = regression_metrics(y_test_actual, naive_test)

        print("\n==== FIXED WINDOW TCN KERAS METRICS ====")
        print(
            pd.DataFrame(
                [valid_metrics, test_metrics],
                index=["Validation", "Test"],
            ).to_string()
        )

        print("\n==== BASELINE NAIVE METRICS ====")
        print(
            pd.DataFrame(
                [baseline_valid_metrics, baseline_test_metrics],
                index=["Naive_Validation", "Naive_Test"],
            ).to_string()
        )

        version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        model_id = f"tcn_window_{version}"

        artifact_prefix = f"{MODEL_PREFIX}/{version}"
        artifact_path = f"s3://{MODEL_BUCKET}/{artifact_prefix}"

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)

            tcn_model.save(td / "model.keras")

            joblib.dump(imputer, td / "imputer.pkl")
            joblib.dump(x_scaler, td / "x_scaler.pkl")
            joblib.dump(y_scaler, td / "y_scaler.pkl")

            with open(td / "feature_columns.json", "w", encoding="utf-8") as f:
                json.dump(
                    list(X_train.columns),
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            metadata = {
                "model_id": model_id,
                "model_name": "tcn_window",
                "model_version": version,
                "target_col": TARGET_COL,
                "feature_table": GOLD_MODEL_FEATURE_TABLE,
                "feature_snapshot_path": feature_snapshot_path,
                "feature_version": feature_version,
                "lookback": LOOKBACK,
                "n_timesteps": int(n_timesteps),
                "n_features": int(n_features),
            }

            with open(td / "metadata.json", "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)

            with open(td / "metrics.json", "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "validation": valid_metrics,
                        "test": test_metrics,
                        "baseline_validation": baseline_valid_metrics,
                        "baseline_test": baseline_test_metrics,
                    },
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            history_payload = {
                "loss": [float(x) for x in history.history.get("loss", [])],
                "val_loss": [float(x) for x in history.history.get("val_loss", [])],
                "rmse": [float(x) for x in history.history.get("rmse", [])],
                "val_rmse": [float(x) for x in history.history.get("val_rmse", [])],
                "mae": [float(x) for x in history.history.get("mae", [])],
                "val_mae": [float(x) for x in history.history.get("val_mae", [])],
            }

            with open(td / "history.json", "w", encoding="utf-8") as f:
                json.dump(history_payload, f, ensure_ascii=False, indent=2)

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
                        "model_name": "tcn_window",
                        "target_col": TARGET_COL,
                        "feature_table": GOLD_MODEL_FEATURE_TABLE,
                        "feature_snapshot_path": feature_snapshot_path,
                        "feature_version": feature_version,
                        "lookback": LOOKBACK,
                        "n_timesteps": int(n_timesteps),
                        "n_features": int(n_features),
                        "conv_filters": 16,
                        "kernel_size": 2,
                        "dilation_rates": "1,2",
                        "dense_units": 8,
                        "learning_rate": 5e-4,
                        "batch_size": 32,
                        "epochs": 100,
                        "early_stopping_patience": 20,
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
                        "baseline_valid_rmse": baseline_valid_metrics["rmse"],
                        "baseline_valid_mae": baseline_valid_metrics["mae"],
                        "baseline_valid_r2": baseline_valid_metrics["r2"],
                        "baseline_test_rmse": baseline_test_metrics["rmse"],
                        "baseline_test_mae": baseline_test_metrics["mae"],
                        "baseline_test_r2": baseline_test_metrics["r2"],
                    }
                )

                mlflow.log_artifact(str(td / "model.keras"))
                mlflow.log_artifact(str(td / "imputer.pkl"))
                mlflow.log_artifact(str(td / "x_scaler.pkl"))
                mlflow.log_artifact(str(td / "y_scaler.pkl"))
                mlflow.log_artifact(str(td / "feature_columns.json"))
                mlflow.log_artifact(str(td / "metadata.json"))
                mlflow.log_artifact(str(td / "metrics.json"))
                mlflow.log_artifact(str(td / "history.json"))

                try:
                    mlflow.keras.log_model(
                        model=tcn_model,
                        artifact_path="model",
                        registered_model_name=MLFLOW_REGISTERED_MODEL_NAME,
                    )
                except Exception as e:
                    print(f"[WARN] mlflow.keras.log_model failed: {e}")

                run_id = run.info.run_id
                mlflow_model_uri = f"runs:/{run_id}/model"

        now = datetime.utcnow()

        registry = [
            (
                model_id,
                "tcn_window",
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
                "tcn_window",
                version,
                "validation",
                valid_metrics["rmse"],
                valid_metrics["mae"],
                valid_metrics["r2"],
                now,
            ),
            (
                model_id,
                "tcn_window",
                version,
                "test",
                test_metrics["rmse"],
                test_metrics["mae"],
                test_metrics["r2"],
                now,
            ),
            (
                model_id,
                "tcn_window_baseline",
                version,
                "validation",
                baseline_valid_metrics["rmse"],
                baseline_valid_metrics["mae"],
                baseline_valid_metrics["r2"],
                now,
            ),
            (
                model_id,
                "tcn_window_baseline",
                version,
                "test",
                baseline_test_metrics["rmse"],
                baseline_test_metrics["mae"],
                baseline_test_metrics["r2"],
                now,
            ),
        ]

        valid_pred_df = pd.DataFrame(
            {
                "date": valid_dates,
                "actual": y_valid_actual,
                "pred": pd.Series(valid_pred).reset_index(drop=True),
            }
        )

        test_pred_df = pd.DataFrame(
            {
                "date": test_dates,
                "actual": y_test_actual,
                "pred": pd.Series(test_pred).reset_index(drop=True),
            }
        )

        validate_prediction_dates(valid_pred_df, test_pred_df)

        pred_rows = []

        for _, r in valid_pred_df.iterrows():
            pred_value = float(r["pred"])
            actual_value = float(r["actual"])

            if not np.isfinite(pred_value) or not np.isfinite(actual_value):
                continue

            pred_rows.append(
                (
                    pd.to_datetime(r["date"]).date(),
                    model_id,
                    "tcn_window",
                    version,
                    TARGET_COL,
                    pred_value,
                    actual_value,
                    "validation",
                    now,
                )
            )

        for _, r in test_pred_df.iterrows():
            pred_value = float(r["pred"])
            actual_value = float(r["actual"])

            if not np.isfinite(pred_value) or not np.isfinite(actual_value):
                continue

            pred_rows.append(
                (
                    pd.to_datetime(r["date"]).date(),
                    model_id,
                    "tcn_window",
                    version,
                    TARGET_COL,
                    pred_value,
                    actual_value,
                    "test",
                    now,
                )
            )

        if len(pred_rows) == 0:
            raise ValueError("No valid prediction rows to write")

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
            "X_train_tcn",
            "X_valid_tcn",
            "X_test_tcn",
            "y_train_tcn",
            "y_valid_tcn",
            "y_test_tcn",
            "valid_pred_scaled",
            "test_pred_scaled",
            "valid_pred",
            "test_pred",
            "registry",
            "metrics_rows",
            "pred_rows",
            "tcn_model",
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
            K.clear_session()
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

    train_tcn(feature_version=args.feature_version)