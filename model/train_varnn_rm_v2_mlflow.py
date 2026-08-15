import argparse
import copy
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
import mlflow.pytorch
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim
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


DEFAULT_FEATURE_VERSION = "varnn_features_diff_no_target"


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


class VARNN_RM_PyTorch(nn.Module):
    def __init__(self, input_dim, memory_dim=4, hidden_dims=(16, 8)):
        super(VARNN_RM_PyTorch, self).__init__()

        self.input_dim = input_dim
        self.memory_dim = memory_dim

        self.W_epsilon = nn.Parameter(torch.randn(1, memory_dim) * 0.1)
        self.b_epsilon = nn.Parameter(torch.zeros(memory_dim))

        mlp_input_dim = input_dim + memory_dim

        self.mlp = nn.Sequential(
            nn.Linear(mlp_input_dim, hidden_dims[0]),
            nn.Tanh(),
            nn.Linear(hidden_dims[0], hidden_dims[1]),
            nn.Tanh(),
            nn.Linear(hidden_dims[1], 1),
        )

    def forward(self, X, y_true=None, initial_residual=0.0):
        T = X.size(0)
        predictions = []

        current_residual = torch.tensor(
            [initial_residual],
            device=X.device,
            dtype=X.dtype,
        )

        for t in range(T):
            h_memory = torch.tanh(
                current_residual.reshape(1, 1) @ self.W_epsilon
                + self.b_epsilon
            ).squeeze(0)

            X_t = X[t]

            combined_input = torch.cat(
                [X_t, h_memory],
                dim=0,
            ).unsqueeze(0)

            y_hat_t = self.mlp(combined_input).squeeze()

            predictions.append(y_hat_t)

            if y_true is not None:
                current_residual = (y_true[t] - y_hat_t).reshape(1)
            else:
                current_residual = torch.zeros(
                    1,
                    device=X.device,
                    dtype=X.dtype,
                )

        return torch.stack(predictions)


def train_varnn_rm_v2(feature_version: str = DEFAULT_FEATURE_VERSION):
    spark = get_spark()

    model_df = None
    train_df = None
    valid_df = None
    test_df = None

    try:
        np.random.seed(SEED)
        torch.manual_seed(SEED)

        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(SEED)

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
            .dropna(subset=["target_next"])
            .copy()
        )

        print(f"[INFO] Rows before drop target_next null: {before_dropna}")
        print(f"[INFO] Rows after drop target_next null: {len(model_df)}")
        print(f"[INFO] Rows dropped: {before_dropna - len(model_df)}")

        if len(model_df) == 0:
            raise ValueError("Không có dữ liệu train sau khi drop target_next null")

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

        print("\nNumber of training features:", len(X_train.columns))
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

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        X_train_tensor = torch.tensor(
            X_train_scaled,
            dtype=torch.float32,
            device=device,
        )

        y_train_tensor = torch.tensor(
            y_train_scaled,
            dtype=torch.float32,
            device=device,
        )

        X_valid_tensor = torch.tensor(
            X_valid_scaled,
            dtype=torch.float32,
            device=device,
        )

        y_valid_tensor = torch.tensor(
            y_valid_scaled,
            dtype=torch.float32,
            device=device,
        )

        X_test_tensor = torch.tensor(
            X_test_scaled,
            dtype=torch.float32,
            device=device,
        )

        y_test_tensor = torch.tensor(
            y_test_scaled,
            dtype=torch.float32,
            device=device,
        )

        print("\n==== SHAPE INFORMATION ====")
        print("X_train_tensor:", tuple(X_train_tensor.shape))
        print("X_valid_tensor:", tuple(X_valid_tensor.shape))
        print("X_test_tensor :", tuple(X_test_tensor.shape))
        print("y_train_tensor:", tuple(y_train_tensor.shape))
        print("y_valid_tensor:", tuple(y_valid_tensor.shape))
        print("y_test_tensor :", tuple(y_test_tensor.shape))

        input_dim = X_train_tensor.shape[1]

        model = VARNN_RM_PyTorch(
            input_dim=input_dim,
            memory_dim=4,
            hidden_dims=(16, 8),
        ).to(device)

        criterion = nn.MSELoss()

        optimizer = optim.Adam(
            model.parameters(),
            lr=1e-3,
            weight_decay=1e-2,
        )

        epochs = 500
        patience = 20

        best_val_loss = float("inf")
        patience_counter = 0
        best_model_state = None

        train_losses = []
        val_losses = []

        for epoch in range(epochs):
            model.train()
            optimizer.zero_grad()

            train_outputs = model(
                X_train_tensor,
                y_train_tensor,
                initial_residual=0.0,
            )

            train_loss = criterion(train_outputs, y_train_tensor)

            train_loss.backward()
            optimizer.step()

            train_losses.append(float(train_loss.item()))

            model.eval()

            with torch.no_grad():
                train_outputs_eval = model(
                    X_train_tensor,
                    y_train_tensor,
                    initial_residual=0.0,
                )

                last_train_res = (
                    y_train_tensor[-1] - train_outputs_eval[-1]
                ).item()

                val_outputs = model(
                    X_valid_tensor,
                    y_valid_tensor,
                    initial_residual=last_train_res,
                )

                val_loss = criterion(val_outputs, y_valid_tensor)
                val_losses.append(float(val_loss.item()))

            if val_loss.item() < best_val_loss:
                best_val_loss = float(val_loss.item())
                patience_counter = 0
                best_model_state = copy.deepcopy(model.state_dict())
            else:
                patience_counter += 1

            if patience_counter >= patience:
                print(f"-> Early stopping kích hoạt tại epoch {epoch + 1}")
                break

        if best_model_state is not None:
            model.load_state_dict(best_model_state)

        model.eval()

        with torch.no_grad():
            train_preds_tensor = model(
                X_train_tensor,
                y_train_tensor,
                initial_residual=0.0,
            )

            last_train_res = (
                y_train_tensor[-1] - train_preds_tensor[-1]
            ).item()

            valid_preds_tensor = model(
                X_valid_tensor,
                y_valid_tensor,
                initial_residual=last_train_res,
            )

            last_valid_res = (
                y_valid_tensor[-1] - valid_preds_tensor[-1]
            ).item()

            test_preds_tensor = model(
                X_test_tensor,
                y_test_tensor,
                initial_residual=last_valid_res,
            )

        valid_pred_scaled = valid_preds_tensor.detach().cpu().numpy()
        test_pred_scaled = test_preds_tensor.detach().cpu().numpy()

        valid_pred = y_scaler.inverse_transform(
            valid_pred_scaled.reshape(-1, 1)
        ).ravel()

        test_pred = y_scaler.inverse_transform(
            test_pred_scaled.reshape(-1, 1)
        ).ravel()

        valid_metrics = regression_metrics(y_valid, valid_pred)
        test_metrics = regression_metrics(y_test, test_pred)

        naive_valid = valid_df[f"{TARGET_COL}_at_t"]
        naive_test = test_df[f"{TARGET_COL}_at_t"]

        baseline_valid_metrics = regression_metrics(y_valid, naive_valid)
        baseline_test_metrics = regression_metrics(y_test, naive_test)

        mlp_params = sum(p.numel() for p in model.mlp.parameters())
        res_w_params = model.W_epsilon.numel()
        res_b_params = model.b_epsilon.numel()
        total_params = mlp_params + res_w_params + res_b_params

        print("\n==== VARNN-RM PYTORCH V2 METRICS ====")
        print(pd.DataFrame(
            [valid_metrics, test_metrics],
            index=["Validation", "Test"],
        ).to_string())

        print("\n==== BASELINE NAIVE METRICS ====")
        print(pd.DataFrame(
            [baseline_valid_metrics, baseline_test_metrics],
            index=["Naive_Validation", "Naive_Test"],
        ).to_string())

        print("\n==== PARAMETER COUNT PYTORCH V2 ====")
        print(f"Input dimension              : {input_dim}")
        print(f"Memory dimension             : {model.memory_dim}")
        print(f"MLP Predictor Parameters     : {mlp_params}")
        print(f"Residual Memory W_epsilon    : {res_w_params}")
        print(f"Residual Memory b_epsilon    : {res_b_params}")
        print(f"Total Learnable Parameters   : {total_params}")

        version = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        model_id = f"varnn_rm_v2_{version}"

        artifact_prefix = f"{MODEL_PREFIX}/{version}"
        artifact_path = f"s3://{MODEL_BUCKET}/{artifact_prefix}"

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)

            torch.save(model.state_dict(), td / "model_state_dict.pt")
            torch.save(model, td / "model_full.pt")

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
                "model_name": "varnn_rm_v2",
                "model_version": version,
                "target_col": TARGET_COL,
                "feature_table": GOLD_MODEL_FEATURE_TABLE,
                "feature_snapshot_path": feature_snapshot_path,
                "feature_version": feature_version,
                "input_dim": int(input_dim),
                "memory_dim": int(model.memory_dim),
                "hidden_dims": [16, 8],
                "last_train_residual": float(last_train_res),
                "last_valid_residual": float(last_valid_res),
                "total_params": int(total_params),
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

            with open(td / "loss_curve.json", "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "train_losses": train_losses,
                        "val_losses": val_losses,
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
                        "model_name": "varnn_rm_v2",
                        "target_col": TARGET_COL,
                        "feature_table": GOLD_MODEL_FEATURE_TABLE,
                        "feature_snapshot_path": feature_snapshot_path,
                        "feature_version": feature_version,
                        "input_dim": int(input_dim),
                        "memory_dim": int(model.memory_dim),
                        "hidden_dims": "(16,8)",
                        "epochs": epochs,
                        "patience": patience,
                        "lr": 1e-3,
                        "weight_decay": 1e-2,
                        "total_params": int(total_params),
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
                        "best_val_loss": float(best_val_loss),
                    }
                )

                mlflow.log_artifact(str(td / "model_state_dict.pt"))
                mlflow.log_artifact(str(td / "model_full.pt"))
                mlflow.log_artifact(str(td / "imputer.pkl"))
                mlflow.log_artifact(str(td / "x_scaler.pkl"))
                mlflow.log_artifact(str(td / "y_scaler.pkl"))
                mlflow.log_artifact(str(td / "feature_columns.json"))
                mlflow.log_artifact(str(td / "metadata.json"))
                mlflow.log_artifact(str(td / "metrics.json"))
                mlflow.log_artifact(str(td / "loss_curve.json"))

                try:
                    mlflow.pytorch.log_model(
                        pytorch_model=model,
                        artifact_path="model",
                        registered_model_name=MLFLOW_REGISTERED_MODEL_NAME,
                    )
                except Exception as e:
                    print(f"[WARN] mlflow.pytorch.log_model failed: {e}")

                run_id = run.info.run_id
                mlflow_model_uri = f"runs:/{run_id}/model"

        now = datetime.utcnow()

        registry = [
            (
                model_id,
                "varnn_rm_v2",
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
                "varnn_rm_v2",
                version,
                "validation",
                valid_metrics["rmse"],
                valid_metrics["mae"],
                valid_metrics["r2"],
                now,
            ),
            (
                model_id,
                "varnn_rm_v2",
                version,
                "test",
                test_metrics["rmse"],
                test_metrics["mae"],
                test_metrics["r2"],
                now,
            ),
            (
                model_id,
                "varnn_rm_v2_baseline",
                version,
                "validation",
                baseline_valid_metrics["rmse"],
                baseline_valid_metrics["mae"],
                baseline_valid_metrics["r2"],
                now,
            ),
            (
                model_id,
                "varnn_rm_v2_baseline",
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
                    "varnn_rm_v2",
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
                    "varnn_rm_v2",
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
            "X_train_tensor",
            "X_valid_tensor",
            "X_test_tensor",
            "y_train_tensor",
            "y_valid_tensor",
            "y_test_tensor",
            "train_preds_tensor",
            "valid_preds_tensor",
            "test_preds_tensor",
            "valid_pred_scaled",
            "test_pred_scaled",
            "valid_pred",
            "test_pred",
            "registry",
            "metrics_rows",
            "pred_rows",
            "model",
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
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
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

    train_varnn_rm_v2(feature_version=args.feature_version)