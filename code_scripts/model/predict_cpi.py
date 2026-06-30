import os
import pickle
import numpy as np
import pandas as pd
import s3fs
from datetime import datetime, timedelta
from deltalake import write_deltalake

SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
    "AWS_S3_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}


def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )


def load_model(model_name: str):
    fs = get_s3fs()
    path = f"{SILVER_BUCKET}/models/cpi/{model_name}_latest.pkl"
    with fs.open(path, "rb") as f:
        return pickle.load(f)


def load_latest_features() -> pd.DataFrame:
    fs = get_s3fs()
    frames = {}
    paths = {
        "cpi": f"{SILVER_BUCKET}/economics/cpi",
        "m2": f"{SILVER_BUCKET}/economics/broad_money",
        "ir": f"{SILVER_BUCKET}/economics/interest_rate",
        "gasoline": f"{SILVER_BUCKET}/product/gasoline",
    }
    for name, path in paths.items():
        try:
            files = fs.glob(f"{path}/**/*.parquet")
            if not files:
                files = fs.glob(f"{path}/*.parquet")
            dfs = []
            for fp in files:
                with fs.open(fp, "rb") as f:
                    dfs.append(pd.read_parquet(f))
            if dfs:
                frames[name] = pd.concat(dfs, ignore_index=True)
        except Exception:
            pass
    return frames


def build_future_features(frames: dict, feature_cols: list, horizon: int) -> pd.DataFrame:
    base_vals = {}
    for col in feature_cols:
        base_vals[col] = 0.0

    if "cpi" in frames and not frames["cpi"].empty:
        cpi_df = frames["cpi"].sort_values("date")
        for lag in [1, 2, 3, 6, 12]:
            key = f"cpi_lag_{lag}"
            if key in feature_cols and len(cpi_df) >= lag:
                base_vals[key] = float(cpi_df["value"].iloc[-lag])

    if "m2" in frames and not frames["m2"].empty:
        m2_df = frames["m2"].sort_values("date")
        for lag in [1, 3]:
            key = f"m2_yoy_lag_{lag}"
            if key in feature_cols and len(m2_df) >= lag:
                base_vals[key] = float(m2_df["m2_yoy_growth"].iloc[-lag])

    if "ir" in frames and not frames["ir"].empty:
        ir_df = frames["ir"].sort_values("date")
        if "ir_lag_1" in feature_cols and len(ir_df) >= 1:
            base_vals["ir_lag_1"] = float(ir_df["interest_rate"].iloc[-1])

    if "gasoline" in frames and not frames["gasoline"].empty:
        gas_df = frames["gasoline"].sort_values("date")
        if "gas_lag_1" in feature_cols and len(gas_df) >= 1:
            base_vals["gas_lag_1"] = float(gas_df["price_92"].iloc[-1] if "price_92" in gas_df.columns else 0)

    base_date = datetime.utcnow().replace(day=1)
    rows = []
    for h in range(1, horizon + 1):
        future_date = base_date + timedelta(days=30 * h)
        row = dict(base_vals)
        row["month"] = future_date.month
        row["_date"] = future_date.strftime("%Y-%m-%d")
        rows.append(row)

    return pd.DataFrame(rows)


def main():
    horizon = int(os.getenv("CPI_FORECAST_HORIZON", "3"))
    trained_at = datetime.utcnow().isoformat()
    frames = load_latest_features()
    records = []

    for model_name in ["xgboost", "lightgbm"]:
        try:
            payload = load_model(model_name)
            model = payload["model"]
            feature_cols = payload["feature_cols"]
            meta = payload["meta"]
            std_err = float(np.std([])) if "std_err" not in meta else meta["std_err"]

            future_df = build_future_features(frames, feature_cols, horizon)
            dates = future_df.pop("_date")
            X_future = future_df[feature_cols]
            preds = model.predict(X_future)

            for date, pred in zip(dates, preds):
                records.append({
                    "date": date,
                    "actual_cpi": None,
                    "predicted_cpi": float(pred),
                    "lower_bound": float(pred - 1.96 * std_err),
                    "upper_bound": float(pred + 1.96 * std_err),
                    "model_name": model_name,
                    "mae": meta.get("mae", 0.0),
                    "rmse": meta.get("rmse", 0.0),
                    "mape": meta.get("mape", 0.0),
                    "horizon_months": horizon,
                    "trained_at": trained_at,
                })
        except Exception:
            continue

    if records:
        df = pd.DataFrame(records)
        write_deltalake(
            f"s3://{SILVER_BUCKET}/cpi_forecast_future",
            df,
            storage_options=DELTA_STORAGE_OPTIONS,
            mode="overwrite",
            schema_mode="overwrite",
        )


if __name__ == "__main__":
    main()
