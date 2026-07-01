import os
import json
import pickle
import numpy as np
import pandas as pd
import s3fs
from datetime import datetime
from pyspark.sql import SparkSession
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error, mean_squared_error
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from deltalake import write_deltalake

SILVER_BUCKET = os.getenv("MINIO_BUCKET_SILVER", "silver")

DELTA_STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    "AWS_S3_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ALLOW_HTTP": "true",
}


def get_spark():
    return (
        SparkSession.builder.appName("CPI-Train")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive:9083")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_s3fs():
    return s3fs.S3FileSystem(
        key=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        secret=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
        use_ssl=False,
    )


def load_features(spark) -> pd.DataFrame:
    frames = {}

    def try_table(tbl, col_map):
        try:
            df = spark.table(tbl).toPandas()
            df = df.rename(columns=col_map)
            df["date"] = pd.to_datetime(df["date"])
            return df.sort_values("date").set_index("date")
        except Exception:
            return None

    frames["cpi"] = try_table(
        "silver.economics_indicators",
        {"date": "date", "value": "cpi"},
    )
    if frames["cpi"] is not None:
        frames["cpi"] = frames["cpi"][frames["cpi"].get("indicator", "cpi") == "cpi"][["cpi"]]

    frames["m2"] = try_table("silver.broad_money", {"date": "date", "m2": "m2", "m2_yoy_growth": "m2_yoy_growth"})
    frames["ir"] = try_table("silver.interest_rate", {"date": "date", "interest_rate": "interest_rate_1m"})
    frames["gasoline"] = try_table("silver.gasoline", {"date": "date", "price_92": "gasoline_price"})

    base = None
    for name, df in frames.items():
        if df is None:
            continue
        if base is None:
            base = df
        else:
            base = base.join(df, how="outer")

    if base is None or "cpi" not in base.columns:
        raise ValueError("CPI data not available from silver layer")

    base = base.resample("MS").last().ffill().dropna(subset=["cpi"])
    return base.reset_index()


def build_features(df: pd.DataFrame, horizon: int = 3) -> tuple:
    df = df.copy().sort_values("date").reset_index(drop=True)

    for lag in [1, 2, 3, 6, 12]:
        if "cpi" in df.columns:
            df[f"cpi_lag_{lag}"] = df["cpi"].shift(lag)
    if "m2_yoy_growth" in df.columns:
        for lag in [1, 3]:
            df[f"m2_yoy_lag_{lag}"] = df["m2_yoy_growth"].shift(lag)
    if "interest_rate_1m" in df.columns:
        df["ir_lag_1"] = df["interest_rate_1m"].shift(1)
    if "gasoline_price" in df.columns:
        df["gas_lag_1"] = df["gasoline_price"].shift(1)

    df["month"] = pd.to_datetime(df["date"]).dt.month
    df["target"] = df["cpi"].shift(-horizon)
    df = df.dropna()

    feature_cols = [c for c in df.columns if c.startswith(("cpi_lag", "m2_yoy", "ir_lag", "gas_lag", "month"))]
    X = df[feature_cols]
    y = df["target"]
    dates = df["date"]
    return X, y, dates, feature_cols


def mape(y_true, y_pred):
    mask = y_true != 0
    return np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100


def train_and_evaluate(X, y, dates, model_name: str, horizon: int):
    tscv = TimeSeriesSplit(n_splits=5)
    maes, rmses, mapes = [], [], []

    if model_name == "xgboost":
        model = XGBRegressor(n_estimators=200, max_depth=4, learning_rate=0.05, subsample=0.8, random_state=42, verbosity=0)
    else:
        model = LGBMRegressor(n_estimators=200, max_depth=4, learning_rate=0.05, subsample=0.8, random_state=42, verbose=-1)

    for train_idx, val_idx in tscv.split(X):
        X_tr, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_tr, y_val = y.iloc[train_idx], y.iloc[val_idx]
        model.fit(X_tr, y_tr)
        preds = model.predict(X_val)
        maes.append(mean_absolute_error(y_val, preds))
        rmses.append(np.sqrt(mean_squared_error(y_val, preds)))
        mapes.append(mape(y_val.values, preds))

    model.fit(X, y)

    cv_mae = float(np.mean(maes))
    cv_rmse = float(np.mean(rmses))
    cv_mape = float(np.mean(mapes))

    in_sample_pred = model.predict(X)
    residuals = y.values - in_sample_pred
    std_err = float(np.std(residuals))

    return model, cv_mae, cv_rmse, cv_mape, std_err


def save_model_to_s3(model, model_name: str, feature_cols: list, meta: dict):
    fs = get_s3fs()
    trained_at = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = f"{SILVER_BUCKET}/models/cpi/{model_name}_{trained_at}.pkl"
    payload = {"model": model, "feature_cols": feature_cols, "meta": meta, "trained_at": trained_at}
    with fs.open(path, "wb") as f:
        pickle.dump(payload, f)
    latest_path = f"{SILVER_BUCKET}/models/cpi/{model_name}_latest.pkl"
    with fs.open(latest_path, "wb") as f:
        pickle.dump(payload, f)


def write_forecast_to_silver(records: list):
    df = pd.DataFrame(records)
    write_deltalake(
        f"s3://{SILVER_BUCKET}/cpi_forecast",
        df,
        storage_options=DELTA_STORAGE_OPTIONS,
        mode="overwrite",
        schema_mode="overwrite",
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = load_features(spark)
    horizon = int(os.getenv("CPI_FORECAST_HORIZON", "3"))

    records = []
    trained_at = datetime.utcnow().isoformat()

    for model_name in ["xgboost", "lightgbm"]:
        X, y, dates, feature_cols = build_features(raw_df, horizon=horizon)
        model, mae, rmse, mape_val, std_err = train_and_evaluate(X, y, dates, model_name, horizon)

        meta = {"mae": mae, "rmse": rmse, "mape": mape_val, "horizon_months": horizon}
        save_model_to_s3(model, model_name, feature_cols, meta)

        preds = model.predict(X)
        for i, (date, actual, pred) in enumerate(zip(dates, y.values, preds)):
            records.append({
                "date": str(date)[:10],
                "actual_cpi": float(actual),
                "predicted_cpi": float(pred),
                "lower_bound": float(pred - 1.96 * std_err),
                "upper_bound": float(pred + 1.96 * std_err),
                "model_name": model_name,
                "mae": mae,
                "rmse": rmse,
                "mape": mape_val,
                "horizon_months": horizon,
                "trained_at": trained_at,
            })

    if records:
        write_forecast_to_silver(records)

    spark.stop()


if __name__ == "__main__":
    main()
