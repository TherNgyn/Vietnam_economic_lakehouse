import gc

import boto3
from botocore.client import Config

from spark_session import get_spark
from config import (
    GOLD_MONTHLY_FEATURE_TABLE,
    GOLD_MODEL_FEATURE_TABLE,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)
from feature_engineering import build_tcn_features


MODEL_FEATURE_PATH = "s3a://gold/model/features/inflation_model_features"
MODEL_FEATURE_BUCKET = "gold"
MODEL_FEATURE_PREFIX = "model/features/inflation_model_features"


def delete_minio_prefix(bucket: str, prefix: str):
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    prefix = prefix.rstrip("/") + "/"
    paginator = client.get_paginator("list_objects_v2")
    deleted = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        objects = page.get("Contents", [])

        if not objects:
            continue

        client.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": [
                    {"Key": obj["Key"]}
                    for obj in objects
                ]
            },
        )

        deleted += len(objects)

    print(f"Deleted {deleted} object(s) from s3://{bucket}/{prefix}")


def build_model_features():
    spark = get_spark()

    raw = None
    model_df = None
    model_sdf = None

    try:
        raw = spark.table(GOLD_MONTHLY_FEATURE_TABLE).toPandas()

        if "date" in raw.columns:
            raw = raw.sort_values("date").reset_index(drop=True)
            print(f"[INFO] Raw date range: {raw['date'].min()} -> {raw['date'].max()}")

        print(f"[INFO] Raw monthly feature shape: {raw.shape}")

        model_df = build_tcn_features(raw, keep_target_null=True)

        print(f"[INFO] Model feature shape: {model_df.shape}")

        if "date" in model_df.columns:
            print(f"[INFO] Model feature date range: {model_df['date'].min()} -> {model_df['date'].max()}")

        spark.sql(f"DROP TABLE IF EXISTS {GOLD_MODEL_FEATURE_TABLE}")
        delete_minio_prefix(MODEL_FEATURE_BUCKET, MODEL_FEATURE_PREFIX)

        model_sdf = spark.createDataFrame(model_df)

        (
            model_sdf
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(MODEL_FEATURE_PATH)
        )

        spark.sql(f"""
        CREATE TABLE {GOLD_MODEL_FEATURE_TABLE}
        USING DELTA
        LOCATION '{MODEL_FEATURE_PATH}'
        """)

        print(f"[INFO] Created table: {GOLD_MODEL_FEATURE_TABLE}")
        print(f"[INFO] Feature path: {MODEL_FEATURE_PATH}")

    finally:
        print("[CLEANUP] Start cleanup")

        try:
            if model_sdf is not None:
                model_sdf.unpersist(blocking=False)
                print("[CLEANUP] Spark model_sdf unpersisted")
        except Exception as e:
            print(f"[CLEANUP][WARN] model_sdf unpersist failed: {e}")

        try:
            spark.catalog.clearCache()
            print("[CLEANUP] Spark cache cleared")
        except Exception as e:
            print(f"[CLEANUP][WARN] clearCache failed: {e}")

        try:
            del raw
        except Exception:
            pass

        try:
            del model_df
        except Exception:
            pass

        try:
            del model_sdf
        except Exception:
            pass

        try:
            spark.stop()
            print("[CLEANUP] Spark stopped")
        except Exception as e:
            print(f"[CLEANUP][WARN] spark.stop failed: {e}")

        gc.collect()
        print("[CLEANUP] Python GC collected")


if __name__ == "__main__":
    build_model_features()