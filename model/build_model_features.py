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
from feature_engineering import build_varnn_features


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

    raw = spark.table(GOLD_MONTHLY_FEATURE_TABLE).toPandas()
    model_df = build_varnn_features(raw, keep_target_null=True)

    spark.sql(f"DROP TABLE IF EXISTS {GOLD_MODEL_FEATURE_TABLE}")
    delete_minio_prefix(MODEL_FEATURE_BUCKET, MODEL_FEATURE_PREFIX)

    (
        spark.createDataFrame(model_df)
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

    print(GOLD_MODEL_FEATURE_TABLE)


if __name__ == "__main__":
    build_model_features()