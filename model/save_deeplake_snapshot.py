import json
import pandas as pd
import deeplake

from spark_session import get_spark
from config import (
    GOLD_MODEL_FEATURE_TABLE,
    DEEPLAKE_URI_PREFIX,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


def get_deeplake_creds():
    return {
        "aws_access_key_id": MINIO_ACCESS_KEY,
        "aws_secret_access_key": MINIO_SECRET_KEY,
        "endpoint_url": MINIO_ENDPOINT,
    }


def save_deeplake_snapshot(version: str):
    spark = get_spark()

    pdf = spark.table(GOLD_MODEL_FEATURE_TABLE).toPandas()
    dataset_path = f"{DEEPLAKE_URI_PREFIX.rstrip('/')}/{version}"

    creds = get_deeplake_creds()

    ds = deeplake.create(dataset_path, creds=creds)

    for col in pdf.columns:
        if pd.api.types.is_integer_dtype(pdf[col]):
            ds.add_column(col, "int64")
        elif pd.api.types.is_float_dtype(pdf[col]):
            ds.add_column(col, "float64")
        elif pd.api.types.is_bool_dtype(pdf[col]):
            ds.add_column(col, "bool")
        else:
            ds.add_column(col, "text")

    records = {}

    for col in pdf.columns:
        
        if pd.api.types.is_numeric_dtype(pdf[col]):
            records[col] = [
                float("nan") if pd.isna(v) else float(v)
                for v in pdf[col].tolist()
            ]
    
        else:
            records[col] = [
                "" if pd.isna(v) else str(v)
                for v in pdf[col].tolist()
            ]

    ds.append(records)

    ds.commit(
        json.dumps(
            {
                "version": version,
                "source_table": GOLD_MODEL_FEATURE_TABLE,
                "rows": len(pdf),
                "columns": list(pdf.columns),
            },
            ensure_ascii=False,
        )
    )

    try:
        ds.tag(version)
    except Exception:
        pass

    print(dataset_path)
    return dataset_path


if __name__ == "__main__":
    import sys

    save_deeplake_snapshot(sys.argv[1])