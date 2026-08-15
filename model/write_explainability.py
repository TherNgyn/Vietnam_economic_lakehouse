import argparse
import gc
from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F

from spark_session import get_spark
from config import (
    GOLD_MODEL_FEATURE_TABLE,
    GOLD_MODEL_REGISTRY_TABLE,
    GOLD_FEATURE_IMPORTANCE_TABLE,
)


def write_explainability(model_name: str | None = None):
    if not model_name:
        model_name = "varnn_rm"

    spark = get_spark()

    try:
        active = (
            spark.table(GOLD_MODEL_REGISTRY_TABLE)
            .where(
                (F.col("model_name") == model_name)
                & (F.col("is_active") == True)
            )
            .orderBy(F.col("created_at").desc())
            .limit(1)
            .toPandas()
        )

        if active.empty:
            raise RuntimeError(f"No active model for model_name={model_name}")

        model_id = active.iloc[0]["model_id"]
        version = active.iloc[0]["model_version"]

        df = (
            spark.table(GOLD_MODEL_FEATURE_TABLE)
            .toPandas()
            .dropna(subset=["target_next"])
        )

        num_cols = [
            c
            for c in df.columns
            if c not in ["date", "target_next"]
            and pd.api.types.is_numeric_dtype(df[c])
        ]

        rows = []
        now = datetime.utcnow()

        for c in num_cols:
            corr = df[[c, "target_next"]].corr().iloc[0, 1]

            if pd.notna(corr):
                rows.append(
                    (
                        model_id,
                        model_name,
                        version,
                        c,
                        float(abs(corr)),
                        "correlation_abs",
                        "positive" if corr >= 0 else "negative",
                        now,
                    )
                )

        rows = sorted(rows, key=lambda x: x[4], reverse=True)

        schema = """
        model_id string,
        model_name string,
        model_version string,
        feature_name string,
        importance_value double,
        importance_type string,
        direction string,
        created_at timestamp
        """

        importance_df = spark.createDataFrame(rows, schema)

        spark.sql(f"""
        DELETE FROM {GOLD_FEATURE_IMPORTANCE_TABLE}
        WHERE model_name = '{model_name}'
        """)

        importance_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(GOLD_FEATURE_IMPORTANCE_TABLE)

        print(
            f"saved {GOLD_FEATURE_IMPORTANCE_TABLE} "
            f"for model_name={model_name}, model_id={model_id}"
        )

    finally:
        cleanup_names = [
            "active",
            "df",
            "importance_df",
            "rows",
            "num_cols",
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
    parser.add_argument("--model-name", default="varnn_rm")
    args = parser.parse_args()

    write_explainability(model_name=args.model_name)