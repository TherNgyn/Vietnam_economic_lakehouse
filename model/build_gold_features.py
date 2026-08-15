from functools import reduce
import gc

import boto3
from botocore.client import Config
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

from spark_session import get_spark
from config import (
    START_MONTH,
    GOLD_MONTHLY_FEATURE_TABLE,
    GOLD_FACT_GDP_GROWTH_TABLE,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


DEBUG = True

MONTHLY_FEATURE_PATH = "s3a://gold/model/features/inflation_forecast_features_monthly"
MONTHLY_FEATURE_BUCKET = "gold"
MONTHLY_FEATURE_PREFIX = "model/features/inflation_forecast_features_monthly"

MODEL_KEEP_COLUMNS = [
    "date",
    "cpi_mom_processed_inflation",
    "wti",
    "gasoline_world",
    "gold",
    "USDVND",
    "policy_rate",
    "interest_rate",
    "broad_money",
    "NIKKEI225",
    "gdp",
]


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

    print(f"[MINIO] Deleted {deleted} object(s) from s3://{bucket}/{prefix}")


def month_col(c):
    return F.to_date(F.date_trunc("month", F.to_date(F.col(c))))


def get_max_cpi_month(spark):
    row = (
        spark.table("silver.cpi_mom")
        .select(F.max(month_col("date")).alias("max_cpi_month"))
        .collect()[0]
    )

    max_cpi_month = row["max_cpi_month"]

    if max_cpi_month is None:
        raise ValueError("Không tìm thấy dữ liệu trong bảng silver.cpi_mom")

    return max_cpi_month


def print_df_debug(title, df, limit=20):
    if not DEBUG:
        return

    print(f"\n{title}")
    try:
        df.show(limit, truncate=False)
    except Exception as e:
        print(f"[WARN] Không thể show DataFrame {title}: {e}")


def print_date_range(title, df):
    if not DEBUG:
        return

    print(f"\nDATE RANGE: {title}")
    try:
        (
            df.select(
                F.min("date").alias("min_date"),
                F.max("date").alias("max_date"),
                F.count("*").alias("row_count"),
            )
            .show(truncate=False)
        )
    except Exception as e:
        print(f"[WARN] Không thể in date range cho {title}: {e}")


def count_total_nulls(df):
    exprs = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]

    row = df.select(exprs).collect()[0].asDict()
    return sum(v for v in row.values() if v is not None)


def print_null_count_by_column(title, df):
    if not DEBUG:
        return

    print(f"\nNULL COUNT: {title}")

    exprs = [
        F.sum(F.col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]

    try:
        null_row = df.select(exprs).collect()[0].asDict()

        for col_name, null_count in null_row.items():
            if null_count and null_count > 0:
                print(f"{col_name:35} | {null_count} nulls")

        total_nulls = sum(v for v in null_row.values() if v is not None)
        print(f"[INFO] Total nulls: {total_nulls}")

    except Exception as e:
        print(f"[WARN] Không thể in null count: {e}")


def monthly_last_value(
    df,
    date_col,
    value_col,
    out_col,
    extra_filter=None,
    max_month=None,
    debug_name=None,
):
    x = df

    if extra_filter is not None:
        x = x.where(extra_filter)

    x = (
        x.select(
            month_col(date_col).alias("date"),
            F.to_date(F.col(date_col)).alias("raw_date"),
            F.col(value_col).cast("double").alias(out_col),
        )
        .where(F.col("date").isNotNull())
    )

    if max_month is not None:
        x = x.where(F.col("date") <= F.lit(max_month))

    w = Window.partitionBy("date").orderBy(F.col("raw_date").desc())

    result = (
        x.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("date", "raw_date", out_col)
    )

    if debug_name:
        print_df_debug(
            f"{debug_name}: ngày raw được chọn trong từng tháng",
            result.orderBy(F.col("date").desc()),
            limit=12,
        )

    return result.select("date", out_col)


def monthly_last_by_symbol(
    spark,
    table_name,
    symbols,
    out_names=None,
    max_month=None,
    debug_name=None,
):
    out_names = out_names or {s: s for s in symbols}

    src = spark.table(table_name)

    x = (
        src.where(F.col("symbol").isin(symbols))
        .select(
            month_col("date").alias("date"),
            F.to_date("date").alias("raw_date"),
            F.col("symbol"),
            F.col("close").cast("double").alias("close"),
        )
        .where(F.col("date").isNotNull())
    )

    if max_month is not None:
        x = x.where(F.col("date") <= F.lit(max_month))

    w = Window.partitionBy("date", "symbol").orderBy(F.col("raw_date").desc())

    selected = (
        x.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("date", "raw_date", "symbol", "close")
    )

    if debug_name:
        print_df_debug(
            f"{debug_name}: ngày raw được chọn theo symbol",
            selected.orderBy(F.col("date").desc(), F.col("symbol")),
            limit=36,
        )

    pivoted = (
        selected
        .groupBy("date")
        .pivot("symbol", symbols)
        .agg(F.first("close"))
    )

    for old, new in out_names.items():
        if old in pivoted.columns and old != new:
            pivoted = pivoted.withColumnRenamed(old, new)

    return pivoted


def build_gdp_monthly(spark, max_month=None, debug=True):
    """
    GDP Q1 -> dùng cho tháng 4, 5, 6
    GDP Q2 -> dùng cho tháng 7, 8, 9
    GDP Q3 -> dùng cho tháng 10, 11, 12
    GDP Q4 -> dùng cho tháng 1, 2, 3 năm sau
    """

    fact = spark.table(GOLD_FACT_GDP_GROWTH_TABLE)

    q = (
        fact
        .groupBy("time_key")
        .agg(F.sum(F.col("constant_value").cast("double")).alias("gdp"))
        .withColumn(
            "quarter_start",
            F.to_date(F.col("time_key").cast("string"), "yyyyMMdd"),
        )
        .where(F.col("quarter_start").isNotNull())
        .withColumn("available_from_month", F.add_months(F.col("quarter_start"), 3))
        .withColumn("available_to_month", F.add_months(F.col("quarter_start"), 5))
    )

    if debug and DEBUG:
        print_df_debug(
            "GDP quarterly mapping: quý GDP được dùng cho các tháng nào",
            q.select(
                "time_key",
                "quarter_start",
                "available_from_month",
                "available_to_month",
                "gdp",
            ).orderBy(F.col("quarter_start").desc()),
            limit=12,
        )

    monthly = (
        q.select(
            F.explode(
                F.sequence(
                    F.col("available_from_month"),
                    F.col("available_to_month"),
                    F.expr("interval 1 month"),
                )
            ).alias("date"),
            F.col("gdp"),
        )
    )

    if max_month is not None:
        monthly = monthly.where(F.col("date") <= F.lit(max_month))

    if debug and DEBUG:
        print_df_debug(
            "GDP monthly sau khi lag 1 quý",
            monthly.orderBy(F.col("date").desc()),
            limit=15,
        )

    return monthly.select("date", "gdp")


def fill_forward_backward(df):
    """
    Tương đương Pandas:
        df.ffill().bfill()

    Giữ nguyên logic cũ nhưng tối ưu hơn:
    - Forward fill tất cả cột trong một select.
    - Backward fill tất cả cột trong một select.
    """

    order_f = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    order_b = Window.orderBy("date").rowsBetween(0, Window.unboundedFollowing)

    value_cols = [c for c in df.columns if c != "date"]

    forward_df = df.select(
        F.col("date"),
        *[
            F.last(F.col(c), ignorenulls=True).over(order_f).alias(c)
            for c in value_cols
        ],
    )

    out = forward_df.select(
        F.col("date"),
        *[
            F.first(F.col(c), ignorenulls=True).over(order_b).alias(c)
            for c in value_cols
        ],
    )

    return out


def ensure_columns(df, columns):
    out = df

    for c in columns:
        if c not in out.columns:
            if c == "date":
                continue
            out = out.withColumn(c, F.lit(None).cast("double"))

    return out.select(*columns)


def unpersist_safely(*dfs):
    for df in dfs:
        try:
            if df is not None:
                df.unpersist(blocking=False)
        except Exception:
            pass


def build_gold_monthly_features():
    spark = get_spark()

    time_axis = None
    cpi = None
    broad = None
    policy = None
    interest_rate = None
    gdp = None
    commodities = None
    global_index = None
    currency = None
    merged = None

    try:
        max_cpi_month = get_max_cpi_month(spark)

        print("\nBUILD MONTHLY FEATURES")
        print(f"[INFO] START_MONTH: {START_MONTH}")
        print(f"[INFO] Max CPI month: {max_cpi_month}")
        print(f"[INFO] Dataset end month based on CPI: {max_cpi_month}")

        time_axis = spark.sql(f"""
            SELECT explode(
                sequence(
                    to_date('{START_MONTH}'),
                    to_date('{max_cpi_month}'),
                    interval 1 month
                )
            ) AS date
        """)

        print_date_range("time_axis", time_axis)

        cpi_raw = (
            spark.table("silver.cpi_mom")
            .select(
                month_col("date").alias("date"),
                F.to_date("date").alias("raw_date"),
                (
                    F.col("cpi_mom").cast("double") - F.lit(100.0)
                ).alias("cpi_mom_processed_inflation"),
            )
            .where(F.col("date") <= F.lit(max_cpi_month))
        )

        w_cpi = Window.partitionBy("date").orderBy(F.col("raw_date").desc())

        cpi_debug = (
            cpi_raw.withColumn("rn", F.row_number().over(w_cpi))
            .where(F.col("rn") == 1)
            .select(
                "date",
                "raw_date",
                "cpi_mom_processed_inflation",
            )
        )

        print_df_debug(
            "CPI: ngày raw được chọn trong từng tháng",
            cpi_debug.orderBy(F.col("date").desc()),
            limit=12,
        )

        cpi = cpi_debug.select(
            "date",
            "cpi_mom_processed_inflation",
        )

        broad = monthly_last_value(
            spark.table("silver.broad_money"),
            "date",
            "value",
            "broad_money",
            max_month=max_cpi_month,
            debug_name="Broad money",
        )

        policy = monthly_last_value(
            spark.table("silver.policy_rate"),
            "date",
            "value",
            "policy_rate",
            max_month=max_cpi_month,
            debug_name="Policy rate",
        )

        interest_rate = monthly_last_value(
            spark.table("silver.interest_rate"),
            "date",
            "interest_rate",
            "interest_rate",
            F.lower(F.col("term")).isin("3 months", "3 month", "3m"),
            max_month=max_cpi_month,
            debug_name="Interest rate 3M",
        )

        gdp = build_gdp_monthly(
            spark,
            max_month=max_cpi_month,
            debug=True,
        )

        commodities = monthly_last_by_symbol(
            spark,
            "silver.ohlc_commodity",
            ["wti", "gasoline", "gold"],
            {
                "wti": "wti",
                "gasoline": "gasoline_world",
                "gold": "gold",
            },
            max_month=max_cpi_month,
            debug_name="Commodities",
        )

        global_index = monthly_last_by_symbol(
            spark,
            "silver.ohlc_index",
            ["NIKKEI225"],
            {
                "NIKKEI225": "NIKKEI225",
            },
            max_month=max_cpi_month,
            debug_name="Global index",
        )

        currency = monthly_last_by_symbol(
            spark,
            "silver.ohlc_currency",
            ["USDVND"],
            {
                "USDVND": "USDVND",
            },
            max_month=max_cpi_month,
            debug_name="Currency",
        )

        dfs = [
            time_axis,
            cpi,
            commodities,
            currency,
            policy,
            interest_rate,
            broad,
            global_index,
            gdp,
        ]

        merged = reduce(
            lambda left, right: left.join(right, "date", "left"),
            dfs,
        )

        print_date_range("merged before fill", merged)

        merged = ensure_columns(
            merged,
            MODEL_KEEP_COLUMNS,
        )

        if DEBUG:
            before_fill_count = merged.count()
            missing_before = count_total_nulls(merged)

            print(f"[INFO] Rows before fill: {before_fill_count}")
            print(f"[INFO] Missing values BEFORE fill: {missing_before}")

            print_null_count_by_column(
                "before fill",
                merged,
            )

        merged = fill_forward_backward(merged)

        merged = merged.persist(StorageLevel.MEMORY_AND_DISK)
        merged.count()

        if DEBUG:
            missing_after = count_total_nulls(merged)

            print(f"[INFO] Missing values AFTER ffill + bfill: {missing_after}")

            print_null_count_by_column(
                "after ffill + bfill",
                merged,
            )

            after_fill_count = merged.count()
            print(f"[INFO] Rows after fill: {after_fill_count}")
            print(f"[INFO] Rows dropped: 0")

            print_date_range("final merged", merged)

            print_df_debug(
                "Final output sample",
                merged.orderBy(F.col("date").desc()),
                limit=20,
            )

        spark.sql(f"DROP TABLE IF EXISTS {GOLD_MONTHLY_FEATURE_TABLE}")
        delete_minio_prefix(MONTHLY_FEATURE_BUCKET, MONTHLY_FEATURE_PREFIX)

        (
            merged
            .coalesce(1)
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(MONTHLY_FEATURE_PATH)
        )

        spark.sql(f"""
        CREATE TABLE {GOLD_MONTHLY_FEATURE_TABLE}
        USING DELTA
        LOCATION '{MONTHLY_FEATURE_PATH}'
        """)

        print(f"[INFO] Created table: {GOLD_MONTHLY_FEATURE_TABLE}")
        print(f"[INFO] Feature path: {MONTHLY_FEATURE_PATH}")
        print(f"[INFO] Dataset end month based on CPI: {max_cpi_month}")

    finally:
        print("\nCLEANUP")

        try:
            unpersist_safely(
                time_axis,
                cpi,
                broad,
                policy,
                interest_rate,
                gdp,
                commodities,
                global_index,
                currency,
                merged,
            )
            print("[CLEANUP] Unpersist DataFrames done")
        except Exception as e:
            print(f"[CLEANUP][WARN] Unpersist failed: {e}")

        cleanup_names = [
            "time_axis",
            "cpi",
            "broad",
            "policy",
            "interest_rate",
            "gdp",
            "commodities",
            "global_index",
            "currency",
            "dfs",
            "merged",
        ]

        for name in cleanup_names:
            if name in locals():
                try:
                    del locals()[name]
                except Exception:
                    pass

        try:
            spark.catalog.clearCache()
            print("[CLEANUP] Spark cache cleared")
        except Exception as e:
            print(f"[CLEANUP][WARN] clearCache failed: {e}")

        try:
            spark.stop()
            print("[CLEANUP] Spark stopped")
        except Exception as e:
            print(f"[CLEANUP][WARN] spark.stop failed: {e}")

        gc.collect()
        print("[CLEANUP] Python GC collected")


if __name__ == "__main__":
    build_gold_monthly_features()