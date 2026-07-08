from functools import reduce
import gc

import boto3
from botocore.client import Config
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session import get_spark
from config import (
    START_MONTH,
    END_MONTH,
    GOLD_MONTHLY_FEATURE_TABLE,
    GOLD_FACT_GDP_GROWTH_TABLE,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


MONTHLY_FEATURE_PATH = "s3a://gold/model/features/inflation_forecast_features_monthly"
MONTHLY_FEATURE_BUCKET = "gold"
MONTHLY_FEATURE_PREFIX = "model/features/inflation_forecast_features_monthly"


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


def month_col(c):
    return F.to_date(F.date_trunc("month", F.to_date(F.col(c))))


def monthly_last_value(df, date_col, value_col, out_col, extra_filter=None):
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

    w = Window.partitionBy("date").orderBy(F.col("raw_date").desc())

    return (
        x.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("date", out_col)
    )


def monthly_last_by_symbol(table_name, symbols, out_names=None):
    spark = get_spark()
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

    w = Window.partitionBy("date", "symbol").orderBy(F.col("raw_date").desc())

    x = (
        x.withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .groupBy("date")
        .pivot("symbol", symbols)
        .agg(F.first("close"))
    )

    for old, new in out_names.items():
        if old in x.columns and old != new:
            x = x.withColumnRenamed(old, new)

    return x


def build_gdp_monthly():
    spark = get_spark()

    fact = spark.table(GOLD_FACT_GDP_GROWTH_TABLE)

    q = (
        fact
        .groupBy("time_key")
        .agg(F.sum(F.col("market_value").cast("double")).alias("gdp"))
        .withColumn(
            "quarter_start",
            F.to_date(F.col("time_key").cast("string"), "yyyyMMdd"),
        )
        .where(F.col("quarter_start").isNotNull())
        .withColumn("quarter_end", F.add_months(F.col("quarter_start"), 2))
    )

    monthly = (
        q
        .select(
            F.explode(
                F.sequence(
                    F.col("quarter_start"),
                    F.col("quarter_end"),
                    F.expr("interval 1 month"),
                )
            ).alias("date"),
            F.col("gdp"),
        )
    )

    return monthly


def fill_forward_backward(df):
    order_f = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, 0)
    order_b = Window.orderBy("date").rowsBetween(0, Window.unboundedFollowing)

    out = df

    for c in [x for x in df.columns if x != "date"]:
        out = out.withColumn(c, F.last(F.col(c), ignorenulls=True).over(order_f))
        out = out.withColumn(c, F.first(F.col(c), ignorenulls=True).over(order_b))

    return out


def build_gold_monthly_features():
    spark = get_spark()

    try:
        time_axis = spark.sql(f"""
            SELECT explode(
                sequence(
                    to_date('{START_MONTH}'),
                    to_date('{END_MONTH}'),
                    interval 1 month
                )
            ) AS date
        """)

        cpi = (
            spark.table("silver.cpi_mom")
            .select(
                month_col("date").alias("date"),
                F.to_date("date").alias("raw_date"),
                F.col("cpi_mom").cast("double").alias("cpi_mom_processed_cpi"),
                (
                    F.col("cpi_mom").cast("double") - F.lit(100.0)
                ).alias("cpi_mom_processed_inflation"),
            )
        )

        w_cpi = Window.partitionBy("date").orderBy(F.col("raw_date").desc())

        cpi = (
            cpi.withColumn("rn", F.row_number().over(w_cpi))
            .where(F.col("rn") == 1)
            .select(
                "date",
                "cpi_mom_processed_cpi",
                "cpi_mom_processed_inflation",
            )
        )

        core = monthly_last_value(
            spark.table("silver.core_inflation_rate"),
            "date",
            "core_inflation_rate",
            "core_inflation_rate",
        )

        ppi = monthly_last_value(
            spark.table("silver.ppi_qoq"),
            "date",
            "ppi_qoq",
            "ppi_qoq",
        )

        m2 = monthly_last_value(
            spark.table("silver.m2"),
            "date",
            "m2",
            "m2",
        )

        broad = monthly_last_value(
            spark.table("silver.broad_money"),
            "date",
            "value",
            "broad_money",
        )

        policy = monthly_last_value(
            spark.table("silver.policy_rate"),
            "date",
            "value",
            "policy_rate",
        )

        interest_rate = monthly_last_value(
            spark.table("silver.interest_rate"),
            "date",
            "interest_rate",
            "interest_rate",
            F.lower(F.col("term")).isin("3 months", "3 month", "3m"),
        )

        gdp = build_gdp_monthly()

        commodities = monthly_last_by_symbol(
            "silver.ohlc_commodity",
            ["brent", "wti", "gasoline", "natural_gas", "gold", "silver"],
            {
                "brent": "brent",
                "wti": "wti",
                "gasoline": "gasoline_world",
                "natural_gas": "natural_gas",
                "gold": "gold",
                "silver": "silver",
            },
        )

        vn_index = monthly_last_by_symbol(
            "silver.ohlc_vietnam_index",
            ["VNINDEX", "VN30", "HNX", "UPCOM"],
            {
                "VNINDEX": "VNINDEX",
                "VN30": "VN30",
                "HNX": "HNX",
                "UPCOM": "UPCOM",
            },
        )

        global_index = monthly_last_by_symbol(
            "silver.ohlc_index",
            ["NASDAQ", "S&P500", "DAX", "DOWJONES", "NIKKEI225", "HANGSENG"],
            {
                "NASDAQ": "NASDAQ",
                "S&P500": "S&P500",
                "DAX": "DAX",
                "DOWJONES": "DOWJONES",
                "NIKKEI225": "NIKKEI225",
                "HANGSENG": "HANGSENG",
            },
        )

        currency = monthly_last_by_symbol(
            "silver.ohlc_currency",
            ["USDVND"],
            {"USDVND": "USDVND"},
        )

        dfs = [
            time_axis,
            cpi,
            core,
            interest_rate,
            ppi,
            m2,
            broad,
            policy,
            gdp,
            commodities,
            vn_index,
            global_index,
            currency,
        ]

        merged = reduce(lambda left, right: left.join(right, "date", "left"), dfs)

        merged = (
            fill_forward_backward(merged)
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("quarter", F.quarter("date"))
        )

        spark.sql(f"DROP TABLE IF EXISTS {GOLD_MONTHLY_FEATURE_TABLE}")
        delete_minio_prefix(MONTHLY_FEATURE_BUCKET, MONTHLY_FEATURE_PREFIX)

        (
            merged.write
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

        print(GOLD_MONTHLY_FEATURE_TABLE)

    finally:
        cleanup_names = [
            "time_axis",
            "cpi",
            "core",
            "ppi",
            "m2",
            "broad",
            "policy",
            "interest_rate",
            "gdp",
            "commodities",
            "vn_index",
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
        except Exception:
            pass

        try:
            spark.stop()
        except Exception:
            pass

        gc.collect()


if __name__ == "__main__":
    build_gold_monthly_features()