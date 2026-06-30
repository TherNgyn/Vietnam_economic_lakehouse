import os
import json
import redis
from pyspark.sql import SparkSession, functions as F


def get_spark():
    return (
        SparkSession.builder.appName("Gold-Redis-Push")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", "thrift://hive:9083")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .enableHiveSupport()
        .getOrCreate()
    )


def get_redis():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", 6379)),
        db=0,
        decode_responses=True,
    )


def push_kpi(r: redis.Redis, key: str, data: dict, ttl: int = 3600):
    r.hset(key, mapping={k: str(v) for k, v in data.items()})
    r.expire(key, ttl)


def push_series(r: redis.Redis, key: str, records: list, ttl: int = 3600):
    r.set(key, json.dumps(records), ex=ttl)


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    r = get_redis()

    # ── Currency latest rates ─────────────────────────────
    try:
        df = spark.sql("""
            SELECT ds.symbol, ds.name, f.close, f.change_percent, f.daily_return_pct, t.date_str
            FROM gold.fact_currency_rate f
            JOIN gold.dim_symbol ds ON ds.symbol_key = f.currency_key
            JOIN gold.dim_time_extended t ON t.time_key = f.time_key
            WHERE t.date_str = (SELECT MAX(date_str) FROM gold.dim_time_extended)
        """).toPandas()
        for _, row in df.iterrows():
            push_kpi(r, f"gold:currency:{row['symbol']}:latest", row.to_dict())
        push_series(r, "gold:currency:latest_all", df.to_dict(orient="records"))
    except Exception:
        pass

    # ── OHLC latest ──────────────────────────────────────
    try:
        df = spark.sql("""
            SELECT ds.symbol, ds.asset_class, f.open, f.high, f.low, f.close,
                   f.volume, f.change_percent, f.daily_return_pct, t.date_str
            FROM gold.fact_ohlc f
            JOIN gold.dim_symbol ds ON ds.symbol_key = f.symbol_key
            JOIN gold.dim_time_extended t ON t.time_key = f.time_key
            WHERE t.date_str = (SELECT MAX(date_str) FROM gold.dim_time_extended)
        """).toPandas()
        for _, row in df.iterrows():
            push_kpi(r, f"gold:ohlc:{row['symbol']}:latest", row.to_dict())
        push_series(r, "gold:ohlc:latest_all", df.to_dict(orient="records"))
    except Exception:
        pass

    # ── Interest rate latest ─────────────────────────────
    try:
        df = spark.sql("""
            SELECT dt.term_name, dt.term_symbol, f.rate_value, t.date_str
            FROM gold.fact_interest_rate f
            JOIN gold.dim_term dt ON dt.term_key = f.term_key
            JOIN gold.dim_time_extended t ON t.time_key = f.time_key
            WHERE t.date_str = (SELECT MAX(date_str) FROM gold.dim_time_extended)
        """).toPandas()
        push_series(r, "gold:interest_rate:latest", df.to_dict(orient="records"))
    except Exception:
        pass

    # ── M2 latest ────────────────────────────────────────
    try:
        row = spark.sql("""
            SELECT f.m2, f.m2_yoy_growth, f.m2_mom_growth, f.unit, t.date_str
            FROM gold.fact_broad_money f
            JOIN gold.dim_time_extended t ON t.time_key = f.time_key
            ORDER BY t.date_str DESC LIMIT 1
        """).toPandas().iloc[0]
        push_kpi(r, "gold:broad_money:latest", row.to_dict())
    except Exception:
        pass

    # ── CPI forecast latest ──────────────────────────────
    try:
        df = spark.sql("""
            SELECT f.actual_cpi, f.predicted_cpi, f.lower_bound, f.upper_bound,
                   f.model_name, f.mae, f.rmse, f.mape, t.date_str
            FROM gold.fact_cpi_forecast f
            JOIN gold.dim_time_extended t ON t.time_key = f.time_key
            ORDER BY t.date_str DESC LIMIT 24
        """).toPandas()
        push_series(r, "gold:cpi_forecast:latest", df.to_dict(orient="records"))
    except Exception:
        pass

    spark.stop()


if __name__ == "__main__":
    main()
