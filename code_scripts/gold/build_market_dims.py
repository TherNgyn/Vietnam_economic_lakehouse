from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def get_spark():
    return (
        SparkSession.builder.appName("Gold-Market-Dims")
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


DELTA_OPTS = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
}


def ddl_new_tables(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    spark.sql("DROP TABLE IF EXISTS gold.dim_time_extended")
    spark.sql("""
        CREATE TABLE gold.dim_time_extended (
            time_key    BIGINT,
            date_str    STRING,
            year        INT,
            quarter     INT,
            month       INT,
            day         INT,
            day_of_week INT,
            week_of_year INT,
            is_weekend  BOOLEAN
        )
        USING DELTA LOCATION 's3a://gold/dim_time_extended'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.dim_currency")
    spark.sql("""
        CREATE TABLE gold.dim_currency (
            currency_key   INT,
            symbol         STRING,
            name           STRING,
            asset_class    STRING,
            base_currency  STRING,
            quote_currency STRING,
            market         STRING
        )
        USING DELTA LOCATION 's3a://gold/dim_currency'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.dim_symbol")
    spark.sql("""
        CREATE TABLE gold.dim_symbol (
            symbol_key  INT,
            symbol      STRING,
            name        STRING,
            asset_class STRING,
            exchange    STRING,
            market      STRING,
            currency    STRING
        )
        USING DELTA LOCATION 's3a://gold/dim_symbol'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.dim_term")
    spark.sql("""
        CREATE TABLE gold.dim_term (
            term_key      INT,
            term_name     STRING,
            term_symbol   STRING,
            duration_days INT
        )
        USING DELTA LOCATION 's3a://gold/dim_term'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.fact_currency_rate")
    spark.sql("""
        CREATE TABLE gold.fact_currency_rate (
            time_key        BIGINT,
            currency_key    INT,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          DOUBLE,
            change_percent  DOUBLE,
            daily_return_pct DOUBLE
        )
        USING DELTA LOCATION 's3a://gold/fact_currency_rate'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.fact_ohlc")
    spark.sql("""
        CREATE TABLE gold.fact_ohlc (
            time_key        BIGINT,
            symbol_key      INT,
            open            DOUBLE,
            high            DOUBLE,
            low             DOUBLE,
            close           DOUBLE,
            volume          DOUBLE,
            change_percent  DOUBLE,
            daily_return_pct DOUBLE,
            daily_range     DOUBLE,
            asset_class     STRING,
            source          STRING
        )
        USING DELTA LOCATION 's3a://gold/fact_ohlc'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.fact_interest_rate")
    spark.sql("""
        CREATE TABLE gold.fact_interest_rate (
            time_key    BIGINT,
            term_key    INT,
            rate_value  DOUBLE,
            volume      DOUBLE,
            source      STRING
        )
        USING DELTA LOCATION 's3a://gold/fact_interest_rate'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.fact_broad_money")
    spark.sql("""
        CREATE TABLE gold.fact_broad_money (
            time_key        BIGINT,
            m2              DOUBLE,
            unit            STRING,
            m2_yoy_growth   DOUBLE,
            m2_mom_growth   DOUBLE,
            source          STRING
        )
        USING DELTA LOCATION 's3a://gold/fact_broad_money'
    """)

    spark.sql("DROP TABLE IF EXISTS gold.fact_cpi_forecast")
    spark.sql("""
        CREATE TABLE gold.fact_cpi_forecast (
            time_key        BIGINT,
            actual_cpi      DOUBLE,
            predicted_cpi   DOUBLE,
            lower_bound     DOUBLE,
            upper_bound     DOUBLE,
            model_name      STRING,
            mae             DOUBLE,
            rmse            DOUBLE,
            mape            DOUBLE,
            horizon_months  INT,
            trained_at      STRING
        )
        USING DELTA LOCATION 's3a://gold/fact_cpi_forecast'
    """)


def time_key_expr():
    return F.concat(
        F.year("date").cast("string"),
        F.lpad(F.quarter("date").cast("string"), 2, "0"),
        F.lpad(F.month("date").cast("string"), 2, "0"),
        F.lpad(F.dayofmonth("date").cast("string"), 2, "0"),
    ).cast("long")


def build_dim_time_extended(spark):
    sources = []
    for tbl, col in [
        ("silver.ohlc_world", "date"),
        ("silver.ohlc_vietnam", "date"),
        ("silver.interest_rate", "date"),
        ("silver.broad_money", "date"),
    ]:
        try:
            sources.append(spark.table(tbl).select(F.col(col).alias("date_str")))
        except Exception:
            pass
    if not sources:
        return

    from functools import reduce
    all_dates = reduce(lambda a, b: a.union(b), sources).distinct().filter(F.col("date_str").isNotNull())
    df = all_dates.withColumn("date", F.to_date("date_str"))

    df = (
        df.withColumn("time_key", time_key_expr())
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("week_of_year", F.weekofyear("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
        .drop("date")
    )
    df.write.format("delta").mode("overwrite").save("s3a://gold/dim_time_extended")
    spark.sql("MSCK REPAIR TABLE gold.dim_time_extended")


def build_dim_currency(spark):
    sources = []
    for tbl in ["silver.ohlc_world", "silver.ohlc_vietnam"]:
        try:
            s = spark.table(tbl).filter(F.col("asset_class") == "currency")
            if "base_currency" not in s.columns:
                s = s.withColumn("base_currency", F.lit("USD")).withColumn("quote_currency", F.col("symbol")).withColumn("market", F.lit("FX"))
            sources.append(s.select("symbol", "name", "asset_class", "base_currency", "quote_currency", "market"))
        except Exception:
            pass
    if not sources:
        return

    from functools import reduce
    df = reduce(lambda a, b: a.union(b), sources).distinct()
    w = Window.orderBy("symbol")
    df = df.withColumn("currency_key", F.row_number().over(w))
    df.write.format("delta").mode("overwrite").save("s3a://gold/dim_currency")
    spark.sql("MSCK REPAIR TABLE gold.dim_currency")


def build_dim_symbol(spark):
    sources = []
    for tbl, ac, ex, mkt, cur in [
        ("silver.ohlc_world", None, None, None, None),
        ("silver.ohlc_vietnam", "vn_index", "HOSE/HNX", "VN", "VND"),
    ]:
        try:
            s = spark.table(tbl)
            for col_name, val in [("asset_class", ac), ("exchange", ex), ("market", mkt), ("currency", cur)]:
                if col_name not in s.columns:
                    s = s.withColumn(col_name, F.lit(val))
            sources.append(s.select("symbol", "name", "asset_class", "exchange", "market", "currency"))
        except Exception:
            pass
    if not sources:
        return

    from functools import reduce
    df = reduce(lambda a, b: a.union(b), sources).distinct()
    w = Window.orderBy("asset_class", "symbol")
    df = df.withColumn("symbol_key", F.row_number().over(w))
    df.write.format("delta").mode("overwrite").save("s3a://gold/dim_symbol")
    spark.sql("MSCK REPAIR TABLE gold.dim_symbol")


def build_dim_term(spark):
    terms = [
        ("Overnight", "ON",  1),
        ("1 Week",    "1W",  7),
        ("2 Weeks",   "2W",  14),
        ("1 Month",   "1M",  30),
        ("2 Months",  "2M",  60),
        ("3 Months",  "3M",  90),
        ("6 Months",  "6M",  180),
        ("9 Months",  "9M",  270),
        ("12 Months", "12M", 365),
    ]
    df = spark.createDataFrame(terms, ["term_name", "term_symbol", "duration_days"])
    w = Window.orderBy("duration_days")
    df = df.withColumn("term_key", F.row_number().over(w))
    df.write.format("delta").mode("overwrite").save("s3a://gold/dim_term")
    spark.sql("MSCK REPAIR TABLE gold.dim_term")


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    ddl_new_tables(spark)
    build_dim_time_extended(spark)
    build_dim_currency(spark)
    build_dim_symbol(spark)
    build_dim_term(spark)
    spark.stop()


if __name__ == "__main__":
    main()
