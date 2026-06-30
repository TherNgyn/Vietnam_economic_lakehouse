from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


def get_spark():
    return (
        SparkSession.builder.appName("Gold-Market-Facts")
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


def build_fact_currency_rate(spark):
    try:
        ohlc = spark.table("silver.ohlc_world").filter(F.col("asset_class") == "currency")
        dim_t = spark.table("gold.dim_time_extended").select("time_key", "date_str")
        dim_c = spark.table("gold.dim_currency").select("currency_key", "symbol")

        fact = (
            ohlc
            .join(dim_t, ohlc["date"] == dim_t["date_str"], "inner")
            .join(dim_c, "symbol", "inner")
            .select(
                "time_key",
                "currency_key",
                F.col("open").cast("double"),
                F.col("high").cast("double"),
                F.col("low").cast("double"),
                F.col("close").cast("double"),
                F.col("volume").cast("double"),
                F.col("change_percent").cast("double"),
                F.round(
                    (F.col("close") - F.col("open")) / F.nullif(F.col("open"), F.lit(0)) * 100, 4
                ).alias("daily_return_pct"),
            )
        )
        fact.write.format("delta").mode("overwrite").save("s3a://gold/fact_currency_rate")
        spark.sql("MSCK REPAIR TABLE gold.fact_currency_rate")
    except Exception as e:
        raise RuntimeError(f"fact_currency_rate failed: {e}") from e


def build_fact_ohlc(spark):
    try:
        world = spark.table("silver.ohlc_world")
        if "source" not in world.columns:
            world = world.withColumn("source", F.lit("yfinance"))

        vn = spark.table("silver.ohlc_vietnam")
        vn = vn.withColumn("asset_class", F.lit("vn_index")).withColumn("source", F.lit("HOSE/HNX"))

        combined = world.select(
            "date", "symbol", "open", "high", "low", "close", "volume", "change_percent", "asset_class", "source"
        ).union(
            vn.select(
                "date", "symbol", "open", "high", "low", "close", "volume", "change_percent", "asset_class", "source"
            )
        )

        dim_t = spark.table("gold.dim_time_extended").select("time_key", "date_str")
        dim_s = spark.table("gold.dim_symbol").select("symbol_key", "symbol")

        fact = (
            combined
            .join(dim_t, combined["date"] == dim_t["date_str"], "inner")
            .join(dim_s, "symbol", "inner")
            .select(
                "time_key",
                "symbol_key",
                F.col("open").cast("double"),
                F.col("high").cast("double"),
                F.col("low").cast("double"),
                F.col("close").cast("double"),
                F.col("volume").cast("double"),
                F.col("change_percent").cast("double"),
                F.round(
                    (F.col("close") - F.col("open")) / F.nullif(F.col("open"), F.lit(0)) * 100, 4
                ).alias("daily_return_pct"),
                F.round(F.col("high") - F.col("low"), 4).alias("daily_range"),
                "asset_class",
                "source",
            )
        )
        fact.write.format("delta").mode("overwrite").save("s3a://gold/fact_ohlc")
        spark.sql("MSCK REPAIR TABLE gold.fact_ohlc")
    except Exception as e:
        raise RuntimeError(f"fact_ohlc failed: {e}") from e


def build_fact_interest_rate(spark):
    try:
        ir = spark.table("silver.interest_rate")
        dim_t = spark.table("gold.dim_time_extended").select("time_key", "date_str")
        dim_term = spark.table("gold.dim_term").select("term_key", "term_symbol")

        fact = (
            ir
            .join(dim_t, ir["date"] == dim_t["date_str"], "inner")
            .join(dim_term, ir["symbol"] == dim_term["term_symbol"], "inner")
            .select(
                "time_key",
                "term_key",
                F.col("interest_rate").cast("double").alias("rate_value"),
                F.col("volume").cast("double"),
                F.col("source"),
            )
        )
        fact.write.format("delta").mode("overwrite").save("s3a://gold/fact_interest_rate")
        spark.sql("MSCK REPAIR TABLE gold.fact_interest_rate")
    except Exception as e:
        raise RuntimeError(f"fact_interest_rate failed: {e}") from e


def build_fact_broad_money(spark):
    try:
        bm = spark.table("silver.broad_money")
        dim_t = spark.table("gold.dim_time_extended").select("time_key", "date_str")

        fact = (
            bm
            .join(dim_t, bm["date"] == dim_t["date_str"], "inner")
            .select(
                "time_key",
                F.col("m2").cast("double"),
                F.col("unit"),
                F.col("m2_yoy_growth").cast("double"),
                F.col("m2_mom_growth").cast("double"),
                F.col("source"),
            )
        )
        fact.write.format("delta").mode("overwrite").save("s3a://gold/fact_broad_money")
        spark.sql("MSCK REPAIR TABLE gold.fact_broad_money")
    except Exception as e:
        raise RuntimeError(f"fact_broad_money failed: {e}") from e


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    build_fact_currency_rate(spark)
    build_fact_ohlc(spark)
    build_fact_interest_rate(spark)
    build_fact_broad_money(spark)
    spark.stop()


if __name__ == "__main__":
    main()
