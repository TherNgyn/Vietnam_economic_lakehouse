from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

import boto3
from botocore.client import Config


builder = SparkSession.builder \
    .appName("Delta-MinIO") \
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    ) \
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    ) \
    .config(
        "spark.sql.catalogImplementation",
        "hive"
    ) \
    .config(
        "hive.metastore.uris",
        "thrift://hive:9083"
    ) \
    .config(
        "spark.sql.warehouse.dir",
        "/tmp/spark-warehouse"
    ) \
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "http://minio:9000"
    ) \
    .config(
        "spark.hadoop.fs.s3a.access.key",
        "minioadmin"
    ) \
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        "minioadmin"
    ) \
    .config(
        "spark.hadoop.fs.s3a.path.style.access",
        "true"
    ) \
    .config(
        "spark.hadoop.fs.s3a.connection.ssl.enabled",
        "false"
    ) \
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    ) \
    .enableHiveSupport()

spark = builder.getOrCreate()




s3_client = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

def delete_minio_prefix(bucket: str, prefix: str):
    """
    Xoá toàn bộ object trong MinIO dưới một prefix cụ thể.
 
    Cần thiết vì sau khi VACUUM và DROP TABLE (external table), Delta vẫn
    còn giữ lại _delta_log/ và các file Parquet active hiện tại trên MinIO.
    Boto3 xoá vật lý hoàn toàn, đảm bảo CREATE TABLE tiếp theo tạo bảng
    thực sự trắng tinh.
 
    Args:
        bucket: tên bucket MinIO (ví dụ: 'silver')
        prefix: prefix thư mục cần xoá (ví dụ: 'gdp/')
    """
    prefix = prefix.rstrip("/") + "/"  # đảm bảo có trailing slash
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
 
    deleted_count = 0
    for page in pages:
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_payload = {"Objects": [{"Key": obj["Key"]} for obj in objects]}
        s3_client.delete_objects(Bucket=bucket, Delete=delete_payload)
        deleted_count += len(objects)
 
    print(f"  → Đã xoá {deleted_count} object(s) tại s3://{bucket}/{prefix}")
 



spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql('DROP TABLE IF EXISTS silver.gdp;')

delete_minio_prefix('silver', 'gdp')

spark.sql("""   
CREATE TABLE silver.gdp (
    sector STRING,
    sub_sector STRING,
    year INT,
    quarter INT,
    value DOUBLE,
    type STRING,
    unit STRING,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/gdp'
""")
spark.sql('DROP TABLE IF EXISTS silver.investment;')
delete_minio_prefix('silver', 'investment')

spark.sql("""
CREATE TABLE silver.investment (
    investment_name STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/investment'
""")

spark.sql('DROP TABLE IF EXISTS silver.international_ecommerce;')
delete_minio_prefix('silver', 'international_ecommerce')

spark.sql("""
CREATE TABLE silver.international_ecommerce (
    product_name STRING,
    type STRING,
    value DOUBLE,
    unit STRING,
    quantity DOUBLE,
    quantity_unit STRING,
    month INT,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/international_ecommerce'
""")

spark.sql('DROP TABLE IF EXISTS silver.forestry;')
delete_minio_prefix('silver', 'forestry')

spark.sql("""
CREATE TABLE silver.forestry (
    forestry_indicator STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/forestry'
""")

spark.sql('DROP TABLE IF EXISTS silver.livestock;')
delete_minio_prefix('silver', 'livestock')

spark.sql("""
CREATE TABLE silver.livestock (
    livestock_indicator STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/livestock'
""")

spark.sql('DROP TABLE IF EXISTS silver.aquatic_products;')
delete_minio_prefix('silver', 'aquatic_products')

spark.sql("""
CREATE TABLE silver.aquatic_products (
    aquatic_type STRING,
    product_name STRING,
    value DOUBLE,
    unit STRING,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/aquatic_products'
""")


spark.sql('DROP TABLE IF EXISTS silver.industry_product;')
delete_minio_prefix('silver', 'industry_product')

spark.sql("""
CREATE TABLE silver.industry_product (
    product_name STRING,
    value DOUBLE,
    unit STRING,
    month INT,
    quarter INT,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/industry_product'
""")


spark.sql('DROP TABLE IF EXISTS silver.investment_by_sector;')
delete_minio_prefix('silver', 'investment_by_sector')

spark.sql("""
CREATE TABLE silver.investment_by_sector (
    name STRING,
    value DOUBLE,
    unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/investment_by_sector'
""")

spark.sql('DROP TABLE IF EXISTS silver.annual_crops;')
delete_minio_prefix('silver', 'annual_crops')

spark.sql("""
CREATE TABLE silver.annual_crops (
    crop_name STRING,
    production DOUBLE,
    production_unit STRING,
    area DOUBLE,
    area_unit STRING,
    yield DOUBLE,
    yield_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/annual_crops'
""")
spark.sql('DROP TABLE IF EXISTS silver.staple_crops;')
delete_minio_prefix('silver', 'staple_crops')

spark.sql("""
CREATE TABLE silver.staple_crops (
    crop_name STRING,
    production DOUBLE,
    production_unit STRING,
    yield DOUBLE,
    yield_unit STRING,
    area DOUBLE,
    area_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/staple_crops'
""")
spark.sql('DROP TABLE IF EXISTS silver.perennial_crops;')
delete_minio_prefix('silver', 'perennial_crops')

spark.sql("""
CREATE TABLE silver.perennial_crops (
    crop_name STRING,
    production DOUBLE,
    production_unit STRING,
    yield DOUBLE,
    yield_unit STRING,
    area DOUBLE,
    area_unit STRING,
    year INT,
    ingest_at TIMESTAMP
)
USING DELTA
LOCATION 's3a://silver/perennial_crops'
""")

spark.sql('DROP TABLE IF EXISTS silver.m2;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.m2 (
        date STRING,
        m2 DOUBLE,
        unit STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/m2'
""")

spark.sql('DROP TABLE IF EXISTS silver.core_inflation_rate;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.core_inflation_rate (
        date STRING,
        core_inflation_rate DOUBLE,
        unit STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/core_inflation_rate'
""")

spark.sql('DROP TABLE IF EXISTS silver.cpi_mom;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.cpi_mom (
        date STRING,
        cpi_mom DOUBLE,
        inflation DOUBLE,
        unit_cpi STRING,
        unit_inflation STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/cpi_mom'
""")

spark.sql('DROP TABLE IF EXISTS silver.cpi_base_year;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.cpi_base_year (
        date STRING,
        cpi_base_year STRING,
        prev_year_base DOUBLE,
        base_2000 DOUBLE,
        base_2005 DOUBLE,
        base_2010 DOUBLE,
        unit_cpi STRING,
        unit_inflation STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/cpi_base_year'
""")

spark.sql('DROP TABLE IF EXISTS silver.ppi_qoq;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ppi_qoq (
        date STRING,
        ppi_qoq DOUBLE,
        unit STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/ppi_qoq'
""")

spark.sql('DROP TABLE IF EXISTS silver.broad_money;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.broad_money (
        date STRING,
        indicator STRING,
        value DOUBLE,
        unit STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/broad_money'
""")

spark.sql('DROP TABLE IF EXISTS silver.gasoline;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.gasoline (
        date STRING,
        type STRING,
        product STRING,
        price DOUBLE,
        change STRING,
        unit STRING,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/product/gasoline'
""")

spark.sql('DROP TABLE IF EXISTS silver.interest_rate;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.interest_rate (
        date STRING,
        term STRING,
        symbol STRING,
        interest_rate DOUBLE,
        volume DOUBLE,
        source STRING,
        processing_date STRING
    )
    USING DELTA
    PARTITIONED BY (processing_date)
    LOCATION 's3a://silver/interest_rate'
""")

spark.sql('DROP TABLE IF EXISTS silver.ohlc_currency;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ohlc_currency (
        date STRING,
        symbol STRING,
        asset_class STRING,
        unit STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        change_percent DOUBLE,
        prev_close DOUBLE,
        change DOUBLE,
        source STRING
    )
    USING DELTA
    PARTITIONED BY (symbol)
    LOCATION 's3a://silver/currency'
""")

spark.sql('DROP TABLE IF EXISTS silver.ohlc_index;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ohlc_index (
        date STRING,
        symbol STRING,
        asset_class STRING,
        unit STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        change_percent DOUBLE,
        prev_close DOUBLE,
        change DOUBLE,
        source STRING
    )
    USING DELTA
    PARTITIONED BY (symbol)
    LOCATION 's3a://silver/index'
""")

spark.sql('DROP TABLE IF EXISTS silver.ohlc_commodity;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ohlc_commodity (
        date STRING,
        symbol STRING,
        asset_class STRING,
        unit STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        change_percent DOUBLE,
        prev_close DOUBLE,
        change DOUBLE,
        source STRING
    )
    USING DELTA
    PARTITIONED BY (symbol)
    LOCATION 's3a://silver/commodity'
""")

spark.sql('DROP TABLE IF EXISTS silver.ohlc_vietnam_index;')
spark.sql("""
    CREATE TABLE IF NOT EXISTS silver.ohlc_vietnam_index (
        date STRING,
        symbol STRING,
        asset_class STRING,
        unit STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        change_percent DOUBLE,
        change DOUBLE,
        source STRING
    )
    USING DELTA
    PARTITIONED BY (symbol)
    LOCATION 's3a://silver/vietnam_index'
""")