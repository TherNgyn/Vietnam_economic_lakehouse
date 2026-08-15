from __future__ import annotations

import os

import streamlit as st
from pyspark.sql import SparkSession


@st.cache_resource(show_spinner="Đang khởi tạo Spark session...")
def get_spark_session() -> SparkSession:
    """Tạo SparkSession dùng cho Streamlit dashboard."""

    spark_master_url = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
    hive_metastore_uri = os.getenv("HIVE_METASTORE_URI", "thrift://hive:9083")

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    executor_cores = os.getenv("SPARK_EXECUTOR_CORES", "4")

    spark = (
        SparkSession.builder
        .appName("Streamlit-Gold-Dashboard")
        .master(spark_master_url)

        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        .config("spark.hadoop.hive.metastore.uris", hive_metastore_uri)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.warehouse.dir", "s3a://gold/")

        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .config("spark.executor.memory", executor_memory)
        .config("spark.executor.cores", executor_cores)

        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark