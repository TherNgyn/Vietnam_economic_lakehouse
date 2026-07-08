from pyspark.sql import SparkSession

from config import (
    APP_NAME,
    SPARK_MASTER_URL,
    HIVE_METASTORE_URI,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
)


def get_spark() -> SparkSession:
    packages = ",".join(
        [
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        ]
    )

    spark = (
        SparkSession.builder
        .appName(APP_NAME)
        .master(SPARK_MASTER_URL)
        .config("spark.jars.packages", packages)
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.pyspark.python", "/usr/bin/python3")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/bin/python3")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", HIVE_METASTORE_URI)
        .config("hive.metastore.uris", HIVE_METASTORE_URI)
        .config("spark.sql.warehouse.dir", "s3a://gold/model/warehouse")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .enableHiveSupport()
        .getOrCreate()
    )

    return spark