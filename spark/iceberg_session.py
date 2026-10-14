import os
import sys
from pathlib import Path

try:
    from runtime_config import configure_spark_runtime
except ImportError:
    from spark.runtime_config import configure_spark_runtime

configure_spark_runtime()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pyspark.sql import SparkSession

from scripts.minio_config import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_REGION,
    MINIO_SECRET_KEY,
)

ICEBERG_CATALOG = os.environ.get("ICEBERG_CATALOG", "lakehouse")
ICEBERG_WAREHOUSE = os.environ.get(
    "ICEBERG_WAREHOUSE",
    f"s3a://{MINIO_BUCKET}/warehouse/",
)
ICEBERG_RUNTIME_PACKAGE = (
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0"
)
HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.4.1"


def create_iceberg_spark_session(
    app_name: str = "ProductImage_Iceberg",
) -> SparkSession:
    ssl_enabled = MINIO_ENDPOINT.lower().startswith("https://")
    return (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config(
            "spark.jars.packages",
            f"{ICEBERG_RUNTIME_PACKAGE},{HADOOP_AWS_PACKAGE}",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse",
            ICEBERG_WAREHOUSE,
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.endpoint.region", MINIO_REGION)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(ssl_enabled).lower())
        .getOrCreate()
    )
