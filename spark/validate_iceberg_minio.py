import uuid

try:
    from iceberg_session import (
        ICEBERG_CATALOG,
        create_iceberg_spark_session,
    )
except ImportError:
    from spark.iceberg_session import (
        ICEBERG_CATALOG,
        create_iceberg_spark_session,
    )

try:
    from scripts.minio_config import MINIO_BUCKET, create_s3_client, ensure_bucket
except ImportError:
    from minio_config import MINIO_BUCKET, create_s3_client, ensure_bucket


def validate_iceberg_minio() -> None:
    ensure_bucket(create_s3_client(), MINIO_BUCKET)
    spark = create_iceberg_spark_session("ProductImage_IcebergMinIO_Check")
    namespace = f"_connection_check_{uuid.uuid4().hex[:12]}"
    table = f"{ICEBERG_CATALOG}.{namespace}.probe"
    table_created = False
    namespace_created = False

    try:
        spark.sql(f"CREATE NAMESPACE {ICEBERG_CATALOG}.`{namespace}`")
        namespace_created = True
        spark.sql(
            f"""
            CREATE TABLE {table} (
                id INT,
                label STRING
            )
            USING iceberg
            """
        )
        table_created = True
        spark.createDataFrame([(1, "minio-iceberg-ok")], ["id", "label"]).writeTo(
            table
        ).append()

        rows = spark.table(table).collect()
        if len(rows) != 1 or rows[0]["id"] != 1 or rows[0]["label"] != "minio-iceberg-ok":
            raise AssertionError(f"Unexpected Iceberg table contents: {rows}")

        print(f"Iceberg catalog '{ICEBERG_CATALOG}' is connected.")
        warehouse = spark.conf.get(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse"
        )
        print(f"Iceberg warehouse: {warehouse}")
        print("S3A write/read to the MinIO warehouse passed.")
    finally:
        if table_created:
            spark.sql(f"DROP TABLE {table} PURGE")
        if namespace_created:
            spark.sql(f"DROP NAMESPACE {ICEBERG_CATALOG}.`{namespace}`")
        spark.stop()


if __name__ == "__main__":
    validate_iceberg_minio()
