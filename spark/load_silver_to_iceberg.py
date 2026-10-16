import json
import os
from pathlib import Path

try:
    from iceberg_session import ICEBERG_CATALOG, create_iceberg_spark_session
except ImportError:
    from spark.iceberg_session import ICEBERG_CATALOG, create_iceberg_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(
    os.environ.get("LAKEHOUSE_DATA_DIR", PROJECT_ROOT / "data" / "lakehouse_real")
).resolve()
SILVER_PATH = Path(
    os.environ.get(
        "SILVER_PARQUET_PATH",
        DATA_DIR / "silver" / "product_images",
    )
).resolve()
REPORT_PATH = DATA_DIR / "etl_report.json"
EXPECTED_RECORDS = 44_441
ICEBERG_TABLE = f"{ICEBERG_CATALOG}.silver.product_images"
REQUIRED_COLUMNS = {
    "image_id",
    "product_id",
    "product_name",
    "category",
    "image_path",
    "image_size",
    "format",
    "width",
    "height",
    "created_at",
}


def _verify_etl_report() -> None:
    if not REPORT_PATH.is_file():
        raise FileNotFoundError(f"Silver ETL report not found: {REPORT_PATH}")

    with REPORT_PATH.open("r", encoding="utf-8") as report_file:
        report = json.load(report_file)

    if report.get("silver_records") != EXPECTED_RECORDS:
        raise ValueError(
            f"ETL report says Silver has {report.get('silver_records')} records; "
            f"expected {EXPECTED_RECORDS}."
        )


def load_silver_to_iceberg() -> int:
    if not SILVER_PATH.is_dir():
        raise FileNotFoundError(f"Silver Parquet path not found: {SILVER_PATH}")
    _verify_etl_report()

    spark = create_iceberg_spark_session("ProductImage_SilverToIceberg")
    try:
        silver = spark.read.parquet(str(SILVER_PATH))
        missing_columns = REQUIRED_COLUMNS.difference(silver.columns)
        if missing_columns:
            raise ValueError(
                "Silver Parquet is missing required column(s): "
                + ", ".join(sorted(missing_columns))
            )

        source_count = silver.count()
        if source_count != EXPECTED_RECORDS:
            raise ValueError(
                f"Silver Parquet contains {source_count} records; "
                f"expected {EXPECTED_RECORDS}. Iceberg table was not changed."
            )

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.silver")
        silver.writeTo(ICEBERG_TABLE).createOrReplace()

        iceberg_count = spark.table(ICEBERG_TABLE).count()
        if iceberg_count != EXPECTED_RECORDS:
            raise RuntimeError(
                f"Iceberg table {ICEBERG_TABLE} contains {iceberg_count} records "
                f"after write; expected {EXPECTED_RECORDS}."
            )

        warehouse = spark.conf.get(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse"
        )
        print(f"Silver source: {SILVER_PATH}")
        print(f"Iceberg table: {ICEBERG_TABLE}")
        print(f"Iceberg warehouse: {warehouse}")
        print(f"Silver rows: {source_count}")
        print(f"Iceberg rows after read-back: {iceberg_count}")
        return iceberg_count
    finally:
        spark.stop()


if __name__ == "__main__":
    load_silver_to_iceberg()
