import json
import os
import sys
from pathlib import Path
from typing import Any

try:
    from runtime_config import configure_spark_runtime
except ImportError:
    from spark.runtime_config import configure_spark_runtime

configure_spark_runtime()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, TimestampType


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("LAKEHOUSE_DATA_DIR", PROJECT_ROOT / "data")).resolve()
SILVER_PATH = DATA_DIR / "silver" / "product_images"
QUARANTINE_PATH = DATA_DIR / "quarantine" / "product_images"
REPORT_PATH = DATA_DIR / "etl_report.json"

EXPECTED_TYPES = {
    "image_id": StringType,
    "product_id": StringType,
    "product_name": StringType,
    "category": StringType,
    "image_path": StringType,
    "image_size": LongType,
    "format": StringType,
    "width": IntegerType,
    "height": IntegerType,
    "created_at": TimestampType,
}
REQUIRED_NON_NULL = (
    "image_id",
    "product_id",
    "category",
    "image_path",
    "image_size",
    "format",
    "width",
    "height",
    "created_at",
)


def _read_report(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"ETL report not found: {path}. Run the ETL first.")
    with path.open("r", encoding="utf-8") as report_file:
        report = json.load(report_file)
    if not isinstance(report, dict):
        raise ValueError(f"ETL report must contain a JSON object: {path}")
    return report


def validate_output() -> None:
    if not SILVER_PATH.is_dir():
        raise FileNotFoundError(
            f"Silver Parquet folder not found: {SILVER_PATH}. Run the ETL first."
        )
    if not QUARANTINE_PATH.is_dir():
        raise FileNotFoundError(
            f"Quarantine Parquet folder not found: {QUARANTINE_PATH}. Run the ETL first."
        )

    report = _read_report(REPORT_PATH)
    spark = (
        SparkSession.builder.appName("ProductImage_ValidateSilver")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    try:
        silver = spark.read.parquet(str(SILVER_PATH))
        quarantined = spark.read.parquet(str(QUARANTINE_PATH))
        failures: list[str] = []

        for column, expected_type in EXPECTED_TYPES.items():
            if column not in silver.columns:
                failures.append(f"Missing Silver column: {column}")
            elif not isinstance(silver.schema[column].dataType, expected_type):
                failures.append(
                    f"{column} has type {silver.schema[column].dataType.simpleString()}, "
                    f"expected {expected_type.__name__}"
                )

        silver_count = silver.count()
        quarantine_count = quarantined.count()
        if silver_count != report.get("silver_records"):
            failures.append("Silver row count does not match the ETL report")
        if quarantine_count != report.get("quarantined_records"):
            failures.append("Quarantine row count does not match the ETL report")

        if all(column in silver.columns for column in REQUIRED_NON_NULL):
            null_counts = silver.select(
                *[
                    F.sum(F.when(F.col(column).isNull(), 1).otherwise(0)).alias(column)
                    for column in REQUIRED_NON_NULL
                ]
            ).first()
            for column in REQUIRED_NON_NULL:
                if null_counts[column]:
                    failures.append(f"{column} contains {null_counts[column]} NULL value(s)")

            invalid_values = silver.filter(
                (F.col("image_size") <= 0)
                | (F.col("width") <= 0)
                | (F.col("height") <= 0)
                | ~F.col("format").isin("JPG", "PNG", "WEBP")
            ).count()
            if invalid_values:
                failures.append(
                    f"Silver contains {invalid_values} invalid size, dimensions, or formats"
                )

        duplicate_count = (
            silver.groupBy("image_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if duplicate_count:
            failures.append(f"Silver contains {duplicate_count} duplicate image_id value(s)")

        if report.get("input_records") != silver_count + quarantine_count:
            failures.append("Input count does not equal Silver plus quarantine counts")
        if report.get("silver_records") != (
            report.get("matched_product_records", 0)
            + report.get("unmatched_product_records", 0)
        ):
            failures.append("Matched and unmatched JOIN counts do not equal Silver count")

        print(f"Silver records: {silver_count}")
        print(f"Quarantined records: {quarantine_count}")
        print(
            "Product JOIN: "
            f"{report.get('matched_product_records', 0)} matched, "
            f"{report.get('unmatched_product_records', 0)} unmatched"
        )
        silver.printSchema()
        silver.show(10, truncate=False)

        if failures:
            raise ValueError("Output validation failed:\n- " + "\n- ".join(failures))
        print("Output validation passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    validate_output()
