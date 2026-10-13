import csv
import json
import os
import shutil
import sys
from pathlib import Path
from typing import Any

try:
    from runtime_config import configure_spark_runtime
except ImportError:
    from spark.runtime_config import configure_spark_runtime

configure_spark_runtime()

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from metadata_extractor import extract_metadata
except ImportError:
    from spark.metadata_extractor import extract_metadata


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = Path(os.environ.get("LAKEHOUSE_DATA_DIR", PROJECT_ROOT / "data")).resolve()
METADATA_CSV = Path(
    os.environ.get("IMAGE_METADATA_CSV", PROJECT_ROOT / "products.csv")
).resolve()
CATALOG_CSV = Path(
    os.environ.get("PRODUCT_CATALOG_CSV", DATA_DIR / "styles.csv")
).resolve()
IMAGE_DIR = Path(
    os.environ.get("LOCAL_IMAGE_DIR", DATA_DIR / "raw" / "images")
).resolve()

SILVER_COLUMNS = [
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
]


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ProductImage_BronzeToSilver")
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )


def _local_image_for_record(record: dict[str, str], project_root: Path) -> Path | None:
    candidates: list[Path] = []
    for key in ("image_path", "object_path"):
        value = (record.get(key) or "").strip()
        if value and "://" not in value:
            path = Path(value)
            candidates.extend(
                [path, project_root / path, IMAGE_DIR / path.name]
            )

    for key in ("image_id", "product_id"):
        image_stem = (record.get(key) or "").strip()
        if image_stem:
            candidates.extend(
                IMAGE_DIR / f"{image_stem}{extension}"
                for extension in (".jpg", ".jpeg", ".png", ".webp")
            )

    return next((path for path in candidates if path.is_file()), None)


def _read_metadata_records(metadata_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    if not metadata_path.is_file():
        raise FileNotFoundError(f"Image metadata CSV not found: {metadata_path}")

    with metadata_path.open("r", encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        if not reader.fieldnames:
            raise ValueError(f"Image metadata CSV has no header: {metadata_path}")
        fieldnames = list(reader.fieldnames)
        records = [dict(row) for row in reader]

    if not records:
        raise ValueError(f"Image metadata CSV has no records: {metadata_path}")

    enrichment_fields = {
        "image_name",
        "image_path",
        "image_size",
        "format",
        "width",
        "height",
        "upload_time",
        "_metadata_extraction_error",
    }
    if any(field not in fieldnames for field in enrichment_fields):
        fieldnames.extend(field for field in enrichment_fields if field not in fieldnames)

    for record in records:
        missing_values = (
            not (record.get("image_size") or record.get("size") or "").strip()
            or not (record.get("format") or "").strip()
            or not (record.get("width") or "").strip()
            or not (record.get("height") or "").strip()
        )
        if not missing_values:
            continue

        image_path = _local_image_for_record(record, PROJECT_ROOT)
        if image_path is None:
            continue

        try:
            extracted = extract_metadata(image_path)
        except (OSError, ValueError) as error:
            record["_metadata_extraction_error"] = (
                f"{type(error).__name__}: {error}"
            )
            continue

        for field in ("image_name", "image_path", "image_size", "format", "width", "height", "upload_time"):
            if not (record.get(field) or "").strip():
                record[field] = str(extracted[field])

    return fieldnames, records


def _source_column(dataframe: DataFrame, name: str) -> Column:
    if name not in dataframe.columns:
        return F.lit(None).cast("string")
    value = F.trim(F.col(name).cast("string"))
    return F.when(value != "", value)


def _first_nonblank(dataframe: DataFrame, *names: str) -> Column:
    return F.coalesce(*(_source_column(dataframe, name) for name in names))


def _read_catalog(spark: SparkSession, catalog_path: Path) -> DataFrame:
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Product catalog CSV not found: {catalog_path}")

    catalog = spark.read.option("header", "true").option("inferSchema", "false").csv(
        str(catalog_path)
    )
    required_columns = {"id", "productDisplayName"}
    missing = required_columns.difference(catalog.columns)
    if missing:
        raise ValueError(
            f"Product catalog is missing required column(s): {', '.join(sorted(missing))}"
        )

    product_name = _source_column(catalog, "productDisplayName").alias("product_name")
    category = F.coalesce(
        _source_column(catalog, "masterCategory"),
        _source_column(catalog, "subCategory"),
    ).alias("catalog_category")
    catalog = catalog.select(
        _source_column(catalog, "id").alias("product_id"),
        product_name,
        category,
    ).filter(F.col("product_id").isNotNull())

    catalog_window = Window.partitionBy("product_id").orderBy(
        F.col("product_name").asc_nulls_last(),
        F.col("catalog_category").asc_nulls_last(),
    )
    return (
        catalog.withColumn("_catalog_rank", F.row_number().over(catalog_window))
        .filter(F.col("_catalog_rank") == 1)
        .drop("_catalog_rank")
    )


def _normalize_metadata(
    raw: DataFrame,
) -> DataFrame:
    image_path = _first_nonblank(
        raw, "image_path", "object_path", "image_name"
    )
    if "image_id" in raw.columns:
        image_id = _source_column(raw, "image_id")
    else:
        basename = F.regexp_extract(image_path, r"([^/\\]+)$", 1)
        image_id = F.regexp_replace(basename, r"\.[^.]+$", "")

    size_text = _first_nonblank(raw, "image_size", "size")
    size_upper = F.upper(size_text)
    size_number = F.regexp_extract(
        size_upper,
        r"^([0-9]+(?:\.[0-9]+)?)\s*(B|BYTES?|KB|KIB|MB|MIB|GB|GIB)?$",
        1,
    )
    size_unit = F.regexp_extract(
        size_upper,
        r"^([0-9]+(?:\.[0-9]+)?)\s*(B|BYTES?|KB|KIB|MB|MIB|GB|GIB)?$",
        2,
    )
    unit_factor = (
        F.when(size_unit.isin("KB"), F.lit(1000))
        .when(size_unit == "KIB", F.lit(1024))
        .when(size_unit == "MB", F.lit(1000000))
        .when(size_unit == "MIB", F.lit(1048576))
        .when(size_unit == "GB", F.lit(1000000000))
        .when(size_unit == "GIB", F.lit(1073741824))
        .otherwise(F.lit(1))
    )
    image_size = F.round(size_number.try_cast("double") * unit_factor).cast("long")

    width_text = _source_column(raw, "width")
    height_text = _source_column(raw, "height")
    width = F.regexp_extract(width_text, r"^([0-9]+)$", 1).try_cast("int")
    height = F.regexp_extract(height_text, r"^([0-9]+)$", 1).try_cast("int")
    upload_time = _source_column(raw, "upload_time")
    created_at = F.coalesce(
        F.try_to_timestamp(upload_time, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")),
        F.try_to_timestamp(upload_time, F.lit("yyyy-MM-dd'T'HH:mm:ssXXX")),
        F.try_to_timestamp(upload_time, F.lit("yyyy-MM-dd'T'HH:mm:ssX")),
        F.try_to_timestamp(upload_time, F.lit("dd/MM/yyyy")),
        F.try_to_timestamp(upload_time, F.lit("yyyy-MM-dd")),
        F.current_timestamp(),
    )

    normalized = raw.select(
        image_id.alias("image_id"),
        _source_column(raw, "product_id").alias("product_id"),
        _source_column(raw, "category").alias("source_category"),
        image_path.alias("image_path"),
        size_text.alias("source_size"),
        image_size.alias("image_size"),
        F.upper(_source_column(raw, "format")).alias("source_format"),
        width.alias("width"),
        height.alias("height"),
        upload_time.alias("upload_time"),
        created_at.alias("created_at"),
        _source_column(raw, "_metadata_extraction_error").alias(
            "_metadata_extraction_error"
        ),
    )
    return normalized.withColumn(
        "format",
        F.when(F.col("source_format") == "JPEG", F.lit("JPG")).otherwise(
            F.col("source_format")
        ),
    )


def _quarantine_reason() -> Column:
    reasons = [
        F.when(F.col("image_id").isNull(), F.lit("missing_image_id")),
        F.when(F.col("product_id").isNull(), F.lit("missing_product_id")),
        F.when(F.col("image_path").isNull(), F.lit("missing_image_path")),
        F.when(
            F.col("image_size").isNull() | (F.col("image_size") <= 0),
            F.lit("invalid_image_size"),
        ),
        F.when(
            F.col("format").isNull()
            | ~F.col("format").isin("JPG", "PNG", "WEBP"),
            F.lit("unsupported_format"),
        ),
        F.when(
            F.col("width").isNull() | (F.col("width") <= 0),
            F.lit("invalid_width"),
        ),
        F.when(
            F.col("height").isNull() | (F.col("height") <= 0),
            F.lit("invalid_height"),
        ),
        F.when(
            F.col("_metadata_extraction_error").isNotNull(),
            F.lit("image_decode_error"),
        ),
    ]
    return F.concat_ws("; ", *reasons)


def _write_parquet(dataframe: DataFrame, path: Path) -> None:
    dataframe.write.mode("overwrite").parquet(str(path))


def run_etl() -> dict[str, Any]:
    fieldnames, records = _read_metadata_records(METADATA_CSV)
    spark = create_spark_session()
    try:
        catalog = _read_catalog(spark, CATALOG_CSV)

        bronze_products = DATA_DIR / "bronze" / "products"
        bronze_metadata = DATA_DIR / "bronze" / "image_metadata"
        bronze_object_info = DATA_DIR / "bronze" / "object_info"
        silver_path = DATA_DIR / "silver" / "product_images"
        quarantine_path = DATA_DIR / "quarantine" / "product_images"

        for folder in (bronze_products, bronze_metadata):
            folder.mkdir(parents=True, exist_ok=True)
        shutil.copy2(CATALOG_CSV, bronze_products / CATALOG_CSV.name)
        shutil.copy2(METADATA_CSV, bronze_metadata / METADATA_CSV.name)

        enriched_csv = bronze_metadata / f"{METADATA_CSV.stem}_enriched.csv"
        with enriched_csv.open("w", encoding="utf-8", newline="") as destination:
            writer = csv.DictWriter(destination, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        metadata_raw = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .csv(str(enriched_csv))
        )
        normalized = _normalize_metadata(metadata_raw)

        _write_parquet(catalog, bronze_products / "parquet")
        _write_parquet(metadata_raw, bronze_metadata / "parquet")

        object_info = normalized.select(
            "image_id",
            "product_id",
            "image_path",
            "source_size",
            "image_size",
            "source_format",
            "width",
            "height",
            "upload_time",
        )
        _write_parquet(object_info, bronze_object_info / "parquet")

        checked = normalized.withColumn(
            "_quarantine_reason", _quarantine_reason()
        )
        invalid = checked.filter(F.col("_quarantine_reason") != "")
        valid = checked.filter(F.col("_quarantine_reason") == "")

        dedupe_window = Window.partitionBy("image_id").orderBy(
            F.col("created_at").desc_nulls_last(),
            F.col("image_path").asc_nulls_last(),
            F.col("product_id").asc_nulls_last(),
        )
        ranked = valid.withColumn("_duplicate_rank", F.row_number().over(dedupe_window))
        duplicate_rows = (
            ranked.filter(F.col("_duplicate_rank") > 1)
            .drop("_duplicate_rank")
            .withColumn("_quarantine_reason", F.lit("duplicate_image_id"))
        )
        deduplicated = ranked.filter(F.col("_duplicate_rank") == 1).drop(
            "_duplicate_rank"
        )
        quarantined = invalid.unionByName(duplicate_rows)
        _write_parquet(quarantined, quarantine_path)

        joined = deduplicated.join(catalog, on="product_id", how="left")
        matched_count = joined.filter(F.col("product_name").isNotNull()).count()
        unmatched_count = joined.filter(F.col("product_name").isNull()).count()
        silver = joined.select(
            "image_id",
            "product_id",
            "product_name",
            F.coalesce("source_category", "catalog_category").alias("category"),
            "image_path",
            "image_size",
            "format",
            "width",
            "height",
            "created_at",
        ).select(*SILVER_COLUMNS)
        _write_parquet(silver, silver_path)

        report = {
            "metadata_source": str(METADATA_CSV.relative_to(PROJECT_ROOT))
            if METADATA_CSV.is_relative_to(PROJECT_ROOT)
            else METADATA_CSV.name,
            "catalog_source": str(CATALOG_CSV.relative_to(PROJECT_ROOT))
            if CATALOG_CSV.is_relative_to(PROJECT_ROOT)
            else CATALOG_CSV.name,
            "input_records": len(records),
            "invalid_records": invalid.count(),
            "duplicate_records": duplicate_rows.count(),
            "quarantined_records": quarantined.count(),
            "silver_records": silver.count(),
            "matched_product_records": matched_count,
            "unmatched_product_records": unmatched_count,
            "join_keys": "image_metadata.product_id = catalog.id (trimmed strings)",
            "size_unit": "bytes; decimal KB/MB/GB and binary KiB/MiB/GiB",
            "duplicate_priority": "newest created_at, then image_path, then product_id",
        }
        report_path = DATA_DIR / "etl_report.json"
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print("ETL completed:")
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return report
    finally:
        spark.stop()


if __name__ == "__main__":
    run_etl()
