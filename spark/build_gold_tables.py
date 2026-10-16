try:
    from iceberg_session import ICEBERG_CATALOG, create_iceberg_spark_session
except ImportError:
    from spark.iceberg_session import ICEBERG_CATALOG, create_iceberg_spark_session

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SOURCE_TABLE = f"{ICEBERG_CATALOG}.silver.product_images"
GOLD_NAMESPACE = f"{ICEBERG_CATALOG}.gold"
CATEGORY_STATS_TABLE = f"{GOLD_NAMESPACE}.category_image_stats"
FORMAT_STATS_TABLE = f"{GOLD_NAMESPACE}.image_format_stats"
REQUIRED_COLUMNS = {"category", "image_size", "format"}


def _normalized_source(spark: SparkSession) -> DataFrame:
    if not spark.catalog.tableExists(SOURCE_TABLE):
        raise ValueError(f"Silver Iceberg table does not exist: {SOURCE_TABLE}")

    source = spark.table(SOURCE_TABLE)
    missing_columns = REQUIRED_COLUMNS.difference(source.columns)
    if missing_columns:
        raise ValueError(
            f"Silver table is missing required column(s): "
            f"{', '.join(sorted(missing_columns))}"
        )

    row_count = source.count()
    if row_count == 0:
        raise ValueError(f"Silver table is empty: {SOURCE_TABLE}")

    invalid_size_count = source.filter(
        F.col("image_size").isNull() | (F.col("image_size") <= 0)
    ).count()
    if invalid_size_count:
        raise ValueError(
            f"Silver table contains {invalid_size_count} row(s) with invalid "
            "image_size; Gold tables were not updated."
        )

    category = F.trim(F.col("category").cast("string"))
    image_format = F.upper(F.trim(F.col("format").cast("string")))
    return source.select(
        F.when(category.isNull() | (category == ""), F.lit("UNKNOWN"))
        .otherwise(category)
        .alias("category"),
        F.col("image_size").cast("long").alias("image_size"),
        F.when(image_format == "JPEG", F.lit("JPG"))
        .when(image_format.isNull() | (image_format == ""), F.lit("UNKNOWN"))
        .otherwise(image_format)
        .alias("format"),
    )


def _assert_same_rows(expected: DataFrame, actual: DataFrame, table_name: str) -> None:
    actual = actual.select(*expected.columns)
    if (
        expected.exceptAll(actual).limit(1).count()
        or actual.exceptAll(expected).limit(1).count()
    ):
        raise RuntimeError(
            f"Read-back validation failed: Iceberg table {table_name} "
            "does not match the computed aggregates."
        )


def build_gold_tables() -> dict[str, dict[str, int]]:
    spark = create_iceberg_spark_session("ProductImage_BuildGold")
    try:
        source = _normalized_source(spark)
        source_metrics = source.agg(
            F.count(F.lit(1)).alias("image_count"),
            F.sum("image_size").alias("total_image_size_bytes"),
        ).first()
        source_count = int(source_metrics["image_count"])
        source_size_bytes = int(source_metrics["total_image_size_bytes"])

        category_stats = source.groupBy("category").agg(
            F.count(F.lit(1)).cast("long").alias("image_count"),
            F.sum("image_size").cast("long").alias("total_image_size_bytes"),
        )
        format_stats = source.groupBy("format").agg(
            F.count(F.lit(1)).cast("long").alias("image_count")
        )

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GOLD_NAMESPACE}")
        category_stats.writeTo(CATEGORY_STATS_TABLE).createOrReplace()
        format_stats.writeTo(FORMAT_STATS_TABLE).createOrReplace()

        category_readback = spark.table(CATEGORY_STATS_TABLE)
        format_readback = spark.table(FORMAT_STATS_TABLE)
        _assert_same_rows(category_stats, category_readback, CATEGORY_STATS_TABLE)
        _assert_same_rows(format_stats, format_readback, FORMAT_STATS_TABLE)

        category_metrics = category_readback.agg(
            F.sum("image_count").alias("image_count"),
            F.sum("total_image_size_bytes").alias("total_image_size_bytes"),
        ).first()
        format_image_count = format_readback.agg(
            F.sum("image_count").alias("image_count")
        ).first()["image_count"]
        category_count = int(category_metrics["image_count"])
        category_size_bytes = int(category_metrics["total_image_size_bytes"])
        format_count = int(format_image_count)
        if (
            category_count != source_count
            or format_count != source_count
            or category_size_bytes != source_size_bytes
        ):
            raise RuntimeError(
                "Gold totals do not reconcile with Silver: "
                f"Silver images={source_count}, category images={category_count}, "
                f"format images={format_count}, Silver bytes={source_size_bytes}, "
                f"category bytes={category_size_bytes}."
            )

        category_groups = category_readback.count()
        format_groups = format_readback.count()
        results = {
            CATEGORY_STATS_TABLE: {
                "groups": category_groups,
                "images": category_count,
                "total_image_size_bytes": category_size_bytes,
            },
            FORMAT_STATS_TABLE: {
                "groups": format_groups,
                "images": format_count,
            },
        }
        print(f"Silver source: {SOURCE_TABLE}")
        print(f"Silver images: {source_count}")
        print(
            f"{CATEGORY_STATS_TABLE}: {category_groups} groups, "
            f"{category_count} images, {category_size_bytes} bytes"
        )
        category_readback.orderBy("category").show(truncate=False)
        print(f"{FORMAT_STATS_TABLE}: {format_groups} groups, {format_count} images")
        format_readback.orderBy("format").show(truncate=False)
        print("Gold read-back and aggregate reconciliation: PASS")
        return results
    finally:
        spark.stop()


if __name__ == "__main__":
    build_gold_tables()
