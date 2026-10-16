try:
    from iceberg_session import ICEBERG_CATALOG, create_iceberg_spark_session
except ImportError:
    from spark.iceberg_session import ICEBERG_CATALOG, create_iceberg_spark_session

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


SOURCE_TABLE = f"{ICEBERG_CATALOG}.silver.product_images"
DEMO_TABLE = f"{ICEBERG_CATALOG}.silver.product_images_evolution_demo"
NEW_COLUMN = "aspect_ratio"
REQUIRED_COLUMNS = {"image_id", "width", "height"}


def _latest_snapshot(spark: SparkSession) -> tuple[int, str]:
    snapshot = spark.sql(
        f"""
        SELECT snapshot_id, CAST(committed_at AS STRING) AS committed_at
        FROM {DEMO_TABLE}.snapshots
        ORDER BY committed_at DESC, snapshot_id DESC
        LIMIT 1
        """
    ).first()
    if snapshot is None:
        raise RuntimeError(f"No Iceberg snapshots found for {DEMO_TABLE}.")
    return int(snapshot["snapshot_id"]), str(snapshot["committed_at"])


def _validate_source(spark: SparkSession) -> DataFrame:
    if not spark.catalog.tableExists(SOURCE_TABLE):
        raise ValueError(f"Silver Iceberg table does not exist: {SOURCE_TABLE}")

    source = spark.table(SOURCE_TABLE)
    missing_columns = REQUIRED_COLUMNS.difference(source.columns)
    if missing_columns:
        raise ValueError(
            f"Silver table is missing required column(s): "
            f"{', '.join(sorted(missing_columns))}"
        )

    invalid_dimensions = source.filter(
        F.col("width").isNull()
        | F.col("height").isNull()
        | (F.col("width") <= 0)
        | (F.col("height") <= 0)
    ).count()
    if invalid_dimensions:
        raise ValueError(
            f"Silver table contains {invalid_dimensions} row(s) with invalid "
            "dimensions; demo table was not changed."
        )

    if source.isEmpty():
        raise ValueError(f"Silver Iceberg table is empty: {SOURCE_TABLE}")
    if NEW_COLUMN in source.columns:
        raise ValueError(
            f"Source table already contains {NEW_COLUMN!r}; "
            "refusing to evolve the demo from an already evolved schema."
        )
    return source


def run_iceberg_evolution_demo() -> dict[str, int | str]:
    spark = create_iceberg_spark_session("ProductImage_IcebergEvolutionDemo")
    try:
        source = _validate_source(spark)
        source_count = source.count()
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.silver")

        source.writeTo(DEMO_TABLE).createOrReplace()
        baseline_snapshot_id, baseline_committed_at = _latest_snapshot(spark)
        baseline = spark.sql(
            f"SELECT image_id, width, height "
            f"FROM {DEMO_TABLE} VERSION AS OF {baseline_snapshot_id}"
        )
        baseline_count = baseline.count()
        if baseline_count != source_count:
            raise RuntimeError(
                f"Baseline time-travel query returned {baseline_count} rows; "
                f"expected {source_count}."
            )

        spark.sql(
            f"ALTER TABLE {DEMO_TABLE} "
            f"ADD COLUMN {NEW_COLUMN} DOUBLE"
        )
        spark.sql(
            f"UPDATE {DEMO_TABLE} "
            f"SET {NEW_COLUMN} = CAST(width AS DOUBLE) / CAST(height AS DOUBLE)"
        )

        evolved = spark.table(DEMO_TABLE)
        current_count = evolved.count()
        if current_count != source_count:
            raise RuntimeError(
                f"Evolved demo table has {current_count} rows; "
                f"expected {source_count}."
            )

        null_ratio_count = evolved.filter(F.col(NEW_COLUMN).isNull()).count()
        if null_ratio_count:
            raise RuntimeError(
                f"Schema evolution left {null_ratio_count} row(s) without "
                f"{NEW_COLUMN}."
            )

        latest_snapshot_id, latest_committed_at = _latest_snapshot(spark)
        if latest_snapshot_id == baseline_snapshot_id:
            raise RuntimeError(
                "Schema evolution did not create a new Iceberg data snapshot."
            )

        historical = spark.read.format("iceberg").option(
            "snapshot-id", str(baseline_snapshot_id)
        ).load(DEMO_TABLE)
        historical_count = historical.count()
        if historical_count != source_count:
            raise RuntimeError(
                f"Snapshot {baseline_snapshot_id} returned {historical_count} "
                f"rows; expected {source_count}."
            )

        print(f"Silver source (unchanged): {SOURCE_TABLE}")
        print(f"Isolated demo table: {DEMO_TABLE}")
        print(f"Rows: {source_count}")
        print(
            f"Baseline snapshot {baseline_snapshot_id} "
            f"({baseline_committed_at}): {baseline_count} rows"
        )
        print(
            f"Evolved snapshot {latest_snapshot_id} "
            f"({latest_committed_at}): {current_count} rows"
        )
        print("Schema before evolution:")
        baseline.printSchema()
        print(f"Schema after adding {NEW_COLUMN}:")
        evolved.printSchema()
        print(f"Historical snapshot read-back schema ({baseline_snapshot_id}):")
        historical.printSchema()
        evolved.select("image_id", "width", "height", NEW_COLUMN).show(
            5, truncate=False
        )
        spark.sql(
            f"SELECT snapshot_id, committed_at, operation "
            f"FROM {DEMO_TABLE}.snapshots "
            "ORDER BY committed_at, snapshot_id"
        ).show(truncate=False)
        print(
            "Time Travel query: "
            f"SELECT image_id, width, height FROM {DEMO_TABLE} "
            f"VERSION AS OF {baseline_snapshot_id}"
        )
        print("Snapshot and schema-evolution checks: PASS")
        return {
            "demo_table": DEMO_TABLE,
            "rows": current_count,
            "baseline_snapshot_id": baseline_snapshot_id,
            "latest_snapshot_id": latest_snapshot_id,
        }
    finally:
        spark.stop()


if __name__ == "__main__":
    run_iceberg_evolution_demo()
