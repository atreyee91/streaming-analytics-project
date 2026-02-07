"""
Bronze Layer Ingestion Script

Reads raw JSON Lines clickstream events from data/raw/events/
and writes them as a Delta table to data/bronze/events/.

Bronze layer retains all data as-is with added ingestion metadata
and quality flags. No rows are dropped — bad data is flagged only.
"""

import os
import sys
from datetime import date

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql.functions import (
    current_timestamp, input_file_name, lit, col, when, count, avg, min as spark_min, max as spark_max
)


def create_spark_session() -> SparkSession:
    """Create a local SparkSession with Delta Lake extensions."""
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def get_event_schema() -> StructType:
    """Return the explicit schema for raw clickstream events."""
    return StructType([
        StructField("event_id", StringType(), nullable=True),
        StructField("user_id", StringType(), nullable=True),
        StructField("session_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=True),
        StructField("timestamp", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("category", StringType(), nullable=True),
        StructField("user_agent", StringType(), nullable=True),
        StructField("ip_address", StringType(), nullable=True),
        StructField("price", DoubleType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
    ])


def read_raw_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read raw JSON Lines files with PERMISSIVE mode to catch malformed rows."""
    schema = get_event_schema()

    # Add _corrupt_record to capture malformed JSON without data loss
    schema_with_corrupt = schema.add(
        StructField("_corrupt_record", StringType(), nullable=True)
    )

    return (
        spark.read
        .schema(schema_with_corrupt)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(input_path)
    )


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    """Add ingestion metadata columns: timestamp, source file, and processing date."""
    return (
        df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
        .withColumn("processing_date", lit(str(date.today())))
    )


def run_quality_checks(df: DataFrame) -> DataFrame:
    """Flag invalid rows without dropping them. Bronze retains all data."""
    valid_event_types = ["page_view", "add_to_cart", "purchase"]

    df = df.withColumn(
        "__is_valid_event_type",
        col("event_type").isin(valid_event_types)
    )

    return df


def write_to_delta(df: DataFrame, output_path: str) -> None:
    """Write DataFrame to Delta table, partitioned by processing_date."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("processing_date")
        .save(output_path)
    )


def print_summary(df: DataFrame, output_path: str) -> None:
    """Print a comprehensive summary of the ingested data."""
    total = df.count()
    print("\n" + "=" * 60)
    print("BRONZE LAYER INGESTION SUMMARY")
    print("=" * 60)

    # Total records
    print(f"\nTotal records ingested: {total}")

    # Events by type with percentages
    print("\nEvents by type:")
    type_counts = (
        df.groupBy("event_type")
        .count()
        .orderBy(col("count").desc())
        .collect()
    )
    for row in type_counts:
        pct = (row["count"] / total) * 100
        print(f"  {row['event_type']:15s} {row['count']:6d}  ({pct:5.1f}%)")

    # Valid/invalid event type counts
    valid_count = df.filter(col("__is_valid_event_type") == True).count()  # noqa: E712
    invalid_count = df.filter(
        (col("__is_valid_event_type") == False) | col("__is_valid_event_type").isNull()  # noqa: E712
    ).count()
    print(f"\nEvent type validation:")
    print(f"  Valid:   {valid_count}")
    print(f"  Invalid: {invalid_count}")

    # Null checks on critical fields
    critical_fields = ["event_id", "user_id", "session_id", "timestamp"]
    print("\nNull checks (critical fields):")
    for field in critical_fields:
        null_count = df.filter(col(field).isNull()).count()
        status = "PASS" if null_count == 0 else "WARN"
        print(f"  {field:15s} nulls: {null_count:5d}  [{status}]")

    # Corrupt records check
    if "_corrupt_record" in df.columns:
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        status = "PASS" if corrupt_count == 0 else "WARN"
        print(f"  {'_corrupt_record':15s} count: {corrupt_count:5d}  [{status}]")

    # Source file count
    file_count = df.select("source_file").distinct().count()
    print(f"\nSource files processed: {file_count}")

    # Purchase price stats
    purchases = df.filter(col("event_type") == "purchase")
    if purchases.count() > 0:
        stats = purchases.select(
            avg("price").alias("avg_price"),
            spark_min("price").alias("min_price"),
            spark_max("price").alias("max_price"),
        ).collect()[0]
        print(f"\nPurchase price stats:")
        print(f"  Min:  ${stats['min_price']:.2f}")
        print(f"  Max:  ${stats['max_price']:.2f}")
        print(f"  Avg:  ${stats['avg_price']:.2f}")

    # Delta table path
    print(f"\nDelta table written to: {output_path}")
    print("=" * 60)


def main():
    """Orchestrate the bronze layer ingestion pipeline."""
    # Resolve paths relative to project root (one level up from src/)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    input_path = os.path.join(project_root, "data", "raw", "events")
    output_path = os.path.join(project_root, "data", "bronze", "events")

    # Validate input directory
    if not os.path.isdir(input_path):
        print(f"ERROR: Input directory not found: {input_path}")
        sys.exit(1)

    json_files = [f for f in os.listdir(input_path) if f.endswith(".json")]
    if not json_files:
        print(f"ERROR: No JSON files found in: {input_path}")
        sys.exit(1)

    print(f"Found {len(json_files)} JSON file(s) in {input_path}")

    spark = None
    try:
        spark = create_spark_session()

        # Read raw events
        df = read_raw_events(spark, input_path)

        # Add ingestion metadata
        df = add_ingestion_metadata(df)

        # Run quality checks (flag, don't drop)
        df = run_quality_checks(df)

        # Cache to allow querying _corrupt_record (Spark restriction)
        df = df.cache()

        # Drop _corrupt_record column before writing if all clean
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count == 0:
            df = df.drop("_corrupt_record")
        else:
            print(f"WARNING: {corrupt_count} corrupt record(s) found — retaining _corrupt_record column")

        # Write to Delta
        write_to_delta(df, output_path)

        # Re-read from Delta to verify
        verified_df = spark.read.format("delta").load(output_path)

        # Print summary from the verified Delta table
        print_summary(verified_df, output_path)

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
