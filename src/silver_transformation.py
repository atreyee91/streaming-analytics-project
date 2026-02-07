"""
Silver Layer Transformation Script

Reads the bronze Delta table from data/bronze/events/ and applies
business-level transformations: filtering invalid records, parsing
timestamps, standardizing strings, enriching with derived columns,
and deduplicating — producing a clean, analysis-ready Delta table
at data/silver/events/.

Dropped from bronze: timestamp, __is_valid_event_type, source_file, processing_date
"""

import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import TimestampType, DateType, IntegerType
from pyspark.sql.functions import (
    col, trim, lower, to_timestamp, to_date, hour, when, regexp_extract,
    count, avg, min as spark_min, max as spark_max, sum as spark_sum,
)


def create_spark_session() -> SparkSession:
    """Create a local SparkSession with Delta Lake extensions."""
    return (
        SparkSession.builder
        .appName("SilverTransformation")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_bronze_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the bronze Delta table."""
    return spark.read.format("delta").load(input_path)


def filter_valid_events(df: DataFrame) -> DataFrame:
    """Keep only valid events and drop internal bronze columns."""
    df = df.filter(col("__is_valid_event_type") == True)  # noqa: E712
    df = df.drop("__is_valid_event_type", "source_file", "processing_date")
    return df


def parse_timestamps(df: DataFrame) -> DataFrame:
    """Convert string timestamp to event_timestamp, extract event_date and event_hour."""
    df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
    df = df.withColumn("event_date", to_date(col("event_timestamp")))
    df = df.withColumn("event_hour", hour(col("event_timestamp")).cast(IntegerType()))
    df = df.drop("timestamp")
    return df


def standardize_strings(df: DataFrame) -> DataFrame:
    """Trim all string fields, lowercase event_type and category."""
    string_columns = [f.name for f in df.schema.fields if str(f.dataType) == "StringType"]
    for col_name in string_columns:
        df = df.withColumn(col_name, trim(col(col_name)))
    df = df.withColumn("event_type", lower(col("event_type")))
    df = df.withColumn("category", lower(col("category")))
    return df


def enrich_events(df: DataFrame) -> DataFrame:
    """Add derived columns: is_bot_traffic, device_type, total_amount."""
    df = df.withColumn(
        "is_bot_traffic",
        col("user_id").startswith("bot_")
    )

    df = df.withColumn(
        "device_type",
        when(
            col("user_agent").rlike(r"(?i)(iPhone|iPod|Android.*Mobile|Windows Phone)"), "mobile"
        ).when(
            col("user_agent").rlike(r"(?i)(iPad|Android(?!.*Mobile))"), "tablet"
        ).when(
            col("user_agent").rlike(r"(?i)(Windows|Macintosh|X11|Linux)"), "desktop"
        ).otherwise("other")
    )

    df = df.withColumn(
        "total_amount",
        when(col("event_type") == "purchase", col("price") * col("quantity"))
        .otherwise(None)
    )

    return df


def deduplicate_events(df: DataFrame) -> DataFrame:
    """Remove duplicate events based on event_id."""
    return df.dropDuplicates(["event_id"])


def write_to_delta(df: DataFrame, output_path: str) -> None:
    """Write DataFrame to Delta table, partitioned by event_date."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(output_path)
    )


def print_summary(df: DataFrame, bronze_count: int, output_path: str) -> None:
    """Print a comprehensive summary of the silver transformation."""
    total = df.count()
    print("\n" + "=" * 60)
    print("SILVER LAYER TRANSFORMATION SUMMARY")
    print("=" * 60)

    # Record counts
    print(f"\nBronze records:  {bronze_count}")
    print(f"Silver records:  {total}")
    filtered = bronze_count - total
    print(f"Records filtered: {filtered}")

    # Events by type
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

    # Events by category
    print("\nEvents by category:")
    cat_counts = (
        df.groupBy("category")
        .count()
        .orderBy(col("count").desc())
        .collect()
    )
    for row in cat_counts:
        pct = (row["count"] / total) * 100
        print(f"  {row['category']:15s} {row['count']:6d}  ({pct:5.1f}%)")

    # Device type distribution
    print("\nDevice type distribution:")
    device_counts = (
        df.groupBy("device_type")
        .count()
        .orderBy(col("count").desc())
        .collect()
    )
    for row in device_counts:
        pct = (row["count"] / total) * 100
        print(f"  {row['device_type']:15s} {row['count']:6d}  ({pct:5.1f}%)")

    # Bot traffic
    bot_count = df.filter(col("is_bot_traffic") == True).count()  # noqa: E712
    bot_pct = (bot_count / total) * 100 if total > 0 else 0
    print(f"\nBot traffic: {bot_count} ({bot_pct:.1f}%)")

    # Purchase metrics
    purchases = df.filter(col("event_type") == "purchase")
    purchase_count = purchases.count()
    if purchase_count > 0:
        stats = purchases.select(
            count("total_amount").alias("cnt"),
            spark_sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_order"),
            spark_min("total_amount").alias("min_order"),
            spark_max("total_amount").alias("max_order"),
        ).collect()[0]
        print(f"\nPurchase metrics ({purchase_count} purchases):")
        print(f"  Total revenue: ${stats['total_revenue']:.2f}")
        print(f"  Avg order:     ${stats['avg_order']:.2f}")
        print(f"  Min order:     ${stats['min_order']:.2f}")
        print(f"  Max order:     ${stats['max_order']:.2f}")

    # Date range
    date_range = df.select(
        spark_min("event_date").alias("min_date"),
        spark_max("event_date").alias("max_date"),
    ).collect()[0]
    print(f"\nDate range: {date_range['min_date']} to {date_range['max_date']}")

    # Null checks on critical fields
    critical_fields = ["event_id", "user_id", "session_id", "event_timestamp",
                       "event_type", "event_date", "event_hour"]
    print("\nNull checks (critical fields):")
    for field in critical_fields:
        null_count = df.filter(col(field).isNull()).count()
        status = "PASS" if null_count == 0 else "WARN"
        print(f"  {field:20s} nulls: {null_count:5d}  [{status}]")

    # Delta table path
    print(f"\nDelta table written to: {output_path}")
    print("=" * 60)


def main():
    """Orchestrate the silver layer transformation pipeline."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    input_path = os.path.join(project_root, "data", "bronze", "events")
    output_path = os.path.join(project_root, "data", "silver", "events")

    # Validate bronze Delta table exists
    delta_log = os.path.join(input_path, "_delta_log")
    if not os.path.isdir(delta_log):
        print(f"ERROR: Bronze Delta table not found: {input_path}")
        print(f"       Missing _delta_log at: {delta_log}")
        sys.exit(1)

    print(f"Reading bronze Delta table from: {input_path}")

    spark = None
    try:
        spark = create_spark_session()

        # Read bronze
        bronze_df = read_bronze_events(spark, input_path)
        bronze_count = bronze_df.count()
        print(f"Bronze records: {bronze_count}")

        # Transform pipeline
        df = filter_valid_events(bronze_df)
        df = parse_timestamps(df)
        df = standardize_strings(df)
        df = enrich_events(df)
        df = deduplicate_events(df)

        # Write silver Delta table
        write_to_delta(df, output_path)

        # Re-read and verify
        silver_df = spark.read.format("delta").load(output_path)
        print_summary(silver_df, bronze_count, output_path)

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
