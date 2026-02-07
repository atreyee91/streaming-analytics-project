"""
Gold Layer Aggregation Script

Reads the silver Delta table from data/silver/events/ and produces
three business-level aggregate tables:
  - data/gold/sales_metrics/     — Revenue and order metrics by date/category
  - data/gold/user_engagement/   — Session, funnel, and conversion metrics by date
  - data/gold/traffic_analysis/  — Hourly traffic patterns by device type
"""

import os
import sys

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, countDistinct,
    when, lit, round as spark_round,
)


def create_spark_session() -> SparkSession:
    """Create a local SparkSession with Delta Lake extensions."""
    return (
        SparkSession.builder
        .appName("GoldAggregation")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def read_silver_events(spark: SparkSession, input_path: str) -> DataFrame:
    """Read the silver Delta table."""
    return spark.read.format("delta").load(input_path)


def create_sales_metrics(df: DataFrame) -> DataFrame:
    """Aggregate purchase events into sales metrics by date and category."""
    purchases = df.filter(col("event_type") == "purchase")

    return (
        purchases
        .groupBy("event_date", "category")
        .agg(
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_order_value"),
            count("*").alias("total_orders"),
            spark_sum("quantity").alias("total_quantity"),
        )
        .orderBy("event_date", "category")
    )


def create_user_engagement(df: DataFrame) -> DataFrame:
    """Compute session-level engagement and conversion funnel metrics by date."""
    # Count events by type per session
    session_events = (
        df.groupBy("event_date", "session_id")
        .agg(
            count("*").alias("event_count"),
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("has_page_view"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("has_add_to_cart"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("has_purchase"),
            countDistinct("user_id").alias("users_in_session"),
        )
    )

    # Flag sessions that had each event type (boolean flags)
    session_flags = (
        session_events
        .withColumn("viewed", when(col("has_page_view") > 0, 1).otherwise(0))
        .withColumn("carted", when(col("has_add_to_cart") > 0, 1).otherwise(0))
        .withColumn("purchased", when(col("has_purchase") > 0, 1).otherwise(0))
    )

    # Aggregate to date level
    daily = (
        session_flags
        .groupBy("event_date")
        .agg(
            count("session_id").alias("total_sessions"),
            spark_sum("users_in_session").alias("total_users"),
            spark_sum("has_page_view").alias("total_page_views"),
            spark_sum("has_add_to_cart").alias("total_add_to_carts"),
            spark_sum("has_purchase").alias("total_purchases"),
            spark_round(avg("event_count"), 2).alias("avg_events_per_session"),
            spark_sum("viewed").alias("_sessions_with_views"),
            spark_sum("carted").alias("_sessions_with_carts"),
            spark_sum("purchased").alias("_sessions_with_purchases"),
        )
    )

    # Compute conversion rates
    daily = daily.withColumn(
        "conversion_rate_view_to_cart",
        spark_round(
            when(col("_sessions_with_views") > 0,
                 col("_sessions_with_carts") / col("_sessions_with_views") * 100)
            .otherwise(0.0), 2
        )
    ).withColumn(
        "conversion_rate_cart_to_purchase",
        spark_round(
            when(col("_sessions_with_carts") > 0,
                 col("_sessions_with_purchases") / col("_sessions_with_carts") * 100)
            .otherwise(0.0), 2
        )
    )

    # Drop intermediate columns
    daily = daily.drop("_sessions_with_views", "_sessions_with_carts", "_sessions_with_purchases")

    return daily.orderBy("event_date")


def create_traffic_analysis(df: DataFrame) -> DataFrame:
    """Analyze traffic patterns by date, hour, and device type."""
    return (
        df.groupBy("event_date", "event_hour", "device_type")
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions"),
            spark_sum(when(col("is_bot_traffic") == True, 1).otherwise(0)).alias("bot_events"),  # noqa: E712
            spark_sum(when(col("is_bot_traffic") == False, 1).otherwise(0)).alias("legitimate_events"),  # noqa: E712
        )
        .orderBy("event_date", "event_hour", "device_type")
    )


def write_gold_table(df: DataFrame, output_path: str, table_name: str) -> None:
    """Write a gold aggregate DataFrame to Delta (no partitioning for small tables)."""
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(output_path)
    )
    print(f"  Wrote {table_name} to: {output_path}")


def print_summary(sales_df: DataFrame, engagement_df: DataFrame,
                  traffic_df: DataFrame, gold_base: str) -> None:
    """Print a comprehensive summary of the gold aggregation results."""
    print("\n" + "=" * 60)
    print("GOLD LAYER AGGREGATION SUMMARY")
    print("=" * 60)

    # Row counts
    sales_count = sales_df.count()
    engagement_count = engagement_df.count()
    traffic_count = traffic_df.count()
    print(f"\nTable row counts:")
    print(f"  sales_metrics:    {sales_count}")
    print(f"  user_engagement:  {engagement_count}")
    print(f"  traffic_analysis: {traffic_count}")

    # Key revenue metrics
    revenue_row = sales_df.agg(
        spark_sum("total_revenue").alias("total_rev"),
        spark_sum("total_orders").alias("total_ord"),
    ).collect()[0]
    print(f"\nKey metrics:")
    print(f"  Total revenue:      ${revenue_row['total_rev']:.2f}")
    print(f"  Total orders:       {revenue_row['total_ord']}")

    # Conversion rates
    eng_rows = engagement_df.collect()
    for row in eng_rows:
        print(f"  View→Cart rate:     {row['conversion_rate_view_to_cart']:.1f}%")
        print(f"  Cart→Purchase rate: {row['conversion_rate_cart_to_purchase']:.1f}%")

    # Peak hour
    peak = traffic_df.groupBy("event_hour").agg(
        spark_sum("event_count").alias("total")
    ).orderBy(col("total").desc()).first()
    if peak:
        print(f"  Peak traffic hour:  {peak['event_hour']}:00 ({peak['total']} events)")

    # Bot percentage
    bot_row = traffic_df.agg(
        spark_sum("bot_events").alias("bots"),
        spark_sum("legitimate_events").alias("legit"),
    ).collect()[0]
    total_events = bot_row["bots"] + bot_row["legit"]
    bot_pct = (bot_row["bots"] / total_events * 100) if total_events > 0 else 0
    print(f"  Bot traffic:        {bot_row['bots']} events ({bot_pct:.1f}%)")

    # Table paths
    print(f"\nGold tables written to:")
    print(f"  {os.path.join(gold_base, 'sales_metrics')}")
    print(f"  {os.path.join(gold_base, 'user_engagement')}")
    print(f"  {os.path.join(gold_base, 'traffic_analysis')}")
    print("=" * 60)


def main():
    """Orchestrate the gold layer aggregation pipeline."""
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    input_path = os.path.join(project_root, "data", "silver", "events")
    gold_base = os.path.join(project_root, "data", "gold")

    sales_path = os.path.join(gold_base, "sales_metrics")
    engagement_path = os.path.join(gold_base, "user_engagement")
    traffic_path = os.path.join(gold_base, "traffic_analysis")

    # Validate silver Delta table exists
    delta_log = os.path.join(input_path, "_delta_log")
    if not os.path.isdir(delta_log):
        print(f"ERROR: Silver Delta table not found: {input_path}")
        print(f"       Missing _delta_log at: {delta_log}")
        sys.exit(1)

    print(f"Reading silver Delta table from: {input_path}")

    spark = None
    try:
        spark = create_spark_session()

        # Read silver
        silver_df = read_silver_events(spark, input_path)
        silver_count = silver_df.count()
        print(f"Silver records: {silver_count}")

        # Create gold aggregates
        print("\nCreating gold tables...")
        sales_metrics = create_sales_metrics(silver_df)
        user_engagement = create_user_engagement(silver_df)
        traffic_analysis = create_traffic_analysis(silver_df)

        # Write gold tables
        write_gold_table(sales_metrics, sales_path, "sales_metrics")
        write_gold_table(user_engagement, engagement_path, "user_engagement")
        write_gold_table(traffic_analysis, traffic_path, "traffic_analysis")

        # Re-read and verify
        sales_verified = spark.read.format("delta").load(sales_path)
        engagement_verified = spark.read.format("delta").load(engagement_path)
        traffic_verified = spark.read.format("delta").load(traffic_path)

        print_summary(sales_verified, engagement_verified, traffic_verified, gold_base)

    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
