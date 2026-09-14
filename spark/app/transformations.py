from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Mirrors producer/app/models.py::Event -- the JSON payload published to every Kafka topic.
EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("country", StringType(), False),
        StructField("device", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("timestamp", TimestampType(), False),
    ]
)

PURCHASE_EVENT_TYPES = ("purchase",)


def parse_events(raw_df, watermark_delay="2 minutes"):
    """Turn raw Kafka records (topic/value bytes) into typed, watermarked event rows."""

    parsed = raw_df.select(
        F.col("topic").alias("kafka_topic"),
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"),
    ).select("kafka_topic", "data.*")

    return (
        parsed.withColumnRenamed("timestamp", "event_time")
        .withColumn("revenue", F.col("price") * F.col("quantity"))
        .withWatermark("event_time", watermark_delay)
    )


def sales_by_category(events_df, window_duration="1 minute"):
    purchases = events_df.filter(F.col("event_type").isin(*PURCHASE_EVENT_TYPES))
    return (
        purchases.groupBy(F.window("event_time", window_duration).alias("w"), "category")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.avg("revenue").alias("avg_order_value"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "category",
            "total_revenue",
            "order_count",
            "avg_order_value",
        )
    )


def sales_by_country(events_df, window_duration="1 minute"):
    purchases = events_df.filter(F.col("event_type").isin(*PURCHASE_EVENT_TYPES))
    return (
        purchases.groupBy(F.window("event_time", window_duration).alias("w"), "country")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.avg("revenue").alias("avg_order_value"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "country",
            "total_revenue",
            "order_count",
            "avg_order_value",
        )
    )


def device_stats(events_df, window_duration="1 minute"):
    return (
        events_df.groupBy(F.window("event_time", window_duration).alias("w"), "device")
        .agg(
            F.count("*").alias("event_count"),
            F.approx_count_distinct("session_id").alias("unique_sessions"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "device",
            "event_count",
            "unique_sessions",
        )
    )


def event_type_counts(events_df, window_duration="1 minute"):
    return (
        events_df.groupBy(F.window("event_time", window_duration).alias("w"), "event_type")
        .agg(F.count("*").alias("event_count"))
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "event_type",
            "event_count",
        )
    )
