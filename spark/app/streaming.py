import logging

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession

from app.config import (
    CHECKPOINT_DIR,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPICS,
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
    TRIGGER_INTERVAL,
    WATERMARK_DELAY,
    WINDOW_DURATION,
)
from app.transformations import (
    device_stats,
    event_type_counts,
    parse_events,
    sales_by_category,
    sales_by_country,
)

logger = logging.getLogger("spark-streaming")


def build_spark_session():
    return (
        SparkSession.builder.appName("realtime-pipeline-streaming")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _pg_connection():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )


def read_events(spark):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return parse_events(raw, watermark_delay=WATERMARK_DELAY)


def write_raw_events(batch_df, batch_id):
    # Written via a single driver-side psycopg2 bulk insert rather than Spark's distributed
    # JDBC writer: at this event volume (tens-hundreds of rows/batch) the JDBC writer's
    # per-partition connection overhead made this query fall badly behind the other four,
    # which all use this same pattern - observed drifting ~250 batches behind under sustained load.
    if batch_df.rdd.isEmpty():
        return

    rows = [
        (
            r.event_id,
            r.event_type,
            r.customer_id,
            r.session_id,
            r.product_id,
            r.category,
            r.price,
            r.quantity,
            r.country,
            r.device,
            r.payment_method,
            r.kafka_topic,
            r.event_time,
        )
        for r in batch_df.select(
            "event_id",
            "event_type",
            "customer_id",
            "session_id",
            "product_id",
            "category",
            "price",
            "quantity",
            "country",
            "device",
            "payment_method",
            "kafka_topic",
            "event_time",
        ).collect()
    ]

    conn = _pg_connection()
    try:
        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO analytics.events
                    (event_id, event_type, customer_id, session_id, product_id, category,
                     price, quantity, country, device, payment_method, kafka_topic, event_time)
                VALUES %s
                ON CONFLICT (event_id) DO NOTHING
                """,
                rows,
            )
        conn.commit()
        logger.info("[events] batch %s: inserted %s rows", batch_id, len(rows))
    finally:
        conn.close()


def _upsert(rows, sql, batch_id, label):
    if not rows:
        return

    conn = _pg_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
        conn.commit()
        logger.info("[%s] batch %s: upserted %s rows", label, batch_id, len(rows))
    finally:
        conn.close()


def write_sales_by_category(batch_df, batch_id):
    rows = [
        (
            r.window_start,
            r.window_end,
            r.category,
            float(r.total_revenue or 0),
            int(r.order_count or 0),
            float(r.avg_order_value or 0),
        )
        for r in batch_df.collect()
    ]
    _upsert(
        rows,
        """
        INSERT INTO analytics.sales_by_category
            (window_start, window_end, category, total_revenue, order_count, avg_order_value)
        VALUES %s
        ON CONFLICT (window_start, category) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            total_revenue = EXCLUDED.total_revenue,
            order_count = EXCLUDED.order_count,
            avg_order_value = EXCLUDED.avg_order_value,
            updated_at = now()
        """,
        batch_id,
        "sales_by_category",
    )


def write_sales_by_country(batch_df, batch_id):
    rows = [
        (
            r.window_start,
            r.window_end,
            r.country,
            float(r.total_revenue or 0),
            int(r.order_count or 0),
            float(r.avg_order_value or 0),
        )
        for r in batch_df.collect()
    ]
    _upsert(
        rows,
        """
        INSERT INTO analytics.sales_by_country
            (window_start, window_end, country, total_revenue, order_count, avg_order_value)
        VALUES %s
        ON CONFLICT (window_start, country) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            total_revenue = EXCLUDED.total_revenue,
            order_count = EXCLUDED.order_count,
            avg_order_value = EXCLUDED.avg_order_value,
            updated_at = now()
        """,
        batch_id,
        "sales_by_country",
    )


def write_device_stats(batch_df, batch_id):
    rows = [
        (
            r.window_start,
            r.window_end,
            r.device,
            int(r.event_count or 0),
            int(r.unique_sessions or 0),
        )
        for r in batch_df.collect()
    ]
    _upsert(
        rows,
        """
        INSERT INTO analytics.device_stats
            (window_start, window_end, device, event_count, unique_sessions)
        VALUES %s
        ON CONFLICT (window_start, device) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            event_count = EXCLUDED.event_count,
            unique_sessions = EXCLUDED.unique_sessions,
            updated_at = now()
        """,
        batch_id,
        "device_stats",
    )


def write_event_type_counts(batch_df, batch_id):
    rows = [
        (r.window_start, r.window_end, r.event_type, int(r.event_count or 0))
        for r in batch_df.collect()
    ]
    _upsert(
        rows,
        """
        INSERT INTO analytics.event_type_counts
            (window_start, window_end, event_type, event_count)
        VALUES %s
        ON CONFLICT (window_start, event_type) DO UPDATE SET
            window_end = EXCLUDED.window_end,
            event_count = EXCLUDED.event_count,
            updated_at = now()
        """,
        batch_id,
        "event_type_counts",
    )


def run():
    logging.basicConfig(level=logging.INFO)
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    events_df = read_events(spark)

    queries = [
        events_df.writeStream.foreachBatch(write_raw_events)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/events")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(),
        sales_by_category(events_df, WINDOW_DURATION)
        .writeStream.foreachBatch(write_sales_by_category)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/sales_by_category")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(),
        sales_by_country(events_df, WINDOW_DURATION)
        .writeStream.foreachBatch(write_sales_by_country)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/sales_by_country")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(),
        device_stats(events_df, WINDOW_DURATION)
        .writeStream.foreachBatch(write_device_stats)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/device_stats")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(),
        event_type_counts(events_df, WINDOW_DURATION)
        .writeStream.foreachBatch(write_event_type_counts)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/event_type_counts")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start(),
    ]

    logger.info("Started %s streaming queries", len(queries))
    spark.streams.awaitAnyTermination()
