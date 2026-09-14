import json

from pyspark.sql import Row

from app.transformations import (
    device_stats,
    event_type_counts,
    parse_events,
    sales_by_category,
    sales_by_country,
)

BASE_EVENT = {
    "event_id": "e1",
    "event_type": "purchase",
    "customer_id": "CUST-1",
    "session_id": "sess-1",
    "product_id": "PROD-1",
    "category": "Books",
    "price": 10.0,
    "quantity": 2,
    "country": "France",
    "device": "Desktop",
    "payment_method": "PayPal",
    "timestamp": "2026-01-01T00:00:00Z",
}


def _kafka_df(spark, events, topic="orders"):
    rows = [Row(topic=topic, value=json.dumps(e).encode("utf-8")) for e in events]
    return spark.createDataFrame(rows)


def test_parse_events_extracts_fields_and_computes_revenue(spark):
    parsed = parse_events(_kafka_df(spark, [BASE_EVENT]), watermark_delay="0 seconds")

    row = parsed.collect()[0]
    assert row.event_id == "e1"
    assert row.category == "Books"
    assert row.kafka_topic == "orders"
    assert row.revenue == 20.0


def test_sales_by_category_only_counts_purchases(spark):
    events = [BASE_EVENT, {**BASE_EVENT, "event_id": "e2", "event_type": "login"}]
    parsed = parse_events(_kafka_df(spark, events), watermark_delay="0 seconds")

    result = sales_by_category(parsed, window_duration="1 hour").collect()

    assert len(result) == 1
    assert result[0].category == "Books"
    assert result[0].order_count == 1
    assert result[0].total_revenue == 20.0


def test_sales_by_country_aggregates_revenue_per_country(spark):
    events = [
        BASE_EVENT,
        {**BASE_EVENT, "event_id": "e2", "country": "France", "price": 5.0, "quantity": 1},
        {**BASE_EVENT, "event_id": "e3", "country": "Spain", "price": 100.0, "quantity": 1},
    ]
    parsed = parse_events(_kafka_df(spark, events), watermark_delay="0 seconds")

    result = {r.country: r for r in sales_by_country(parsed, window_duration="1 hour").collect()}

    assert result["France"].total_revenue == 25.0
    assert result["France"].order_count == 2
    assert result["Spain"].total_revenue == 100.0


def test_device_stats_counts_all_event_types_not_just_purchases(spark):
    events = [BASE_EVENT, {**BASE_EVENT, "event_id": "e2", "event_type": "login"}]
    parsed = parse_events(_kafka_df(spark, events), watermark_delay="0 seconds")

    result = device_stats(parsed, window_duration="1 hour").collect()

    assert len(result) == 1
    assert result[0].device == "Desktop"
    assert result[0].event_count == 2
    assert result[0].unique_sessions == 1


def test_event_type_counts_groups_by_event_type(spark):
    events = [
        BASE_EVENT,
        {**BASE_EVENT, "event_id": "e2", "event_type": "login"},
        {**BASE_EVENT, "event_id": "e3", "event_type": "login"},
    ]
    parsed = parse_events(_kafka_df(spark, events), watermark_delay="0 seconds")

    result = {r.event_type: r.event_count for r in event_type_counts(parsed, "1 hour").collect()}

    assert result["purchase"] == 1
    assert result["login"] == 2
