from unittest.mock import MagicMock, patch

from pyspark.sql import Row

from app.streaming import (
    write_device_stats,
    write_event_type_counts,
    write_raw_events,
    write_sales_by_category,
    write_sales_by_country,
)


def _mock_pg_connection():
    conn = MagicMock()
    cursor_cm = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor_cm
    return conn


def test_write_sales_by_category_upserts_expected_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                window_start="2026-01-01T00:00:00",
                window_end="2026-01-01T00:01:00",
                category="Books",
                total_revenue=100.0,
                order_count=5,
                avg_order_value=20.0,
            )
        ]
    )

    with (
        patch("app.streaming._pg_connection", return_value=_mock_pg_connection()),
        patch("app.streaming.execute_values") as mock_execute_values,
    ):
        write_sales_by_category(df, batch_id=1)

    _cursor, sql, rows = mock_execute_values.call_args.args
    assert "analytics.sales_by_category" in sql
    assert rows == [("2026-01-01T00:00:00", "2026-01-01T00:01:00", "Books", 100.0, 5, 20.0)]


def test_write_device_stats_upserts_expected_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                window_start="2026-01-01T00:00:00",
                window_end="2026-01-01T00:01:00",
                device="Mobile",
                event_count=12,
                unique_sessions=7,
            )
        ]
    )

    with (
        patch("app.streaming._pg_connection", return_value=_mock_pg_connection()),
        patch("app.streaming.execute_values") as mock_execute_values,
    ):
        write_device_stats(df, batch_id=1)

    _cursor, sql, rows = mock_execute_values.call_args.args
    assert "analytics.device_stats" in sql
    assert rows == [("2026-01-01T00:00:00", "2026-01-01T00:01:00", "Mobile", 12, 7)]


def test_write_event_type_counts_upserts_expected_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                window_start="2026-01-01T00:00:00",
                window_end="2026-01-01T00:01:00",
                event_type="login",
                event_count=3,
            )
        ]
    )

    with (
        patch("app.streaming._pg_connection", return_value=_mock_pg_connection()),
        patch("app.streaming.execute_values") as mock_execute_values,
    ):
        write_event_type_counts(df, batch_id=1)

    _cursor, sql, rows = mock_execute_values.call_args.args
    assert "analytics.event_type_counts" in sql
    assert rows == [("2026-01-01T00:00:00", "2026-01-01T00:01:00", "login", 3)]


def test_write_sales_by_country_skips_upsert_when_batch_is_empty(spark):
    empty_df = spark.createDataFrame(
        [], "window_start string, window_end string, country string, "
        "total_revenue double, order_count int, avg_order_value double"
    )

    with (
        patch("app.streaming._pg_connection") as mock_pg_connection,
        patch("app.streaming.execute_values") as mock_execute_values,
    ):
        write_sales_by_country(empty_df, batch_id=1)

    mock_pg_connection.assert_not_called()
    mock_execute_values.assert_not_called()


def test_write_raw_events_inserts_expected_rows(spark):
    df = spark.createDataFrame(
        [
            Row(
                event_id="e1",
                event_type="purchase",
                customer_id="CUST-1",
                session_id="sess-1",
                product_id="PROD-1",
                category="Books",
                price=10.0,
                quantity=2,
                country="France",
                device="Desktop",
                payment_method="PayPal",
                kafka_topic="orders",
                event_time="2026-01-01T00:00:00",
            )
        ]
    )

    with (
        patch("app.streaming._pg_connection", return_value=_mock_pg_connection()),
        patch("app.streaming.execute_values") as mock_execute_values,
    ):
        write_raw_events(df, batch_id=1)

    _cursor, sql, rows = mock_execute_values.call_args.args
    assert "analytics.events" in sql
    assert "ON CONFLICT (event_id) DO NOTHING" in sql
    assert rows == [
        (
            "e1", "purchase", "CUST-1", "sess-1", "PROD-1", "Books",
            10.0, 2, "France", "Desktop", "PayPal", "orders", "2026-01-01T00:00:00",
        )
    ]


def test_write_raw_events_skips_insert_when_batch_is_empty(spark):
    # An empty batch must short-circuit before opening a Postgres connection.
    empty_df = spark.createDataFrame(
        [], "event_id string, event_type string, customer_id string, session_id string, "
        "product_id string, category string, price double, quantity int, country string, "
        "device string, payment_method string, kafka_topic string, event_time string"
    )

    with (
        patch("app.streaming._pg_connection") as mock_pg_connection,
        patch("app.streaming.execute_values") as mock_execute_values,
    ):
        write_raw_events(empty_df, batch_id=1)

    mock_pg_connection.assert_not_called()
    mock_execute_values.assert_not_called()
