CREATE SCHEMA IF NOT EXISTS analytics;

-- Raw event log: every event ingested from Kafka, written by the Spark streaming job.
CREATE TABLE IF NOT EXISTS analytics.events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    event_type      VARCHAR(32) NOT NULL,
    customer_id     VARCHAR(32) NOT NULL,
    session_id      VARCHAR(32) NOT NULL,
    product_id      VARCHAR(32) NOT NULL,
    category        VARCHAR(64) NOT NULL,
    price           NUMERIC(10, 2) NOT NULL,
    quantity        INTEGER NOT NULL,
    country         VARCHAR(64) NOT NULL,
    device          VARCHAR(32) NOT NULL,
    payment_method  VARCHAR(32) NOT NULL,
    kafka_topic     VARCHAR(64) NOT NULL,
    event_time      TIMESTAMPTZ NOT NULL,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id)
);

CREATE INDEX IF NOT EXISTS idx_events_event_time ON analytics.events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON analytics.events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_category ON analytics.events (category);

-- 1-minute windowed revenue aggregation by product category (purchase events only).
CREATE TABLE IF NOT EXISTS analytics.sales_by_category (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    category         VARCHAR(64) NOT NULL,
    total_revenue    NUMERIC(14, 2) NOT NULL DEFAULT 0,
    order_count      INTEGER NOT NULL DEFAULT 0,
    avg_order_value  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, category)
);

CREATE INDEX IF NOT EXISTS idx_sales_by_category_window ON analytics.sales_by_category (window_start DESC);

-- 1-minute windowed revenue aggregation by country (purchase events only).
CREATE TABLE IF NOT EXISTS analytics.sales_by_country (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    country          VARCHAR(64) NOT NULL,
    total_revenue    NUMERIC(14, 2) NOT NULL DEFAULT 0,
    order_count      INTEGER NOT NULL DEFAULT 0,
    avg_order_value  NUMERIC(14, 2) NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, country)
);

CREATE INDEX IF NOT EXISTS idx_sales_by_country_window ON analytics.sales_by_country (window_start DESC);

-- 1-minute windowed activity by device type (all events).
CREATE TABLE IF NOT EXISTS analytics.device_stats (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    device           VARCHAR(32) NOT NULL,
    event_count      INTEGER NOT NULL DEFAULT 0,
    unique_sessions  INTEGER NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, device)
);

CREATE INDEX IF NOT EXISTS idx_device_stats_window ON analytics.device_stats (window_start DESC);

-- 1-minute windowed counts by event type (funnel view: login -> search -> add_to_cart -> checkout -> purchase).
CREATE TABLE IF NOT EXISTS analytics.event_type_counts (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    event_type       VARCHAR(32) NOT NULL,
    event_count      INTEGER NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, event_type)
);

CREATE INDEX IF NOT EXISTS idx_event_type_counts_window ON analytics.event_type_counts (window_start DESC);

-- Deletes rows older than retention_days from every analytics table. Called on a schedule by
-- the `retention` service (see docker-compose.yml / scripts/retention.sh) so the raw event log
-- and windowed aggregates don't grow unbounded.
CREATE OR REPLACE FUNCTION analytics.cleanup_old_data(retention_days INTEGER)
RETURNS TABLE(table_name TEXT, deleted_rows BIGINT) AS $$
DECLARE
    deleted BIGINT;
    cutoff TIMESTAMPTZ := now() - (retention_days || ' days')::interval;
BEGIN
    DELETE FROM analytics.events WHERE event_time < cutoff;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    table_name := 'events'; deleted_rows := deleted; RETURN NEXT;

    DELETE FROM analytics.sales_by_category WHERE window_start < cutoff;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    table_name := 'sales_by_category'; deleted_rows := deleted; RETURN NEXT;

    DELETE FROM analytics.sales_by_country WHERE window_start < cutoff;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    table_name := 'sales_by_country'; deleted_rows := deleted; RETURN NEXT;

    DELETE FROM analytics.device_stats WHERE window_start < cutoff;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    table_name := 'device_stats'; deleted_rows := deleted; RETURN NEXT;

    DELETE FROM analytics.event_type_counts WHERE window_start < cutoff;
    GET DIAGNOSTICS deleted = ROW_COUNT;
    table_name := 'event_type_counts'; deleted_rows := deleted; RETURN NEXT;
END;
$$ LANGUAGE plpgsql;
