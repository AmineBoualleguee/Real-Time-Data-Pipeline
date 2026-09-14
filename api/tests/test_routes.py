from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.database import get_db
from app.main import app


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def client(mock_db):
    def _override():
        yield mock_db

    app.dependency_overrides[get_db] = _override
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_root():
    with TestClient(app) as c:
        resp = c.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"service": "realtime-data-pipeline-api", "status": "running"}


def test_health_ok(client, mock_db):
    resp = client.get("/health")

    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
    mock_db.execute.assert_called_once()


def test_recent_events_shapes_rows_into_json(client, mock_db):
    row = {
        "event_id": "evt-1",
        "event_type": "purchase",
        "customer_id": "CUST-1",
        "product_id": "PROD-1",
        "category": "Books",
        "price": 9.99,
        "quantity": 1,
        "country": "France",
        "device": "Desktop",
        "payment_method": "PayPal",
        "kafka_topic": "orders",
        "event_time": "2026-01-01T00:00:00Z",
    }
    mock_db.execute.return_value.mappings.return_value.all.return_value = [row]

    resp = client.get("/events/recent?limit=10")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["events"] == [row]


def test_sales_by_category_returns_windows(client, mock_db):
    window = {
        "window_start": "2026-01-01T00:00:00Z",
        "window_end": "2026-01-01T00:01:00Z",
        "category": "Books",
        "total_revenue": 100.0,
        "order_count": 5,
        "avg_order_value": 20.0,
    }
    mock_db.execute.return_value.mappings.return_value.all.return_value = [window]

    resp = client.get("/analytics/sales-by-category?minutes=30")

    assert resp.status_code == 200
    assert resp.json() == {"count": 1, "windows": [window]}


def test_summary_combines_totals_and_top_entries(client, mock_db):
    totals = MagicMock()
    totals.mappings.return_value.one.return_value = {"total_revenue": 500.5, "total_orders": 42}

    top_category = MagicMock()
    top_category.mappings.return_value.first.return_value = {
        "category": "Books",
        "total_revenue": 200.0,
    }

    top_country = MagicMock()
    top_country.mappings.return_value.first.return_value = {
        "country": "France",
        "total_revenue": 300.0,
    }

    mock_db.execute.side_effect = [totals, top_category, top_country]

    resp = client.get("/analytics/summary?minutes=60")

    assert resp.status_code == 200
    body = resp.json()
    assert body["total_revenue"] == 500.5
    assert body["total_orders"] == 42
    assert body["top_category"] == {"category": "Books", "total_revenue": 200.0}
    assert body["top_country"] == {"country": "France", "total_revenue": 300.0}
