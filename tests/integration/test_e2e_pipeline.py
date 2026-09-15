"""End-to-end smoke test against a running `docker compose` stack.

Proves the full chain is alive: producer -> Kafka -> Spark -> Postgres -> API.
Requires at minimum `postgres`, `kafka`, `producer`, `spark`, and `api` to be up
(see .github/workflows/ci.yml's `e2e-test` job, or run it locally against
`docker compose up -d --build`).
"""

import os
from datetime import datetime, timezone

import requests

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")
API_KEY = os.getenv("API_KEY", "local-dev-key-change-me")
HEADERS = {"X-API-Key": API_KEY}


def test_health_endpoint_confirms_db_connectivity():
    resp = requests.get(f"{API_BASE_URL}/health", timeout=10)
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_protected_routes_reject_requests_without_an_api_key():
    resp = requests.get(f"{API_BASE_URL}/events/recent", timeout=10)
    assert resp.status_code == 401


def test_latest_raw_event_is_fresh():
    """A recent event_time proves producer -> Kafka -> Spark -> Postgres is live,
    not just that historical rows exist from an earlier run."""
    resp = requests.get(f"{API_BASE_URL}/events/recent?limit=1", headers=HEADERS, timeout=10)
    assert resp.status_code == 200

    body = resp.json()
    assert body["count"] == 1

    event_time = datetime.fromisoformat(body["events"][0]["event_time"].replace("Z", "+00:00"))
    age_seconds = (datetime.now(timezone.utc) - event_time).total_seconds()
    assert age_seconds < 120, f"latest event is {age_seconds:.0f}s old - pipeline may be stalled"


def test_windowed_aggregates_have_been_computed():
    """Proves the Spark streaming job's windowed aggregation + Postgres upsert path works,
    not just the raw event log."""
    resp = requests.get(
        f"{API_BASE_URL}/analytics/summary?minutes=1440", headers=HEADERS, timeout=10
    )
    assert resp.status_code == 200

    body = resp.json()
    assert body["total_orders"] > 0
    assert body["total_revenue"] > 0


def test_metrics_endpoint_is_scrapeable():
    resp = requests.get(f"{API_BASE_URL}/metrics", timeout=10)
    assert resp.status_code == 200
    assert "http_requests_total" in resp.text
