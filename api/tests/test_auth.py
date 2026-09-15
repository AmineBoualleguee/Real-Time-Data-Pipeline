from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app.auth as auth
from app.database import get_db
from app.main import app


def _db_override(mock_db):
    def _override():
        yield mock_db

    return _override


@pytest.fixture
def client_with_api_key():
    app.dependency_overrides[get_db] = _db_override(MagicMock())
    original = auth.API_KEY
    auth.API_KEY = "secret-test-key"
    yield TestClient(app)
    auth.API_KEY = original
    app.dependency_overrides.clear()


def test_protected_route_rejects_missing_key(client_with_api_key):
    resp = client_with_api_key.get("/events/recent")
    assert resp.status_code == 401


def test_protected_route_rejects_wrong_key(client_with_api_key):
    resp = client_with_api_key.get("/events/recent", headers={"X-API-Key": "nope"})
    assert resp.status_code == 401


def test_protected_route_accepts_correct_key(client_with_api_key):
    mock_db = MagicMock()
    mock_db.execute.return_value.mappings.return_value.all.return_value = []
    app.dependency_overrides[get_db] = _db_override(mock_db)

    resp = client_with_api_key.get("/events/recent", headers={"X-API-Key": "secret-test-key"})
    assert resp.status_code == 200


def test_health_stays_open_without_a_key(client_with_api_key):
    resp = client_with_api_key.get("/health")
    assert resp.status_code == 200
