from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models import Event


def _valid_payload(**overrides):
    payload = dict(
        event_id="evt-1",
        event_type="purchase",
        customer_id="CUST-1",
        session_id="sess-1",
        product_id="PROD-1",
        category="Books",
        price=9.99,
        quantity=1,
        country="France",
        device="Desktop",
        payment_method="PayPal",
        timestamp=datetime.now(timezone.utc),
    )
    payload.update(overrides)
    return payload


def test_event_accepts_valid_payload():
    event = Event(**_valid_payload())

    assert event.event_id == "evt-1"
    assert event.price == 9.99


def test_event_rejects_missing_required_field():
    payload = _valid_payload()
    del payload["price"]

    with pytest.raises(ValidationError):
        Event(**payload)
