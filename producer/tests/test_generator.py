from app.constants import CATEGORIES, COUNTRIES, DEVICES, EVENT_TYPES, PAYMENT_METHODS
from app.generator import EventGenerator


def test_generate_returns_event_with_valid_fields():
    event = EventGenerator().generate()

    assert event.event_type in EVENT_TYPES
    assert event.category in CATEGORIES
    assert event.country in COUNTRIES
    assert event.device in DEVICES
    assert event.payment_method in PAYMENT_METHODS
    assert event.customer_id.startswith("CUST-")
    assert event.product_id.startswith("PROD-")
    assert 10 <= event.price <= 2500
    assert 1 <= event.quantity <= 5


def test_generate_produces_unique_event_and_session_ids():
    events = [EventGenerator().generate() for _ in range(20)]

    assert len({e.event_id for e in events}) == len(events)
    assert len({e.session_id for e in events}) == len(events)
