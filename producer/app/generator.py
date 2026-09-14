import random
import uuid
from datetime import datetime, timezone

from faker import Faker

from app.constants import (
    EVENT_TYPES,
    CATEGORIES,
    COUNTRIES,
    DEVICES,
    PAYMENT_METHODS,
)
from app.models import Event

fake = Faker()


class EventGenerator:

    def generate(self) -> Event:
        return Event(
            event_id=str(uuid.uuid4()),
            event_type=random.choice(EVENT_TYPES),

            customer_id=f"CUST-{random.randint(1000,9999)}",
            session_id=str(uuid.uuid4())[:12],
            product_id=f"PROD-{random.randint(100,999)}",

            category=random.choice(CATEGORIES),

            price=round(random.uniform(10, 2500), 2),
            quantity=random.randint(1, 5),

            country=random.choice(COUNTRIES),
            device=random.choice(DEVICES),

            payment_method=random.choice(PAYMENT_METHODS),

            timestamp=datetime.now(timezone.utc)
        )