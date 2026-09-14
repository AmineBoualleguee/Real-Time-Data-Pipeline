from pydantic import BaseModel
from datetime import datetime


class Event(BaseModel):
    event_id: str
    event_type: str

    customer_id: str
    session_id: str
    product_id: str

    category: str

    price: float
    quantity: int

    country: str
    device: str

    payment_method: str

    timestamp: datetime