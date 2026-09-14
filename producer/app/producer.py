from app.generator import EventGenerator
from app.kafka_client import producer

TOPIC_MAPPING = {
    "login": "user-events",
    "logout": "user-events",
    "search": "user-events",
    "product_view": "user-events",

    "add_to_cart": "orders",
    "remove_from_cart": "orders",
    "checkout": "orders",

    "payment": "payments",

    "purchase": "orders",

    "review": "notifications",
}


class KafkaEventProducer:

    def __init__(self):
        self.generator = EventGenerator()

    def send(self):

        event = self.generator.generate()

        topic = TOPIC_MAPPING[event.event_type]

        producer.send(
            topic,
            event.model_dump(mode="json")
        )

        producer.flush()

        return event