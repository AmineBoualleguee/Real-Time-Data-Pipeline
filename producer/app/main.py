from time import sleep

from app.config import EVENTS_PER_SECOND
from app.logger import logger
from app.producer import KafkaEventProducer
from app.topic_manager import create_topics

create_topics()

producer = KafkaEventProducer()

delay = 1 / EVENTS_PER_SECOND

logger.info("Starting Event Producer...")

while True:

    event = producer.send()

    logger.info(
        f"{event.event_type} -> {event.product_id}"
    )

    sleep(delay)