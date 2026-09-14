import orjson
from kafka import KafkaProducer

from app.config import KAFKA_BOOTSTRAP


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: orjson.dumps(v),
    retries=5,
)