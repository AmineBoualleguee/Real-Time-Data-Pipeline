from unittest.mock import patch

from app.constants import EVENT_TYPES
from app.producer import TOPIC_MAPPING, KafkaEventProducer


def test_every_event_type_has_a_topic_mapping():
    for event_type in EVENT_TYPES:
        assert event_type in TOPIC_MAPPING


def test_send_publishes_generated_event_to_its_mapped_topic():
    with patch("app.producer.producer") as mock_producer:
        event = KafkaEventProducer().send()

        topic, payload = mock_producer.send.call_args.args
        assert topic == TOPIC_MAPPING[event.event_type]
        assert payload["event_id"] == event.event_id
        mock_producer.flush.assert_called_once()
