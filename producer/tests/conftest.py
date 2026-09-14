import sys
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# app.kafka_client builds a real KafkaProducer at import time, which tries to
# connect to a broker. Stub it out so the test suite doesn't need a live Kafka.
import kafka  # noqa: E402

kafka.KafkaProducer = MagicMock(return_value=MagicMock())
