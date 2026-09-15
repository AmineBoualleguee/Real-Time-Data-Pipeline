import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPICS = os.getenv(
    "KAFKA_TOPICS",
    "user-events,orders,payments,inventory,notifications",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "analytics")
POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin123")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints")
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "10 seconds")
