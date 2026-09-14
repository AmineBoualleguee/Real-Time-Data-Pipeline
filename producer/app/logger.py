from loguru import logger
import sys

logger.remove()

logger.add(
    sys.stdout,
    level="INFO",
    colorize=True,
)

logger.add(
    "logs/producer.log",
    rotation="10 MB",
    retention="10 days",
    level="DEBUG",
)