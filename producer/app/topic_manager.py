from kafka.admin import KafkaAdminClient, NewTopic

from app.config import KAFKA_BOOTSTRAP

TOPICS = [
    "user-events",
    "orders",
    "payments",
    "inventory",
    "notifications",
]


def create_topics():

    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP
    )

    existing = admin.list_topics()

    new_topics = []

    for topic in TOPICS:
        if topic not in existing:
            new_topics.append(
                NewTopic(
                    name=topic,
                    num_partitions=3,
                    replication_factor=1,
                )
            )

    if new_topics:
        admin.create_topics(new_topics)

    admin.close()