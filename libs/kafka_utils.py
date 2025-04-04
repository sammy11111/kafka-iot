from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging

def create_topic_if_missing(broker, topic, partitions=1, replication=1):
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)])
        logging.info(f"Created missing topic: {topic}")
    except TopicAlreadyExistsError:
        logging.info(f"Topic already exists: {topic}")
    finally:
        admin.close()
