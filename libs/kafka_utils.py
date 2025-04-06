from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import logging
import time
from kafka.errors import NoBrokersAvailable


def create_topic_if_missing(broker, topic, partitions=1, replication=1):
    admin = KafkaAdminClient(bootstrap_servers=broker)
    try:
        admin.create_topics(
            [
                NewTopic(
                    name=topic,
                    num_partitions=partitions,
                    replication_factor=replication,
                )
            ]
        )
        logging.info(f"Created missing topic: {topic}")
    except TopicAlreadyExistsError:
        logging.info(f"Topic already exists: {topic}")
    finally:
        admin.close()

def connect_with_retry(factory, retries=5, base_delay=2):
    for i in range(retries):
        try:
            return factory()
        except NoBrokersAvailable as e:
            print(f"❌ Attempt {i+1}: No brokers available - {e}")
            time.sleep(base_delay * (2 ** i))
    print("❌ All attempts to connect to Kafka failed.")
    return None

