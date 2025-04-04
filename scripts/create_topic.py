import argparse
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic(broker, topic, partitions, replication):
    admin_client = KafkaAdminClient(bootstrap_servers=broker)
    topic_obj = NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)
    try:
        admin_client.create_topics([topic_obj])
        print(f"✅ Created topic: {topic}")
    except TopicAlreadyExistsError:
        print(f"ℹ️ Topic already exists: {topic}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create Kafka topic if it doesn't exist")
    parser.add_argument('--broker', type=str, default='kafka:9092', help='Kafka bootstrap server')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic name')
    parser.add_argument('--partitions', type=int, default=1, help='Number of partitions')
    parser.add_argument('--replication', type=int, default=1, help='Replication factor')
    parser.add_argument('--delay', type=int, default=10, help='Delay before attempting topic creation (sec)')
    args = parser.parse_args()

    print(f"Waiting {args.delay} seconds for Kafka to initialize...")
    time.sleep(args.delay)
    create_topic(args.broker, args.topic, args.partitions, args.replication)
