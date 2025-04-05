import argparse
from kafka.admin import KafkaAdminClient
from kafka.errors import UnknownTopicOrPartitionError


def delete_topic(broker, topic):
    admin_client = KafkaAdminClient(bootstrap_servers=broker)
    try:
        admin_client.delete_topics([topic])
        print(f"✅ Deleted topic: {topic}")
    except UnknownTopicOrPartitionError:
        print(f"ℹ️ Topic does not exist: {topic}")
    finally:
        admin_client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Delete a Kafka topic")
    parser.add_argument(
        "--broker", type=str, default="kafka:9092", help="Kafka bootstrap server"
    )
    parser.add_argument("--topic", type=str, required=True, help="Kafka topic name")
    args = parser.parse_args()
    delete_topic(args.broker, args.topic)
