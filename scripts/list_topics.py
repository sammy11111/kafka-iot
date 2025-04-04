import argparse
from kafka import KafkaConsumer

def list_topics(broker):
    consumer = KafkaConsumer(bootstrap_servers=broker)
    topics = consumer.topics()
    for topic in sorted(topics):
        print(f"- {topic}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List Kafka topics")
    parser.add_argument('--broker', type=str, default='kafka:9092', help='Kafka bootstrap server')
    args = parser.parse_args()
    list_topics(args.broker)
