import argparse
import logging
import time
import json
import requests
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def with_retries(func):
    def wrapper(*args, **kwargs):
        for attempt in range(5):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed: {e}")
                time.sleep(5)
        raise Exception("Max retries exceeded.")

    return wrapper


@with_retries
def create_topic(broker, topic, partitions, replication):
    admin_client = KafkaAdminClient(bootstrap_servers=broker)
    topic_obj = NewTopic(
        name=topic, num_partitions=partitions, replication_factor=replication
    )
    try:
        admin_client.create_topics([topic_obj])
        logging.info(f"Created topic: {topic}")
    except TopicAlreadyExistsError:
        logging.info(f"Topic already exists: {topic}")
    finally:
        admin_client.close()


@with_retries
def delete_topic(broker, topic):
    admin_client = KafkaAdminClient(bootstrap_servers=broker)
    try:
        admin_client.delete_topics([topic])
        logging.info(f"Deleted topic: {topic}")
    except UnknownTopicOrPartitionError:
        logging.info(f"Topic does not exist: {topic}")
    finally:
        admin_client.close()


@with_retries
def list_topics(broker):
    consumer = KafkaConsumer(bootstrap_servers=broker)
    topics = consumer.topics()
    for t in sorted(topics):
        print(t)


def register_schema(subject, file, registry_url):
    with open(file, "r") as f:
        schema_json = f.read()
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    payload = {"schemaType": "JSON", "schema": schema_json}
    url = f"{registry_url}/subjects/{subject}/versions"
    response = requests.post(url, headers=headers, json=payload)
    if response.ok:
        logging.info(f"Registered schema for subject: {subject}")
    else:
        logging.error(f"Error registering schema: {response.text}")


def get_schema(subject, registry_url):
    url = f"{registry_url}/subjects/{subject}/versions/latest"
    response = requests.get(url)
    if response.ok:
        schema = response.json()
        print(json.dumps(schema, indent=2))
    else:
        logging.error(f"Error fetching schema: {response.text}")


def list_schemas(registry_url):
    url = f"{registry_url}/subjects"
    response = requests.get(url)
    if response.ok:
        for subject in response.json():
            print(subject)
    else:
        logging.error(f"Error listing schemas: {response.text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Tools CLI")
    subparsers = parser.add_subparsers(dest="command")

    # Topic commands
    topic_parser = subparsers.add_parser("topic")
    topic_sub = topic_parser.add_subparsers(dest="action")
    t_create = topic_sub.add_parser("create")
    t_create.add_argument("--broker", default="kafka:9092")
    t_create.add_argument("--topic", required=True)
    t_create.add_argument("--partitions", type=int, default=1)
    t_create.add_argument("--replication", type=int, default=1)

    t_delete = topic_sub.add_parser("delete")
    t_delete.add_argument("--broker", default="kafka:9092")
    t_delete.add_argument("--topic", required=True)

    t_list = topic_sub.add_parser("list")
    t_list.add_argument("--broker", default="kafka:9092")

    # Schema commands
    schema_parser = subparsers.add_parser("schema")
    schema_sub = schema_parser.add_subparsers(dest="action")
    s_register = schema_sub.add_parser("register")
    s_register.add_argument("--subject", required=True)
    s_register.add_argument("--file", required=True)
    s_register.add_argument("--registry-url", default="http://localhost:8081")

    s_get = schema_sub.add_parser("get")
    s_get.add_argument("--subject", required=True)
    s_get.add_argument("--registry-url", default="http://localhost:8081")

    s_list = schema_sub.add_parser("list")
    s_list.add_argument("--registry-url", default="http://localhost:8081")

    args = parser.parse_args()

    if args.command == "topic":
        if args.action == "create":
            create_topic(args.broker, args.topic, args.partitions, args.replication)
        elif args.action == "delete":
            delete_topic(args.broker, args.topic)
        elif args.action == "list":
            list_topics(args.broker)
    elif args.command == "schema":
        if args.action == "register":
            register_schema(args.subject, args.file, args.registry_url)
        elif args.action == "get":
            get_schema(args.subject, args.registry_url)
        elif args.action == "list":
            list_schemas(args.registry_url)
