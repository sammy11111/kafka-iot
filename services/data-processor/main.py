import json
import logging
import os

import requests
from libs.kafka_utils import create_topic_if_missing
from kafka import KafkaConsumer, KafkaProducer
import pymongo
import jsonschema

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
create_topic_if_missing(KAFKA_BROKER, ERROR_TOPIC)
ERROR_TOPIC = "iot.errors.raw-data"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMA_SUBJECT = f"{KAFKA_TOPIC}-value"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

mongo_client = pymongo.MongoClient(MONGO_URI)
db = mongo_client["iot"]
collection = db["sensor_data"]

def get_latest_schema():
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{SCHEMA_SUBJECT}/versions/latest"
    try:
        res = requests.get(url)
        res.raise_for_status()
        return json.loads(res.json()["schema"])
    except Exception as e:
        logging.error(f"Failed to fetch schema: {e}")
        return None

schema = get_latest_schema()

def validate(data):
    try:
        jsonschema.validate(instance=data, schema=schema)
        return True
    except jsonschema.exceptions.ValidationError as e:
        logging.warning(f"Validation error: {e.message}")
        return False

def process_message(msg):
    if validate(msg):
        collection.insert_one(msg)
        logging.info(f"Stored message to MongoDB.")
    else:
        producer.send(ERROR_TOPIC, msg)
        logging.info("Sent invalid message to error topic.")

if __name__ == "__main__":
    logging.info("Starting data-processor with schema validation...")
    for message in consumer:
        process_message(message.value)
