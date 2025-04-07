import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "libs")))

import json
import logging
import os
import threading

import requests
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI
import pymongo
import jsonschema
from libs.kafka_utils import create_topic_if_missing

# ----------------------------------------
# Load environment variables from root .env
# ----------------------------------------
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)

# ----------------------------------------
# Add shared libraries to sys.path
# ----------------------------------------
libs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../libs"))
if libs_path not in sys.path:
    sys.path.append(libs_path)

# ----------------------
# FastAPI App with Health Check
# ----------------------
app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "ok"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ----------------------
# Environment & Globals
# ----------------------

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
ERROR_TOPIC = "iot.errors.raw-data"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
SCHEMA_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
SCHEMA_SUBJECT = f"{KAFKA_TOPIC}-value"

create_topic_if_missing(KAFKA_BROKER, ERROR_TOPIC)

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

mongo_client = pymongo.MongoClient(MONGO_URI)
collection = mongo_client["iot"]["sensor_data"]

# ----------------------
# Schema Utilities
# ----------------------
def get_latest_schema():
    url = f"{SCHEMA_URL}/subjects/{SCHEMA_SUBJECT}/versions/latest"
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
        logging.info("Stored message to MongoDB.")
    else:
        producer.send(ERROR_TOPIC, msg)
        logging.info("Sent invalid message to error topic.")

# ----------------------
# Background Worker
# ----------------------
def start_ingestion_loop():
    logging.info("Starting data-processor with schema validation loop...")
    for message in consumer:
        process_message(message.value)

# Start ingestion in background when app loads
@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_ingestion_loop, daemon=True).start()
