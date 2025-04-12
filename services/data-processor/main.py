import sys
import os
import json
import logging
import threading
import pymongo
import jsonschema
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI
from dotenv import load_dotenv
from libs.kafka_utils import create_topic_if_missing

# ----------------------------------------
# Load environment variables
# ----------------------------------------
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)

# ----------------------------------------
# Logging
# ----------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("data-processor")

# ----------------------------------------
# FastAPI
# ----------------------------------------
app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "ok"}

# ----------------------------------------
# Globals
# ----------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
ERROR_TOPIC = "iot.errors.raw-data"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")

# ----------------------------------------
# Kafka + Mongo Setup
# ----------------------------------------
logger.info(f"🔌 Setting up Kafka with broker: {KAFKA_BROKER}")
create_topic_if_missing(KAFKA_BROKER, ERROR_TOPIC)

try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    logger.info(f"✅ Connected to Kafka topic: {KAFKA_TOPIC}")
except Exception as e:
    logger.error(f"❌ Kafka connection failed: {e}")
    consumer = None

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    logger.info("✅ Kafka producer for error topic ready")
except Exception as e:
    logger.error(f"❌ Kafka producer error: {e}")
    producer = None

try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client["iot"]
    collection = db["sensor_data"]
    logger.info("✅ Connected to MongoDB")
except Exception as e:
    logger.error(f"❌ MongoDB connection failed: {e}")
    mongo_client = None
    collection = None

# ----------------------------------------
# Static Schema
# ----------------------------------------
static_schema = {
    "type": "object",
    "properties": {
        "sensor_id": {"type": "string"},
        "value": {"type": "number"},
        "unit": {"type": "string"},
        "timestamp": {"type": "string"},
        "location": {
            "type": "object",
            "properties": {
                "lat": {"type": "number"},
                "lon": {"type": "number"}
            },
            "required": ["lat", "lon"]
        },
        "box_id": {"type": "string"},
        "box_name": {"type": "string"},
        "exposure": {"type": "string"},
        "height": {"type": ["number", "null"]},
        "sensor_type": {"type": "string"},
        "phenomenon": {"type": "string"}
    },
    "required": ["sensor_id", "value", "unit", "timestamp", "location"]
}
schema = static_schema
logger.info("✅ Using static schema")

# ----------------------------------------
# Validation + Processing
# ----------------------------------------
def validate(data):
    try:
        jsonschema.validate(instance=data, schema=schema)
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.warning(f"❌ Schema validation failed: {e.message}")
        return False
    except Exception as e:
        logger.error(f"❌ Validation error: {str(e)}")
        return False

def process_message(msg):
    try:
        if validate(msg):
            if collection is not None:
                existing = collection.find_one({
                    "sensor_id": msg["sensor_id"],
                    "timestamp": msg["timestamp"]
                })

                if existing:
                    logger.info("🔁 Duplicate message detected. Skipping insert.")
                else:
                    collection.insert_one(msg)
                    logger.info("📦 Inserted new message into MongoDB")
            else:
                logger.error("❌ MongoDB not available")
        else:
            if producer:
                producer.send(ERROR_TOPIC, msg)
                logger.info("🚨 Sent invalid message to error topic")
            else:
                logger.error("❌ Kafka producer unavailable")
    except Exception as e:
        logger.error(f"❌ Processing error: {str(e)}")

# ----------------------------------------
# Kafka Ingestion Loop
# ----------------------------------------
def start_ingestion_loop():
    if not consumer:
        logger.error("❌ No Kafka consumer. Ingestion cannot start.")
        return

    logger.info("🚀 Starting data-processor ingestion loop")

    try:
        for message in consumer:
            try:
                process_message(message.value)
            except Exception as e:
                logger.error(f"❌ Kafka message error: {str(e)}")
    except Exception as e:
        logger.error(f"❌ Fatal error in loop: {str(e)}")

@app.on_event("startup")
def startup_event():
    threading.Thread(target=start_ingestion_loop, daemon=True).start()
    logger.info("✅ Ingestion thread started")
