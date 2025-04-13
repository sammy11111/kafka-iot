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


@app.get("/debug/status")
def get_debug_status():
    return {
        "kafka_consumer": consumer is not None,
        "kafka_producer": producer is not None,
        "mongodb_connection": mongo_client is not None,
        "mongodb_collection": collection is not None,
        "schema_loaded": schema is not None
    }


# ----------------------------------------
# Globals
# ----------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
ERROR_TOPIC = "iot.errors.raw-data"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")

# Stats tracking
messages_received = 0
validation_passed = 0
validation_failed = 0
mongo_inserts = 0
mongo_errors = 0

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
        group_id="data-processor-group"
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
    # Test the connection
    mongo_client.admin.command('ping')
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
    global validation_passed, validation_failed

    try:
        # Log incoming data for debugging (truncated for readability)
        data_snippet = json.dumps(data)[:200] + "..." if len(json.dumps(data)) > 200 else json.dumps(data)
        logger.info(f"🔍 Validating data: {data_snippet}")

        if schema is None:
            logger.error("❌ Schema is None. Cannot validate data.")
            validation_failed += 1
            return False

        jsonschema.validate(instance=data, schema=schema)
        logger.info(f"✅ Validation passed for sensor: {data.get('sensor_id', 'unknown')}")
        validation_passed += 1
        return True
    except jsonschema.exceptions.ValidationError as e:
        logger.warning(f"❌ Schema validation failed: {e.message}")
        validation_failed += 1
        return False
    except Exception as e:
        logger.error(f"❌ Validation error: {str(e)}")
        validation_failed += 1
        return False


def process_message(msg):
    global messages_received, mongo_inserts, mongo_errors

    messages_received += 1
    try:
        logger.info(f"📩 Received message #{messages_received} from Kafka")

        if validate(msg):
            if collection is not None:
                try:
                    # Check for duplicates
                    existing = collection.find_one({
                        "sensor_id": msg["sensor_id"],
                        "timestamp": msg["timestamp"]
                    })

                    if existing:
                        logger.info(f"🔄 Duplicate message detected for sensor {msg['sensor_id']}. Skipping insert.")
                    else:
                        result = collection.insert_one(msg)
                        mongo_inserts += 1
                        logger.info(f"📦 Stored message to MongoDB with ID: {result.inserted_id}")
                except Exception as mongo_err:
                    mongo_errors += 1
                    logger.error(f"❌ MongoDB insert error: {str(mongo_err)}")
            else:
                logger.error("❌ MongoDB collection not available")
        else:
            if producer is not None:
                producer.send(ERROR_TOPIC, msg)
                logger.info("🚨 Sent invalid message to error topic")
            else:
                logger.error("❌ Kafka producer not available")
    except Exception as e:
        logger.error(f"❌ Error processing message: {str(e)}")


@app.get("/debug/stats")
def get_stats():
    return {
        "messages_received": messages_received,
        "validation_passed": validation_passed,
        "validation_failed": validation_failed,
        "mongo_inserts": mongo_inserts,
        "mongo_errors": mongo_errors,
        "mongo_uri": MONGO_URI.replace("://", "://***:***@") if "://" in MONGO_URI else MONGO_URI,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_broker": KAFKA_BROKER
    }


# ----------------------------------------
# Kafka Ingestion Loop
# ----------------------------------------
def start_ingestion_loop():
    if not consumer:
        logger.error("❌ No Kafka consumer. Ingestion cannot start.")
        return

    logger.info("🚀 Starting data-processor with schema validation loop...")

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


# For direct execution
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("DATA_PROCESSOR_PORT", "8002"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)