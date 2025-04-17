import sys
import os
import json
import logging
import threading
import pymongo
import jsonschema
import math
import time
from datetime import datetime, timedelta
from kafka import KafkaConsumer, KafkaProducer
from fastapi import FastAPI
from tracing import setup_tracer
from dotenv import load_dotenv
from libs.kafka_utils import create_topic_if_missing
from libs.env_loader import PROJECT_ROOT  # do not remove

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

# Attach tracing to this service
setup_tracer(app)


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
        "schema_loaded": schema is not None,
        "mongodb_scanner_running": mongodb_scanner_running
    }


# ----------------------------------------
# Globals
# ----------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
ERROR_TOPIC = "iot.errors.raw-data"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
SCAN_INTERVAL = int(os.getenv("NAN_SCAN_INTERVAL", "60"))  # Seconds between MongoDB scans

# Stats tracking
messages_received = 0
validation_passed = 0
validation_failed = 0
nan_values_detected = 0
nan_values_reported = 0
mongo_inserts = 0
mongo_errors = 0
mongodb_scanner_running = False
last_scan_time = None

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
    scan_state_coll = db["scan_state"]
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
def check_for_nan(data):
    """Check if the value field contains NaN"""
    global nan_values_detected

    if "value" in data:
        try:
            # Check if value is NaN
            if isinstance(data["value"], (int, float)) and math.isnan(data["value"]):
                logger.warning(f"🚨 NaN value detected for sensor: {data.get('sensor_id', 'unknown')}")
                nan_values_detected += 1
                return True
        except Exception as e:
            logger.error(f"❌ Error checking for NaN: {str(e)}")

    return False


def validate(data):
    global validation_passed, validation_failed

    try:
        # Log incoming data for debugging (truncated for readability)
        data_snippet = json.dumps(data)[:200] + "..." if len(json.dumps(data)) > 200 else json.dumps(data)
        logger.info(f"🔍 Validating data: {data_snippet}")

        # Check for NaN values before schema validation
        has_nan = check_for_nan(data)

        # We still want to log this, but we won't fail validation
        if has_nan:
            logger.warning(f"⚠️ NaN value detected for sensor: {data.get('sensor_id', 'unknown')}")

        if schema is None:
            logger.error("❌ Schema is None. Cannot validate data.")
            validation_failed += 1
            return False

        # Continue with schema validation
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


def send_to_error_topic(data, error_type, error_description):
    """Send a message to the error topic with standardized format"""
    global nan_values_reported

    if producer is None:
        logger.error("❌ Cannot send to error topic: Kafka producer not available")
        return False

    try:
        error_message = {
            "original_data": data,
            "error_type": error_type,
            "error_description": error_description,
            "sensor_id": data.get("sensor_id", "unknown"),
            "timestamp": data.get("timestamp"),
            "detected_at": datetime.now().isoformat()
        }

        producer.send(ERROR_TOPIC, error_message)

        if error_type == "nan_value":
            nan_values_reported += 1

        logger.info(f"🚨 Sent message with error type '{error_type}' to error topic")
        return True
    except Exception as e:
        logger.error(f"❌ Error sending to error topic: {str(e)}")
        return False


def process_message(msg):
    global messages_received, mongo_inserts, mongo_errors

    messages_received += 1
    try:
        logger.info(f"📩 Received message #{messages_received} from Kafka")

        # Check for NaN values - if present, send to error topic
        has_nan = check_for_nan(msg)
        if has_nan:
            send_to_error_topic(
                msg,
                "nan_value",
                "Value is NaN (Not a Number)"
            )

        # Continue with validation and MongoDB insertion regardless of NaN
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
                        # Insert into MongoDB regardless of NaN value
                        result = collection.insert_one(msg)
                        mongo_inserts += 1
                        logger.info(f"📦 Stored message to MongoDB with ID: {result.inserted_id}")
                except Exception as mongo_err:
                    mongo_errors += 1
                    logger.error(f"❌ MongoDB insert error: {str(mongo_err)}")
            else:
                logger.error("❌ MongoDB collection not available")
        else:
            # For schema validation failures, also send to error topic
            send_to_error_topic(
                msg,
                "schema_validation_failure",
                "Data does not match required schema"
            )
    except Exception as e:
        logger.error(f"❌ Error processing message: {str(e)}")


from datetime import datetime

# ----------------------------------------
# MongoDB NaN Scanner (Incremental)
# ----------------------------------------
def scan_mongodb_for_nan_values(last_scan_time=None):
    start = last_scan_time or datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    end = datetime.utcnow()

    query = {
        "$expr": {"$ne": ["$value", "$value"]},
        "timestamp": {"$gte": start, "$lt": end},
        "error_reported": {"$ne": True}
    }

    try:
        nan_docs = list(collection.find(query))

        logger.info(f"🔍 Found {len(nan_docs)} documents with NaN values")

        for doc in nan_docs:
            detected_at = datetime.utcnow()

            error_payload = {
                "original_data": doc,
                "error_type": "nan_value",
                "error_description": "Value is NaN (Not a Number) - found during MongoDB scan",
                "sensor_id": doc.get("sensor_id"),
                "timestamp": str(doc.get("timestamp")),
                "detected_at": str(detected_at)
            }

            logger.warning("⚠️ NaN value detected during scan: %s", error_payload)

            collection.update_one(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "error_reported": True,
                        "error_type": "nan_value",
                        "detected_at": detected_at
                    }
                }
            )

            send_to_error_topic(error_payload)

        # ✅ ADD THIS BEFORE final log
        scan_state_coll.update_one(
            {"type": "nan_scan"},
            {"$set": {"last_scan": end}},
            upsert=True
        )

        logger.info(f"✅ MongoDB scan complete. {len(nan_docs)} NaN values reported")

    except Exception as e:
        logger.error(f"❌ Error scanning MongoDB for NaN values: {str(e)}")




def mongodb_scanner_loop():
    """Background thread that periodically scans MongoDB for NaN values"""
    global mongodb_scanner_running

    mongodb_scanner_running = True
    logger.info(f"🔄 MongoDB NaN scanner started with interval of {SCAN_INTERVAL} seconds")

    try:
        while True:
            scan_mongodb_for_nan_values()
            time.sleep(SCAN_INTERVAL)
    except Exception as e:
        logger.error(f"❌ MongoDB scanner error: {str(e)}")
    finally:
        mongodb_scanner_running = False


@app.get("/debug/stats")
def get_stats():
    return {
        "messages_received": messages_received,
        "validation_passed": validation_passed,
        "validation_failed": validation_failed,
        "nan_values_detected": nan_values_detected,
        "nan_values_reported": nan_values_reported,
        "mongo_inserts": mongo_inserts,
        "mongo_errors": mongo_errors,
        "mongo_uri": MONGO_URI.replace("://", "://***:***@") if "://" in MONGO_URI else MONGO_URI,
        "kafka_topic": KAFKA_TOPIC,
        "kafka_broker": KAFKA_BROKER,
        "last_mongodb_scan": last_scan_time.isoformat() if last_scan_time else None,
        "mongodb_scanner_running": mongodb_scanner_running
    }


@app.post("/actions/scan-mongodb")
def trigger_mongodb_scan():
    """Endpoint to manually trigger a MongoDB scan for NaN values"""
    try:
        scan_mongodb_for_nan_values()
        return {"status": "ok", "message": "MongoDB scan for NaN values initiated"}
    except Exception as e:
        logger.error(f"❌ Error initiating MongoDB scan: {str(e)}")
        return {"status": "error", "message": str(e)}


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
    # Start the Kafka ingestion thread
    threading.Thread(target=start_ingestion_loop, daemon=True).start()
    logger.info("✅ Kafka ingestion thread started")

    # Start the MongoDB scanner thread
    threading.Thread(target=mongodb_scanner_loop, daemon=True).start()
    logger.info("✅ MongoDB scanner thread started")


# For direct execution
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("DATA_PROCESSOR_PORT", "8002"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)