import os
import sys
import time
import json
import logging
import threading
import shutil
import psutil
import requests
import asyncio
from kafka import KafkaProducer, KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from kafka.admin import NewTopic
from dotenv import load_dotenv
from fastapi import FastAPI
from tracing import setup_tracer
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta

from libs.env_loader import PROJECT_ROOT  # do not remove

# ----------------------------------------
# Honor PYTHONPATH manually at runtime
# ----------------------------------------
pythonpath = os.environ.get("PYTHONPATH", "/app/libs")
if pythonpath and pythonpath not in sys.path:
    sys.path.append(pythonpath)
    print(f"🔧 Added PYTHONPATH to sys.path: {pythonpath}")
print(f"💡 PYTHONPATH: {os.environ.get('PYTHONPATH')}")
print(f"📁 sys.path = {sys.path}")

# ----------------------------------------
# Configure Logging
# ----------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("data-ingestor")

# ----------------------------------------
# Import diagnostics module or fallback
# ----------------------------------------
try:
    from diagnostic import init_diagnostics, startup_diagnostics, schedule_diagnostics_refresh
except ImportError as e:
    logger.warning(f"⚠️ Diagnostics module not found: {str(e)}")


    def init_diagnostics(app, service_name):
        pass


    def startup_diagnostics():
        pass

# ----------------------------------------
# Global Variables for Health Monitoring
# ----------------------------------------
PROCESS_START_TIME = time.time()
LAST_SUCCESSFUL_API_CALL = None
LAST_SUCCESSFUL_KAFKA_SEND = None
LAST_SUCCESSFUL_DATA_INGESTION = None
INGESTOR_HEALTH_STATUS = "starting"
API_HEALTH_STATUS = "unknown"
KAFKA_HEALTH_STATUS = "unknown"
TOTAL_BOXES_PROCESSED = 0
TOTAL_MESSAGES_SENT = 0
CONSECUTIVE_API_FAILURES = 0
CONSECUTIVE_KAFKA_FAILURES = 0
MAX_CONSECUTIVE_FAILURES = 5  # After this many failures, trigger recovery action
ERROR_STATS = {}
INGESTION_HISTORY = []  # Track last 100 ingestion cycles

# ----------------------------------------
# Global Variables for Configuration
# ----------------------------------------
POOL_SIZE = int(os.getenv("WORKER_POOL_SIZE", "8"))
TIME_WINDOW = int(os.getenv("INGESTION_INTERVAL", "10"))
SENSOR_DATA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "30"))
UNHEALTHY_THRESHOLD = int(os.getenv("UNHEALTHY_THRESHOLD", "300"))  # 5 minutes without data


# ----------------------------------------
# Kafka Producer Function
# ----------------------------------------
def get_producer_with_retry(bootstrap_servers, max_retries=5, delay=3):
    global KAFKA_HEALTH_STATUS

    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                # Add some configurations for better reliability
                retries=5,  # Retry sending on failure
                acks='all',  # Wait for all replicas to acknowledge
                request_timeout_ms=10000,  # Longer timeout
            )
            if producer.bootstrap_connected():
                logger.info(f"✅ Successfully connected to Kafka broker at {bootstrap_servers}")
                KAFKA_HEALTH_STATUS = "healthy"
                return producer
            else:
                logger.warning(f"⚠️ Failed to connect to Kafka broker at {bootstrap_servers}. Retrying...")
                KAFKA_HEALTH_STATUS = "degraded"
        except NoBrokersAvailable:
            logger.warning(
                f"⚠️ Kafka broker not available at {bootstrap_servers} (attempt {attempt + 1}/{max_retries})")
            KAFKA_HEALTH_STATUS = "degraded"
        except Exception as e:
            logger.error(f"❌ Unexpected error connecting to Kafka: {str(e)}")
            KAFKA_HEALTH_STATUS = "unhealthy"
        time.sleep(delay)
    logger.error(f"❌ Failed to connect to Kafka broker at {bootstrap_servers} after {max_retries} attempts")
    KAFKA_HEALTH_STATUS = "unhealthy"
    return None


# Initialize producer globally
logger.info(f"🚀 Initializing Kafka producer for {KAFKA_BOOTSTRAP_SERVER}...")
producer = get_producer_with_retry(KAFKA_BOOTSTRAP_SERVER)

# Global store for tracking sent messages
sent_timestamps = {}  # Format: {cache_key: (timestamp, time_when_sent)}

logger.info("🚀 DATA_INGESTOR PORT: %s", os.getenv("DATA_INGESTOR_PORT"))


# ----------------------------------------
# Health Check Functions
# ----------------------------------------
def check_api_health():
    """Check if the OpenSenseMap API is accessible"""
    global API_HEALTH_STATUS

    try:
        response = requests.get("https://api.opensensemap.org/stats", timeout=API_TIMEOUT)
        if response.status_code == 200:
            API_HEALTH_STATUS = "healthy"
            return True
        else:
            API_HEALTH_STATUS = "degraded"
            logger.warning(f"⚠️ API returned unexpected status code: {response.status_code}")
            return False
    except requests.exceptions.Timeout:
        API_HEALTH_STATUS = "degraded"
        logger.warning("⚠️ API request timed out")
        return False
    except requests.exceptions.RequestException as e:
        API_HEALTH_STATUS = "unhealthy"
        logger.error(f"❌ API request failed: {str(e)}")
        return False


def check_kafka_health():
    """Check if Kafka is accessible"""
    global KAFKA_HEALTH_STATUS

    if producer is None:
        KAFKA_HEALTH_STATUS = "unhealthy"
        return False

    try:
        # A simple metadata request to check connection
        producer.partitions_for(SENSOR_DATA_TOPIC)
        KAFKA_HEALTH_STATUS = "healthy"
        return True
    except Exception as e:
        KAFKA_HEALTH_STATUS = "degraded"
        logger.warning(f"⚠️ Kafka health check failed: {str(e)}")
        return False


def update_ingestor_health_status():
    """Update the overall health status of the ingestor"""
    global INGESTOR_HEALTH_STATUS

    current_time = time.time()

    # If we've never successfully ingested data, status depends on API and Kafka
    if LAST_SUCCESSFUL_DATA_INGESTION is None:
        if API_HEALTH_STATUS == "healthy" and KAFKA_HEALTH_STATUS == "healthy":
            INGESTOR_HEALTH_STATUS = "starting"
        else:
            INGESTOR_HEALTH_STATUS = "degraded"
        return

    # Check time since last successful ingestion
    time_since_last_ingestion = current_time - LAST_SUCCESSFUL_DATA_INGESTION

    if time_since_last_ingestion > UNHEALTHY_THRESHOLD:
        INGESTOR_HEALTH_STATUS = "unhealthy"
    elif time_since_last_ingestion > (UNHEALTHY_THRESHOLD / 2):
        INGESTOR_HEALTH_STATUS = "degraded"
    else:
        INGESTOR_HEALTH_STATUS = "healthy"


# ----------------------------------------
# Diagnostics Endpoint
# ----------------------------------------
def debug_env():
    env_keys = ["KAFKA_BOOTSTRAP_SERVER", "KAFKA_TOPIC", "DATA_INGESTOR_PORT"]
    safe_env = {k: os.getenv(k, "not_set") for k in env_keys}

    memory_info = psutil.Process(os.getpid()).memory_info()
    memory_usage_mb = round(memory_info.rss / 1024 / 1024, 2)

    disk = shutil.disk_usage("/app")
    disk_percent = round((disk.used / disk.total) * 100, 2)

    uptime_seconds = round(time.time() - PROCESS_START_TIME, 2)
    thread_count = threading.active_count()

    kafka_status = False
    kafka_topics = []
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")

    try:
        # Check bootstrap connection
        test_producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        kafka_status = test_producer.bootstrap_connected()
        test_producer.close()

        # List topics using KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="diagnostic")
        kafka_topics = sorted(admin.list_topics())
        admin.close()
    except Exception as e:
        kafka_topics = [f"Error: {str(e)}"]

    # Update health status before returning
    update_ingestor_health_status()

    last_api_time = "never" if LAST_SUCCESSFUL_API_CALL is None else datetime.fromtimestamp(
        LAST_SUCCESSFUL_API_CALL).isoformat()
    last_kafka_time = "never" if LAST_SUCCESSFUL_KAFKA_SEND is None else datetime.fromtimestamp(
        LAST_SUCCESSFUL_KAFKA_SEND).isoformat()
    last_ingestion_time = "never" if LAST_SUCCESSFUL_DATA_INGESTION is None else datetime.fromtimestamp(
        LAST_SUCCESSFUL_DATA_INGESTION).isoformat()

    return JSONResponse(content={
        "env": safe_env,
        "memory_usage_mb": memory_usage_mb,
        "disk_usage_percent": disk_percent,
        "active_thread_count": thread_count,
        "uptime_seconds": uptime_seconds,
        "kafka_bootstrap_connected": kafka_status,
        "kafka_topics": kafka_topics,
        "global_producer_initialized": producer is not None,
        "health": {
            "status": INGESTOR_HEALTH_STATUS,
            "api_status": API_HEALTH_STATUS,
            "kafka_status": KAFKA_HEALTH_STATUS,
            "last_successful_api_call": last_api_time,
            "last_successful_kafka_send": last_kafka_time,
            "last_successful_ingestion": last_ingestion_time,
        },
        "stats": {
            "total_boxes_processed": TOTAL_BOXES_PROCESSED,
            "total_messages_sent": TOTAL_MESSAGES_SENT,
            "consecutive_api_failures": CONSECUTIVE_API_FAILURES,
            "consecutive_kafka_failures": CONSECUTIVE_KAFKA_FAILURES,
            "error_stats": ERROR_STATS
        }
    })


# ----------------------------------------
# FastAPI and Lifespan
# ----------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    logger.info("🚀 Starting background ingestion task...")
    ingestion_task = asyncio.create_task(run_ingestion_loop())
    health_check_task = asyncio.create_task(run_health_check_loop())

    init_diagnostics(app, service_name="data-ingestor")

    try:
        logger.info("🟢 Ingestion loop task created")
        yield
    except Exception as e:
        logger.error(f"❌ Startup error: {str(e)}", exc_info=True)
        raise
    finally:
        logger.info("🛑 Shutting down background tasks...")
        ingestion_task.cancel()
        health_check_task.cancel()

        try:
            await ingestion_task
            await health_check_task
        except asyncio.CancelledError:
            logger.info("✅ Background tasks stopped cleanly.")

        # Close Kafka producer
        if producer:
            logger.info("🔌 Closing Kafka producer...")
            producer.close()


app = FastAPI()
app.router.lifespan_context = lifespan

# Attach tracing to this service
setup_tracer(app)


@app.get("/health")
def health_check():
    # Update health status first
    update_ingestor_health_status()

    # If unhealthy, return 503
    if INGESTOR_HEALTH_STATUS == "unhealthy":
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "detail": "Data ingestor is not ingesting data"
            }
        )

    # If degraded, return 200 but with warning
    if INGESTOR_HEALTH_STATUS == "degraded":
        return {
            "status": "degraded",
            "warning": "Data ingestor is experiencing issues"
        }

    # Otherwise return OK
    return {"status": "ok"}


@app.get("/producer/status")
def producer_status():
    return {
        "initialized": producer is not None,
        "broker": KAFKA_BOOTSTRAP_SERVER,
        "topic": SENSOR_DATA_TOPIC,
        "health_status": KAFKA_HEALTH_STATUS,
        "last_successful_send": datetime.fromtimestamp(
            LAST_SUCCESSFUL_KAFKA_SEND).isoformat() if LAST_SUCCESSFUL_KAFKA_SEND else None
    }


@app.post("/actions/reset-failures")
def reset_failure_counters():
    """Reset consecutive failure counters"""
    global CONSECUTIVE_API_FAILURES, CONSECUTIVE_KAFKA_FAILURES

    CONSECUTIVE_API_FAILURES = 0
    CONSECUTIVE_KAFKA_FAILURES = 0

    return {"status": "ok", "message": "Failure counters reset"}


@app.post("/actions/reconnect-kafka")
def reconnect_kafka():
    """Force reconnection to Kafka"""
    global producer

    # Close existing producer if any
    if producer:
        try:
            producer.close()
        except:
            pass

    # Create new producer
    producer = get_producer_with_retry(KAFKA_BOOTSTRAP_SERVER)

    return {
        "status": "ok" if producer is not None else "error",
        "message": "Kafka reconnected successfully" if producer is not None else "Failed to reconnect to Kafka",
        "kafka_status": KAFKA_HEALTH_STATUS
    }


app.add_api_route("/debug/env", debug_env, methods=["GET"])


@app.get("/debug/ingestion-history")
def get_ingestion_history():
    """Get recent ingestion history"""
    return {"history": INGESTION_HISTORY}


@app.on_event("startup")
def run_startup_diagnostics():
    logger.info("🚀 FastAPI app starting up...")
    startup_diagnostics()


# ----------------------------------------
# Health check loop
# ----------------------------------------
async def run_health_check_loop():
    """Background task that periodically checks health of components"""
    logger.info("🔄 Starting health check loop...")

    while True:
        try:
            # Check API health
            check_api_health()

            # Check Kafka health
            check_kafka_health()

            # Update overall health status
            update_ingestor_health_status()

            # Log current health status
            logger.info(
                f"🔍 Health status: Ingestor={INGESTOR_HEALTH_STATUS}, API={API_HEALTH_STATUS}, Kafka={KAFKA_HEALTH_STATUS}")

            # If unhealthy for too long, try recovery actions
            if INGESTOR_HEALTH_STATUS == "unhealthy":
                if KAFKA_HEALTH_STATUS != "healthy":
                    logger.warning("⚠️ Trying to reconnect to Kafka...")
                    reconnect_kafka()
        except Exception as e:
            logger.error(f"❌ Error in health check loop: {str(e)}", exc_info=True)

        # Sleep for a while before next check
        await asyncio.sleep(60)  # Check every minute


# ----------------------------------------
# Fetch updated sensor boxes with better error handling
# ----------------------------------------
def getUpdatedBox(updatedSince):
    global LAST_SUCCESSFUL_API_CALL, CONSECUTIVE_API_FAILURES, ERROR_STATS, API_HEALTH_STATUS

    logger.info(f"Getting updated boxes since: {updatedSince}...")

    try:
        # Add timeout to prevent hanging
        response = requests.get("https://api.opensensemap.org/boxes", timeout=API_TIMEOUT)

        if response.status_code != 200:
            CONSECUTIVE_API_FAILURES += 1
            API_HEALTH_STATUS = "degraded"
            error_type = f"API_STATUS_{response.status_code}"
            ERROR_STATS[error_type] = ERROR_STATS.get(error_type, 0) + 1
            logger.error(f"❌ API returned status code {response.status_code}")
            return []

        # Reset failure counter on success
        CONSECUTIVE_API_FAILURES = 0
        LAST_SUCCESSFUL_API_CALL = time.time()
        API_HEALTH_STATUS = "healthy"

        boxes_data = response.json()
        if not isinstance(boxes_data, list):
            logger.error(f"❌ API returned unexpected data type: {type(boxes_data)}")
            ERROR_STATS["API_INVALID_DATA"] = ERROR_STATS.get("API_INVALID_DATA", 0) + 1
            return []

        rtn = []
        for box in boxes_data:
            try:
                if "lastMeasurementAt" in box:
                    # Handle different timestamp formats with more robust parsing
                    try:
                        # Remove potentially problematic parts from timestamp
                        timestamp_str = box["lastMeasurementAt"].split('.')[0] + 'Z'
                        last_ts = int(time.mktime(time.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")))
                    except Exception as ts_error:
                        logger.warning(f"⚠️ Could not parse timestamp {box.get('lastMeasurementAt')}: {str(ts_error)}")
                        continue

                    if last_ts > int(updatedSince):
                        box_dict = {
                            "id": box["_id"],
                            "name": box.get("name", "Unknown"),
                            "exposure": box.get("exposure"),
                        }
                        coords = box.get("currentLocation", {}).get("coordinates", [])
                        if len(coords) >= 2:
                            box_dict["lat"] = coords[0]
                            box_dict["lon"] = coords[1]
                            if len(coords) == 3:
                                box_dict["height"] = coords[2]
                        rtn.append(box_dict)
            except Exception as e:
                ERROR_STATS["BOX_PARSING_ERROR"] = ERROR_STATS.get("BOX_PARSING_ERROR", 0) + 1
                logger.warning(f"⚠️ Error parsing box: {str(e)}")
                continue

        logger.info(f"✅ Boxes updated: {len(rtn)}")
        return rtn

    except requests.exceptions.Timeout:
        CONSECUTIVE_API_FAILURES += 1
        API_HEALTH_STATUS = "degraded"
        ERROR_STATS["API_TIMEOUT"] = ERROR_STATS.get("API_TIMEOUT", 0) + 1
        logger.error("❌ API request timed out")
        return []
    except requests.exceptions.RequestException as e:
        CONSECUTIVE_API_FAILURES += 1
        API_HEALTH_STATUS = "unhealthy"
        ERROR_STATS["API_CONNECTION_ERROR"] = ERROR_STATS.get("API_CONNECTION_ERROR", 0) + 1
        logger.error(f"❌ API request error: {str(e)}")
        return []
    except Exception as e:
        CONSECUTIVE_API_FAILURES += 1
        ERROR_STATS["UNEXPECTED_ERROR"] = ERROR_STATS.get("UNEXPECTED_ERROR", 0) + 1
        logger.error(f"❌ Unexpected error getting boxes: {str(e)}", exc_info=True)
        return []


# ----------------------------------------
# Send sensor values to Kafka with better error handling
# ----------------------------------------
def getLastMeasurement(box):
    global TOTAL_MESSAGES_SENT, LAST_SUCCESSFUL_KAFKA_SEND, CONSECUTIVE_KAFKA_FAILURES, ERROR_STATS

    try:
        logger.info(f"📊 Getting measurements for box: {box['id']} ({box['name']})")

        box_id = box["id"]
        url = f"https://api.opensensemap.org/boxes/{box_id}/sensors"

        try:
            response = requests.get(url, timeout=API_TIMEOUT)

            if response.status_code != 200:
                ERROR_STATS[f"SENSOR_API_STATUS_{response.status_code}"] = ERROR_STATS.get(
                    f"SENSOR_API_STATUS_{response.status_code}", 0) + 1
                logger.error(f"❌ API error for box {box_id}: {response.status_code}")
                return

            measurements = response.json()
        except requests.exceptions.Timeout:
            ERROR_STATS["SENSOR_API_TIMEOUT"] = ERROR_STATS.get("SENSOR_API_TIMEOUT", 0) + 1
            logger.error(f"❌ API timeout for box {box_id}")
            return
        except requests.exceptions.RequestException as e:
            ERROR_STATS["SENSOR_API_ERROR"] = ERROR_STATS.get("SENSOR_API_ERROR", 0) + 1
            logger.error(f"❌ API request error for box {box_id}: {str(e)}")
            return
        except json.JSONDecodeError:
            ERROR_STATS["SENSOR_JSON_ERROR"] = ERROR_STATS.get("SENSOR_JSON_ERROR", 0) + 1
            logger.error(f"❌ Invalid JSON from API for box {box_id}")
            return

        # Validate response structure
        if not isinstance(measurements, dict) or "sensors" not in measurements:
            ERROR_STATS["INVALID_SENSORS_RESPONSE"] = ERROR_STATS.get("INVALID_SENSORS_RESPONSE", 0) + 1
            logger.error(f"❌ Invalid response format for box {box_id}")
            return

        sensors = measurements.get("sensors", [])
        if not sensors:
            logger.info(f"ℹ️ No sensors found for box {box_id}")
            return

        logger.info(f"🔍 Found {len(sensors)} sensors")

        # Check if producer is initialized
        if producer is None:
            CONSECUTIVE_KAFKA_FAILURES += 1
            ERROR_STATS["KAFKA_NOT_INITIALIZED"] = ERROR_STATS.get("KAFKA_NOT_INITIALIZED", 0) + 1
            logger.error("❌ Kafka producer is not initialized")
            return

        messages_sent = 0
        current_time = time.time()

        # Clean up old entries in sent_timestamps - do this before processing sensors
        for k, (timestamp, sent_time) in list(sent_timestamps.items()):
            if current_time - sent_time > 3600:  # 1 hour TTL
                del sent_timestamps[k]

        for sensor in sensors:
            # Validate sensor structure
            if not isinstance(sensor, dict):
                ERROR_STATS["INVALID_SENSOR_FORMAT"] = ERROR_STATS.get("INVALID_SENSOR_FORMAT", 0) + 1
                continue

            last = sensor.get("lastMeasurement")
            if not last or not isinstance(last, dict) or "value" not in last:
                continue

            try:
                key = sensor.get("_id")
                if not key:
                    continue

                ts = last.get("createdAt")
                if not ts:
                    continue

                # Check if we've already processed this reading
                cache_key = f"{key}:{ts}"
                if cache_key in sent_timestamps:
                    cache_ts, cache_time = sent_timestamps[cache_key]
                    if current_time - cache_time < 3600:
                        logger.info(f"⏭️ Skipping duplicate for sensor {key}")
                        continue

                # Parse value with better error handling
                try:
                    value = float(last["value"])
                except (ValueError, TypeError):
                    ERROR_STATS["INVALID_VALUE_FORMAT"] = ERROR_STATS.get("INVALID_VALUE_FORMAT", 0) + 1
                    logger.warning(f"⚠️ Invalid value for sensor {key}: {last['value']}")
                    continue

                # Record that we're processing this reading
                sent_timestamps[cache_key] = (ts, current_time)

                # Build message
                message = {
                    "sensor_id": key,
                    "value": value,
                    "unit": sensor.get("unit", ""),
                    "timestamp": ts,
                    "location": {
                        "lat": box.get("lat"),
                        "lon": box.get("lon")
                    },
                    "box_id": box["id"],
                    "box_name": box.get("name", "Unknown"),
                    "exposure": box.get("exposure"),
                    "height": box.get("height"),
                    "sensor_type": sensor.get("sensorType", ""),
                    "phenomenon": sensor.get("title", ""),
                    "ingested_at": datetime.now().isoformat()
                }

                # Send to Kafka with better error handling
                try:
                    future = producer.send(SENSOR_DATA_TOPIC, message)
                    # Wait for the message to be delivered with a timeout
                    record_metadata = future.get(timeout=10)
                    messages_sent += 1
                    TOTAL_MESSAGES_SENT += 1
                    LAST_SUCCESSFUL_KAFKA_SEND = time.time()
                    CONSECUTIVE_KAFKA_FAILURES = 0  # Reset failure counter on success
                    logger.info(
                        f"📤 Sensor data sent to Kafka for sensor {key} (partition={record_metadata.partition}, offset={record_metadata.offset})")
                except Exception as e:
                    CONSECUTIVE_KAFKA_FAILURES += 1
                    ERROR_STATS["KAFKA_SEND_ERROR"] = ERROR_STATS.get("KAFKA_SEND_ERROR", 0) + 1
                    logger.error(f"❌ Error sending to Kafka: {str(e)}")
                    # If we've had too many consecutive failures, try reconnecting
                    if CONSECUTIVE_KAFKA_FAILURES >= MAX_CONSECUTIVE_FAILURES:
                        logger.warning("⚠️ Too many Kafka failures, attempting to reconnect...")
                        reconnect_kafka()
            except Exception as e:
                ERROR_STATS["SENSOR_PROCESSING_ERROR"] = ERROR_STATS.get("SENSOR_PROCESSING_ERROR", 0) + 1
                logger.error(f"❌ Error processing sensor: {str(e)}", exc_info=True)
    except Exception as e:
        ERROR_STATS["BOX_PROCESSING_ERROR"] = ERROR_STATS.get("BOX_PROCESSING_ERROR", 0) + 1
        logger.error(f"❌ Error processing box {box.get('id', 'unknown')}: {str(e)}", exc_info=True)

    if messages_sent > 0:
        logger.info(f"✅ Sent {messages_sent} messages for box {box['id']}")
    else:
        logger.info(f"ℹ️ No messages sent for box {box['id']}")


# ----------------------------------------
# Ingestion loop — background threadpool
# ----------------------------------------
executor = ThreadPoolExecutor(max_workers=POOL_SIZE)


async def run_ingestion_loop():
    global producer, TOTAL_BOXES_PROCESSED, LAST_SUCCESSFUL_DATA_INGESTION, INGESTOR_HEALTH_STATUS, INGESTION_HISTORY

    loop = asyncio.get_event_loop()
    cycle_count = 0

    logger.info("🔄 Starting ingestion loop...")

    while True:
        cycle_start_time = time.time()
        cycle_count += 1
        updated_since = cycle_start_time - TIME_WINDOW

        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(updated_since))
        logger.info(f"🔍 Cycle {cycle_count}: Checking for boxes updated since {formatted_time}")

        # Check if the producer is initialized before starting the cycle
        if producer is None:
            logger.error("❌ Kafka producer is not initialized. Attempting to reconnect...")
            producer = get_producer_with_retry(KAFKA_BOOTSTRAP_SERVER)
            if producer is None:
                logger.warning("⚠️ Still unable to connect to Kafka. Will retry in next cycle.")

                # Record this cycle in history
                cycle_info = {
                    "cycle": cycle_count,
                    "start_time": datetime.fromtimestamp(cycle_start_time).isoformat(),
                    "end_time": datetime.fromtimestamp(time.time()).isoformat(),
                    "status": "failed",
                    "reason": "kafka_connection_failed",
                    "boxes_processed": 0,
                    "messages_sent": 0
                }
                INGESTION_HISTORY.insert(0, cycle_info)
                if len(INGESTION_HISTORY) > 100:
                    INGESTION_HISTORY.pop()

                await asyncio.sleep(TIME_WINDOW)
                continue

        # Try to fetch updated boxes
        try:
            boxes = getUpdatedBox(updated_since)
            TOTAL_BOXES_PROCESSED += len(boxes)

            if not boxes:
                logger.info("ℹ️ No updated boxes found in this cycle")
            else:
                logger.info(f"🎯 Found {len(boxes)} updated boxes")

                # Process boxes with better error handling
                try:
                    tasks = [
                        loop.run_in_executor(executor, getLastMeasurement, box)
                        for box in boxes
                    ]
                    await asyncio.gather(*tasks)

                    # If we got this far, record successful ingestion
                    LAST_SUCCESSFUL_DATA_INGESTION = time.time()
                    INGESTOR_HEALTH_STATUS = "healthy"
                except Exception as e:
                    logger.error(f"❌ Error processing boxes: {str(e)}", exc_info=True)
        except Exception as e:
            logger.error(f"❌ Error in ingestion cycle: {str(e)}", exc_info=True)

        # Calculate how long this cycle took
        cycle_duration = time.time() - cycle_start_time

        # Record this cycle in history
        cycle_info = {
            "cycle": cycle_count,
            "start_time": datetime.fromtimestamp(cycle_start_time).isoformat(),
            "end_time": datetime.fromtimestamp(time.time()).isoformat(),
            "duration_seconds": round(cycle_duration, 2),
            "status": "success" if LAST_SUCCESSFUL_DATA_INGESTION and LAST_SUCCESSFUL_DATA_INGESTION >= cycle_start_time else "failed",
            "boxes_processed": len(boxes) if boxes else 0,
            "messages_sent": TOTAL_MESSAGES_SENT
        }
        INGESTION_HISTORY.insert(0, cycle_info)
        if len(INGESTION_HISTORY) > 100:
            INGESTION_HISTORY.pop()

        # Calculate sleep time (to maintain consistent cycle timing)
        elapsed = time.time() - cycle_start_time
        sleep_time = max(1, TIME_WINDOW - elapsed)  # At least 1 second

        logger.info(f"😴 Cycle {cycle_count} completed in {elapsed:.2f}s. Sleeping for {sleep_time:.2f} seconds...")
        await asyncio.sleep(sleep_time)


# For direct execution
if __name__ == "__main__":
    try:
        from diagnostic import store_diagnostics_snapshot

        store_diagnostics_snapshot()
    except Exception as e:
        logger.error(f"❌ Failed to run __main__ diagnostics snapshot: {str(e)}")

    # Start the FastAPI app
    import uvicorn

    port = int(os.getenv("DATA_INGESTOR_PORT", "8001"))
    logger.info(f"🚀 Starting data-ingestor on port {port}")
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)