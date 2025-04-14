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

from libs.env_loader import PROJECT_ROOT # do not remove

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
# Import diagnostics module or fallback
# ----------------------------------------
try:
    from diagnostic import init_diagnostics, startup_diagnostics, schedule_diagnostics_refresh
except ImportError as e:
    print(f"⚠️ Diagnostics module not found: {str(e)}")


    def init_diagnostics(app, service_name):
        pass


    def startup_diagnostics():
        pass


# ----------------------------------------
# Kafka Producer Function
# ----------------------------------------
def get_producer_with_retry(bootstrap_servers, max_retries=5, delay=3):
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            if producer.bootstrap_connected():
                print(f"✅ Successfully connected to Kafka broker at {bootstrap_servers}")
                return producer
            else:
                print(f"⚠️ Failed to connect to Kafka broker at {bootstrap_servers}. Retrying...")
        except NoBrokersAvailable:
            print(f"⚠️ Kafka broker not available at {bootstrap_servers} (attempt {attempt + 1}/{max_retries})")
        except Exception as e:
            print(f"⚠️ Unexpected error connecting to Kafka: {str(e)}")
        time.sleep(delay)
    print(f"❌ Failed to connect to Kafka broker at {bootstrap_servers} after {max_retries} attempts")
    return None


# ----------------------------------------
# Global Variables
# ----------------------------------------
PROCESS_START_TIME = time.time()
POOL_SIZE = 8
TIME_WINDOW = 10
SENSOR_DATA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")

# Initialize producer globally
print(f"🚀 Initializing Kafka producer for {KAFKA_BOOTSTRAP_SERVER}...")
producer = get_producer_with_retry(KAFKA_BOOTSTRAP_SERVER)

# Global store for tracking sent messages
sent_timestamps = {}  # Format: {cache_key: (timestamp, time_when_sent)}

print("🚀 DATA_INGESTOR PORT:", os.getenv("DATA_INGESTOR_PORT"))


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

    return JSONResponse(content={
        "env": safe_env,
        "memory_usage_mb": memory_usage_mb,
        "disk_usage_percent": disk_percent,
        "active_thread_count": thread_count,
        "uptime_seconds": uptime_seconds,
        "kafka_bootstrap_connected": kafka_status,
        "kafka_topics": kafka_topics,
        "global_producer_initialized": producer is not None
    })


# ----------------------------------------
# FastAPI and Lifespan
# ----------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting background ingestion task...")
    ingestion_task = asyncio.create_task(run_ingestion_loop())

    init_diagnostics(app, service_name="data-ingestor")

    try:
        print("🟢 Ingestion loop task created")
        yield
    except Exception as e:
        print("❌ Startup error:", str(e))
        raise
    finally:
        print("🛑 Shutting down background tasks...")
        ingestion_task.cancel()
        try:
            await ingestion_task
        except asyncio.CancelledError:
            print("✅ Ingestion loop stopped cleanly.")


app = FastAPI()
app.router.lifespan_context = lifespan

# Attach tracing to this service
setup_tracer(app)

@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/producer/status")
def producer_status():
    return {
        "initialized": producer is not None,
        "broker": KAFKA_BOOTSTRAP_SERVER,
        "topic": SENSOR_DATA_TOPIC
    }


app.add_api_route("/debug/env", debug_env, methods=["GET"])


@app.on_event("startup")
def run_startup_diagnostics():
    logging.info("🚀 FastAPI app starting up...")
    startup_diagnostics()


# ----------------------------------------
# Configure Logging
# ----------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ----------------------------------------
# Fetch updated sensor boxes
# ----------------------------------------
def getUpdatedBox(updatedSince):
    print("Getting updated boxes since: " + str(updatedSince) + "...")
    boxes = requests.get("https://api.opensensemap.org/boxes")
    rtn = []
    for box in boxes.json():
        try:
            if "lastMeasurementAt" in box:
                last_ts = int(
                    time.mktime(
                        time.strptime(box["lastMeasurementAt"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    )
                )
                if last_ts > int(updatedSince):
                    box_dict = {
                        "id": box["_id"],
                        "name": box["name"],
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
            print(f"Error parsing box: {e}")
    print(f"Boxes updated: {len(rtn)}")
    return rtn


# ----------------------------------------
# Send sensor values to Kafka
# ----------------------------------------
def getLastMeasurement(box):
    try:
        print(f"📊 Getting measurements for box: {box['id']} ({box['name']})")

        box_id = box["id"]
        url = f"https://api.opensensemap.org/boxes/{box_id}/sensors"
        response = requests.get(url, timeout=10)

        if response.status_code != 200:
            print(f"❌ API error for box {box_id}: {response.status_code}")
            return

        measurements = response.json()

        if not measurements.get("sensors"):
            print(f"ℹ️ No sensors found for box {box_id}")
            return

        print(f"🔍 Found {len(measurements.get('sensors', []))} sensors")

        # Check if producer is initialized
        if producer is None:
            print("❌ Kafka producer is not initialized")
            return

        messages_sent = 0
        for sensor in measurements.get("sensors", []):
            last = sensor.get("lastMeasurement")
            if not last or "value" not in last:
                continue

            try:
                value = float(last["value"])
            except (ValueError, TypeError):
                print(f"⚠️ Invalid value for sensor {sensor.get('_id')}")
                continue

            key = sensor["_id"]
            ts = last["createdAt"]

            current_time = time.time()

            # Clean up old entries in sent_timestamps
            if current_time % 3600 < 10:
                for k, (timestamp, sent_time) in list(sent_timestamps.items()):
                    if current_time - sent_time > 3600:
                        del sent_timestamps[k]

            cache_key = f"{key}:{ts}"
            if cache_key in sent_timestamps:
                cache_ts, cache_time = sent_timestamps[cache_key]
                if current_time - cache_time < 3600:
                    print(f"⏭️ Skipping duplicate for sensor {key}")
                    continue

            sent_timestamps[cache_key] = (ts, current_time)

            message = {
                "sensor_id": sensor["_id"],
                "value": value,
                "unit": sensor.get("unit", ""),
                "timestamp": ts,
                "location": {
                    "lat": box["lat"],
                    "lon": box["lon"]
                },
                "box_id": box["id"],
                "box_name": box["name"],
                "exposure": box["exposure"],
                "height": box.get("height"),
                "sensor_type": sensor.get("sensorType", ""),
                "phenomenon": sensor.get("title", "")
            }

            try:
                producer.send(SENSOR_DATA_TOPIC, message)
                messages_sent += 1
                print(f"📤 Sensor data sent to Kafka for sensor {key}")
            except Exception as e:
                print(f"❌ Error sending to Kafka: {str(e)}")
    except Exception as e:
        print(f"❌ Error processing box {box.get('id', 'unknown')}: {str(e)}")

    if messages_sent > 0:
        print(f"✅ Sent {messages_sent} messages for box {box['id']}")
    else:
        print(f"ℹ️ No messages sent for box {box['id']}")


# ----------------------------------------
# Ingestion loop — background threadpool
# ----------------------------------------
executor = ThreadPoolExecutor(max_workers=POOL_SIZE)


# Fix the run_ingestion_loop function to properly use the global keyword

async def run_ingestion_loop():
    global producer  # Declare global at the beginning of the function
    loop = asyncio.get_event_loop()
    cycle_count = 0

    print("🔄 Starting ingestion loop...")

    while True:
        cycle_count += 1
        current_time = time.time()
        updated_since = current_time - TIME_WINDOW

        formatted_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(updated_since))
        print(f"🔍 Cycle {cycle_count}: Checking for boxes updated since {formatted_time}")

        # Check if the producer is initialized before starting the cycle
        if producer is None:
            print("❌ Kafka producer is not initialized. Attempting to reconnect...")
            producer = get_producer_with_retry(KAFKA_BOOTSTRAP_SERVER)
            if producer is None:
                print("⚠️ Still unable to connect to Kafka. Will retry in next cycle.")
                await asyncio.sleep(TIME_WINDOW)
                continue

        boxes = getUpdatedBox(updated_since)

        if not boxes:
            print("ℹ️ No updated boxes found in this cycle")
        else:
            print(f"🎯 Found {len(boxes)} updated boxes")
            tasks = [
                loop.run_in_executor(executor, getLastMeasurement, box)
                for box in boxes
            ]
            await asyncio.gather(*tasks)

        print(f"😴 Sleeping for {TIME_WINDOW} seconds...")
        await asyncio.sleep(TIME_WINDOW)

if __name__ == "__main__":
    try:
        from diagnostic import store_diagnostics_snapshot

        store_diagnostics_snapshot()
    except Exception as e:
        print(f"❌ Failed to run __main__ diagnostics snapshot: {e}")