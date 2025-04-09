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
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor

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
# Load environment variables from root .env
# ----------------------------------------
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path=dotenv_path)

# ----------------------------------------
# Import diagnostics module or fallback
# ----------------------------------------
try:
    from diagnostic import init_diagnostics, startup_diagnostics, schedule_diagnostics_refresh
except ImportError as e:
    print(f"⚠️ Diagnostics module not found: {str(e)}")
    def init_diagnostics(app, service_name): pass
    def startup_diagnostics(): pass

# ----------------------------------------
# Diagnostics Endpoint
# ----------------------------------------
PROCESS_START_TIME = time.time()

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
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        kafka_status = producer.bootstrap_connected()
        producer.close()

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
        "kafka_topics": kafka_topics
    })

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

# ----------------------------------------
# FastAPI app and diagnostics
# ----------------------------------------
app = FastAPI()
app.router.lifespan_context = lifespan

@app.get("/health")
def health_check():
    return {"status": "ok"}

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
# Ingestion loop configuration
# ----------------------------------------
POOL_SIZE = 8
TIME_WINDOW = 60
SENSOR_DATA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")

print("🚀 DATA_INGESTOR PORT:", os.getenv("DATA_INGESTOR_PORT"))

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
def getLastMeasurement(box, max_retries=5, backoff_seconds=2):
    measurements = requests.get(
        f"https://api.opensensemap.org/boxes/{box['id']}/sensors"
    )
    producer = None
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            if producer.bootstrap_connected():
                break
            else:
                print(f"❌ Attempt {attempt}: Producer not connected to Kafka")
        except NoBrokersAvailable as e:
            print(f"❌ Attempt {attempt}: No brokers available - {str(e)}")
        except Exception as e:
            print(f"❌ Attempt {attempt}: Unexpected error - {str(e)}")
        time.sleep(backoff_seconds * attempt)
    else:
        print("❌ All attempts to connect to Kafka failed.")
        return

    try:
        for sensor in measurements.json().get("sensors", []):
            last = sensor.get("lastMeasurement")
            if last and "value" in last:
                message = {
                    "boxId": box["id"],
                    "boxName": box["name"],
                    "exposure": box["exposure"],
                    "lat": box["lat"],
                    "lon": box["lon"],
                    "height": box.get("height"),
                    "sensorId": sensor["_id"],
                    "sensorType": sensor.get("sensorType"),
                    "phenomenon": sensor.get("title"),
                    "value": last["value"],
                    "unit": sensor.get("unit"),
                    "createdAt": last["createdAt"],
                }
                try:
                    producer.send(SENSOR_DATA_TOPIC, message)
                    print("📤 Sensor data sent to Kafka")
                except Exception as e:
                    print("❌ Error sending to Kafka:", str(e))
        producer.flush()
    except Exception as e:
        print("❌ Error processing measurements:", str(e))

# ----------------------------------------
# Ingestion loop — background threadpool
# ----------------------------------------
executor = ThreadPoolExecutor(max_workers=POOL_SIZE)

async def run_ingestion_loop():
    loop = asyncio.get_event_loop()
    while True:
        updated_since = time.time() - TIME_WINDOW
        boxes = getUpdatedBox(updated_since)
        tasks = [
            loop.run_in_executor(executor, getLastMeasurement, box)
            for box in boxes
        ]
        await asyncio.gather(*tasks)
        await asyncio.sleep(TIME_WINDOW)

def get_producer_with_retry(max_retries=5, delay=3):
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            if producer.bootstrap_connected():
                return producer
            else:
                print("Kafka bootstrap not connected. Retrying...")
        except NoBrokersAvailable:
            print(f"Kafka broker not available (attempt {attempt+1}/{max_retries})")
        time.sleep(delay)
    raise RuntimeError("Kafka broker unavailable after retries.")

if __name__ == "__main__":
    try:
        from diagnostic import store_diagnostics_snapshot
        store_diagnostics_snapshot()
    except Exception as e:
        print(f"❌ Failed to run __main__ diagnostics snapshot: {e}")
