import os
import sys
import time
import json
from kafka import KafkaProducer
from dotenv import load_dotenv
import requests
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
from kafka.errors import NoBrokersAvailable
from concurrent.futures import ThreadPoolExecutor

# ----------------------------------------
# FastAPI app and lifespan manager
# ----------------------------------------
app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "ok"}

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

# ----------------------------------------
# Ingestion loop configuration
# ----------------------------------------
POOL_SIZE = 8  # Number of concurrent OpenSenseMap API requests
TIME_WINDOW = 60  # Fetch boxes updated in the last 60 seconds
SENSOR_DATA_TOPIC = os.environ.get("KAFKA_TOPIC", "iot.raw-data.opensensemap")
KAFKA_BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP_SERVER", "kafka:29092")

print("🚀 DATA_INGESTOR PORT:", os.environ.get("DATA_INGESTOR_PORT"))

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🚀 Starting background ingestion task...")
    try:
        task = asyncio.create_task(run_ingestion_loop())
        print("🟢 Ingestion loop task created")
        yield
    except Exception as e:
        print("❌ Startup error:", str(e))
        raise
    finally:
        print("🛑 Shutting down background ingestion task...")
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            print("✅ Ingestion loop stopped cleanly.")


app.router.lifespan_context = lifespan

# ----------------------------------------
# Fetch sensor boxes updated since the given timestamp
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
# Fetch latest sensor values for a box and send to Kafka
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
# Ingestion loop — runs in the background using threads
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