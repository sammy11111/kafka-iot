import os
import logging
from fastapi import FastAPI
from routes.sensor import sensor_router

# Logging config
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get Kafka topic from environment or use default
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "iot.raw-data.opensensemap")
logger.info(f"[Startup] Using Kafka topic: {KAFKA_TOPIC}")

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Kafka IoT API is running"}

app.include_router(sensor_router, prefix="/sensor")