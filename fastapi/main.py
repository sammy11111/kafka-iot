from fastapi import FastAPI
from app.routes.sensor import sensor_router

app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Kafka IoT API is running"}

app.include_router(sensor_router, prefix="/sensor")
