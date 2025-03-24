from fastapi import APIRouter, HTTPException
from app.schemas.sensor import SensorData
from app.services.kafka_producer import produce_to_kafka

sensor_router = APIRouter()

@sensor_router.post("/")
async def post_sensor_data(sensor_data: SensorData):
    try:
        produce_to_kafka(sensor_data.dict())
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
