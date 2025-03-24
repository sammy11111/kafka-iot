from pydantic import BaseModel

class SensorData(BaseModel):
    device_id: str
    timestamp: str
    temperature: float
    humidity: float
