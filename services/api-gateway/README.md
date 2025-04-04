```markdown
# FastAPI API Gateway

Exposes HTTP endpoints to interact with the IoT data pipeline. Accepts input, validates payloads, and sends to Kafka.

## Endpoints
- `POST /sensor/` — Accept sensor data and forward to Kafka
- `GET /` — Health check

## Libraries Used
- `fastapi`
- `uvicorn`
- `pydantic`
```