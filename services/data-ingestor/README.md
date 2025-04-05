```markdown
# Data Ingestor

Polls OpenSenseMap API every N seconds (configurable via `INTERVAL_SECONDS`) and publishes raw JSON data to Kafka topic `sensor-data-dev`.

## Libraries Used
- `requests`
- `kafka-python`

## Environment Variables
- `KAFKA_BROKER`
- `KAFKA_TOPIC`
- `INTERVAL_SECONDS`
```
