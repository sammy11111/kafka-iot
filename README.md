# Kafka IoT Pipeline

A containerized, microservice-based pipeline for real-time sensor ingestion, processing, and visualization using Kafka, FastAPI, MongoDB, and Grafana.

---

## Developer Onboarding

### One-Step Setup
```bash
python onboard.py
```
This will copy `.env.template` to `.env` if not already created and start all services using `make up`.
Once the .env file is created, populate the environmental variables with appropriate values.

---

## Microservices Overview

| Service         | Port | Description                                     |
|----------------|------|-------------------------------------------------|
| api-gateway     | 8000 | Entry point for routing & orchestration         |
| data-ingestor   | 8001 | Polls OpenSenseMap, sends raw data to Kafka     |
| data-processor  | 8002 | Consumes from Kafka, validates & stores to DB   |
| data-aggregator | 8003 | Future: aggregate for visualization             |
| mongodb         | 27017| Stores structured sensor data                   |
| grafana         | 3000 | Dashboards via MongoDB plugin                   |
| prometheus      | 9090 | Metrics scraping                                |
| kafka-ui        | 8080 | Inspect Kafka topics                            |


---

## Makefile Commands

```bash
make up                         # Start services
make dev-up                     # Start all services using docker-compose.yml + docker-compose.override.yml (full dev stack)
make down                       # Stop services
make restart                    # Restart all with rebuild
make logs SERVICE=x             # Tail logs for a specific service
make rebuild-service SERVICE=x  # Rebuild one service
make health                     # Ping /health endpoint on all services
make lint                       # Run flake8 on services and libs
make format                     # Run black formatter on services and libs
make create-topic               # Create the OpenSenseMap topic in Kafka
make delete-topic               # Delete the OpenSenseMap topic from Kafka
make list-topics                # List all Kafka topics
make install-deps               # Install Python packages from requirements.txt
make help                       # List all available Makefile tasks
```

---

## Kafka Topic Management (Python CLI Tools)

All scripts are located in `scripts/` and are cross-platform Python tools.

### Create a Topic
```bash
make create-topic
# or manually
python scripts/create_topic.py --broker kafka:9092 --topic iot.raw-data.opensensemap
```

### Delete a Topic
```bash
make delete-topic
# or manually
python scripts/delete_topic.py --broker kafka:9092 --topic iot.raw-data.opensensemap
```

### List Topics
```bash
make list-topics
# or manually
python scripts/list_topics.py --broker kafka:9092
```

# Register schema
python scripts/kafka_tools.py schema register --subject my-subject --file path/to/schema.json
```

These scripts use the `kafka-python` library and will work on macOS, Windows, and Linux.
## Running Tests

To test FastAPI `/health` endpoints:

pytest tests/data-ingestor/test_health.py
pytest tests/data-processor/test_health.py
pytest tests/api-gateway/test_health.py
```

You can run all with:

```bash
pytest tests/
```

---

## VSCode Development

1. Open the project folder in VSCode
2. Install the Python and Docker extensions
3. Your `.vscode/settings.json` and `launch.json` are pre-configured
4. Press F5 to run a selected microservice using hot reload

---

## PyCharm Development

1. Set the project interpreter and working directory to the appropriate service
2. Use `.env` for environment variables
3. Add `libs/` to your interpreter paths for shared modules

---

## Devcontainer Support

If you're using VSCode Remote Containers:

1. Open the folder in VSCode
2. Reopen in Container when prompted
3. All dependencies will auto-install

---

## Monitoring Dashboards

- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Kafka UI: http://localhost:8080

---

## Folder Structure

```
services/
  api-gateway/
  data-ingestor/
  data-processor/
  data-aggregator/
libs/
  kafka_utils.py
  message_schemas.py
base-images/
  Dockerfile.dev
.vscode/
  settings.json
  launch.json
.devcontainer/
  devcontainer.json
tests/
  data-ingestor/
  data-processor/
  api-gateway/
```

---

## License

MIT License (add LICENSE file)

## Kafka CLI Tools (kafka-tools.py)

A unified Python CLI tool for managing Kafka topics and JSON schemas.

### Topic Commands

Create a topic:
```bash
python scripts/kafka_tools.py topic create --topic my.topic --broker kafka:9092
```

Delete a topic:
```bash
python scripts/kafka_tools.py topic delete --topic my.topic --broker kafka:9092
```

List topics:
```bash
python scripts/kafka_tools.py topic list --broker kafka:9092
```

### Schema Commands

Register a schema:
```bash
python scripts/kafka_tools.py schema register --subject my.topic --file schema.json
```

Get the latest schema for a subject:
```bash
python scripts/kafka_tools.py schema get --subject my.topic
```

List all schema subjects:
```bash
python scripts/kafka_tools.py schema list
```

The default schema registry URL is `http://localhost:8081`, but you can override it using `--registry-url`.

## Schema Inspector Dashboard

The `schema-inspector` service provides a developer-friendly dashboard to view recent schema validation errors.

### Endpoint

```http
GET /schema-errors?limit=10
```

### Example Usage

View the 5 most recent schema validation errors:

```bash
curl http://localhost:8004/schema-errors?limit=5
```

### Run Logs for Schema Inspector

```bash
make schema-inspector-logs
```

### Planned Enhancements

- Pagination and timestamp filters (e.g., `?after=...&before=...`)
- Front-end dashboard for viewing, searching, and exporting error logs
