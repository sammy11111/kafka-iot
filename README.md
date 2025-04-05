# Kafka IoT Pipeline

A containerized, microservice-based pipeline for real-time sensor ingestion, processing, and visualization using Kafka, FastAPI, MongoDB, and Grafana.

---

## Developer Onboarding

### One-Step Setup

```bash
python onboard.py
```

This will copy `.env.template` to `.env` if not already created and start all services using `make up`.
Once the `.env` file is created, populate the environmental variables with appropriate values.

---

## Microservices Overview

| Service           | Port Env Var            | Description                                     |
|------------------|--------------------------|-------------------------------------------------|
| api-gateway       | `API_GATEWAY_PORT`       | Entry point for routing & orchestration         |
| data-ingestor     | `DATA_INGESTOR_PORT`     | Polls OpenSenseMap, sends raw data to Kafka     |
| data-processor    | `DATA_PROCESSOR_PORT`    | Consumes from Kafka, validates & stores to DB   |
| data-aggregator   | `DATA_AGGREGATOR_PORT`   | Future: aggregate for visualization             |
| schema-inspector  | `SCHEMA_INSPECTOR_PORT`  | Views recent schema validation errors           |
| mongodb           | `MONGO_PORT`             | Stores structured sensor data                   |
| grafana           | `GRAFANA_PORT`           | Dashboards via MongoDB plugin                   |
| prometheus        | `PROMETHEUS_PORT`        | Metrics scraping                                |
| kafka-ui          | `KAFKA_UI_PORT`          | Inspect Kafka topics                            |

---

## Makefile Commands

```bash
make up                         # Start services
make dev-up                     # Start all services using docker-compose.yml + docker-compose.override.yml
make down                       # Stop services
make restart                    # Restart all with rebuild
make logs SERVICE=x             # Tail logs for a specific service
make logs-ingestor              # Tail logs for data-ingestor
make logs-processor             # Tail logs for data-processor
make logs-gateway               # Tail logs for api-gateway
make schema-inspector-logs     # Tail logs for schema-inspector
make rebuild-service SERVICE=x  # Rebuild one service
make shell SERVICE=x            # Enter a shell in the running container
make status                     # Show running containers and mapped ports
make health                     # Ping /health endpoint on all services
make install-deps               # Install Python packages from requirements.txt
make build-base                 # Build the base Python image used by services
make dev-reset                  # Rebuild base and reset full dev environment
make help                       # List all available Makefile tasks
```

---

## Docker Compose Targets

- `make up`: Run core services
- `make dev-up`: Run all services in dev mode
- `make down`: Stop all running containers
- `make restart`: Stop and restart everything with rebuild
- `make status`: View currently running containers and their port mappings

---

## Health Check Commands

```bash
make health
```

Pings:

- `http://localhost:$API_GATEWAY_PORT/health`
- `http://localhost:$DATA_INGESTOR_PORT/health`
- `http://localhost:$DATA_PROCESSOR_PORT/health`

---

## Compose Profiles

```bash
make test     # Compose profile for tests
make prod     # Compose profile for production
```

---

## Dev Setup

```bash
make install-deps    # Installs Python requirements
make build-base      # Builds shared Python base image
```

---

## Monitoring Dashboards

- Grafana: [http://localhost:$GRAFANA_PORT](http://localhost:$GRAFANA_PORT) (admin/admin)
- Prometheus: [http://localhost:$PROMETHEUS_PORT](http://localhost:$PROMETHEUS_PORT)
- Kafka UI: [http://localhost:$KAFKA_UI_PORT](http://localhost:$KAFKA_UI_PORT)

---

## Folder Structure

```
services/
  api-gateway/
  data-ingestor/
  data-processor/
  data-aggregator/
  schema-inspector/
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

---

## Schema Inspector Dashboard

The `schema-inspector` service offers a web UI for recent schema validation errors.

### Endpoint

```
GET /schema-errors?limit=10
```

### Example Usage

```bash
curl http://localhost:$SCHEMA_INSPECTOR_PORT/schema-errors?limit=5
```

### Logs

```bash
make schema-inspector-logs
```

### Planned Features

- Pagination, timestamp filters
- Web dashboard for searching/exporting error logs

---

## Notes

- All FastAPI ports are now controlled via the `.env` file (e.g., `DATA_PROCESSOR_PORT`, `SCHEMA_INSPECTOR_PORT`) and injected using `docker-compose`.
- No ports are hardcoded in Dockerfiles. Each microservice uses:
  ```Dockerfile
  CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT --reload"]
  ```
- Ensure `env_file: .env` and `environment: - PORT=...` are configured in `docker-compose.yml` for each service.