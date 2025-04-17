# Kafka IoT Pipeline

A containerized microservice-based pipeline for real-time sensor ingestion, processing, and storage using Kafka, FastAPI, and MongoDB. Built for modular development, data validation, and scalable architecture.

---

## 🚀 Developer Onboarding

### One-Step Setup

```bash
python onboard.py
```

This script:
- Copies `.env.template` to `.env` if missing
- Starts all services using `make dev-up`

After setup, populate `.env` with local overrides if necessary.

---

## 🧱 Microservices Overview

| Service           | Port Env Var             | Description                                      |
|-------------------|---------------------------|--------------------------------------------------|
| data-ingestor     | `DATA_INGESTOR_PORT`      | Polls OpenSenseMap and publishes to Kafka        |
| data-processor    | `DATA_PROCESSOR_PORT`     | Consumes from Kafka and validates schema         |
| data-aggregator   | `DATA_AGGREGATOR_PORT`    | Prepares data for visualization (future)         |
| mongodb           | `MONGO_PORT`              | Document-based storage                           |
| kafka-ui          | `KAFKA_UI_PORT`           | Kafka topic inspection UI                        |

---

## 🛠️ Makefile Commands

```bash
make up                     # Start base infrastructure services
make dev-up                 # Start full stack with dev override
make down                   # Stop containers
make dev-down               # Stop dev profile services
make restart                # Rebuild and restart
make logs                  # View all logs
make logs-ingestor          # Logs for data-ingestor
make logs-processor         # Logs for data-processor
make logs-gateway           # Logs for API gateway
make schema-inspector-logs  # Logs for schema-inspector
make rebuild-service SERVICE=name  # Rebuild specific service
make shell SERVICE=name     # Open shell in container
make status                 # List running containers with port mappings
make build-base             # Build the shared Python base image
make build-data-processor   # Build just the data-processor image
make install-deps           # Install Python requirements
make dev-reset              # Clean rebuild with override
make prune                  # Remove unused Docker objects (safe)
make prune-all              # Full Docker system prune (destructive)
make nuke                   # Nuclear reset: teardown, prune, rebuild, restart
make smoketest              # Run microservice connectivity/health check
make create-topic           # Kafka topic creation script
make delete-topic           # Kafka topic deletion script
make build-no-volumes       # Build without dev volumes
make help                   # Show this help menu
```

---

## 🧪 Health Checks

```bash
make health
```

Runs GET requests against:

- `http://localhost:$DATA_INGESTOR_PORT/health`
- `http://localhost:$DATA_PROCESSOR_PORT/health`

---

## ⚙️ Dev Setup

```bash
make install-deps    # Install shared Python packages
make build-base      # Build shared base image for Python microservices
```

---

## 📁 Folder Structure

```
services/
  data-ingestor/
  data-processor/
  data-aggregator/
libs/
  kafka_utils.py
  message_schemas.py
base-images/
  Dockerfile.dev
.devcontainer/
  devcontainer.json
scripts/
  smoketest.sh
  create_topic.py
  delete_topic.py
tests/
  data-ingestor/
  data-processor/
```

---

## ⚙️ Port Management

All ports are declared in `.env` and injected into `docker-compose`:

```Dockerfile
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port $PORT --reload"]
```

Avoid hardcoded ports and ensure each `docker-compose.yml` service has:

```yaml
env_file: .env
environment:
  - PORT=...
```

---

## 📜 License

MIT License (see `LICENSE` file)
---

## Monitoring Dashboards

- Kafka UI: [http://localhost:$KAFKA_UI_PORT](http://localhost:$KAFKA_UI_PORT)
  Use this interface to inspect Kafka topics, view messages, and monitor broker state.
