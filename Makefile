
# ---------------------
# Docker Compose Targets
# ---------------------

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up --build -d

dev-up:
	docker compose -f docker-compose.yml -f docker-compose.override.yml up -d

dev-down:
	docker compose -f docker-compose.yml -f docker-compose.override.yml down

logs:
	docker compose logs -f

logs-ingestor:
	docker compose logs -f data-ingestor

logs-processor:
	docker compose logs -f data-processor

logs-gateway:
	docker compose logs -f api-gateway

schema-inspector-logs:
	docker compose logs -f schema-inspector

rebuild-service:
	docker compose build $$SERVICE

shell:
	docker exec -it $$SERVICE /bin/sh

status:
	docker ps --format "table {{.Names}}	{{.Status}}	{{.Ports}}"

build-base: ## Build the shared base Python image used by services
	@echo "🛠️  Building base image with fresh requirements..."
	docker build ${NO_CACHE:+--no-cache} -f base-images/Dockerfile.dev -t base-python-dev .

dev-reset: ## Tear down, rebuild base, and start full dev stack clean (nuclear reset option)
	docker compose -f docker-compose.yml -f docker-compose.override.yml down -v --remove-orphans
	make build-base
	make dev-up

# ---------------------
# Dev Setup
# ---------------------

install-deps: ## Install Python dependencies from requirements.txt
	pip install -r requirements.txt

# ---------------------
# Health Check Commands
# ---------------------

health: ## Check service health with retry, summary, and fail on error
	@max_retries=10; delay=2; all_healthy=1; \
	for service in "API Gateway|8000" "Data Ingestor|8001" "Data Processor|8002"; do \
		name=$${service%|*}; port=$${service#*|}; \
		echo "🔍 Checking $$name on port $$port..."; \
		i=1; success=0; \
		while [ $$i -le $$max_retries ]; do \
			status=$$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$$port/health); \
			if [ "$$status" = "200" ]; then \
				echo "✅ $$name is healthy (HTTP $$status)"; \
				success=1; break; \
			else \
				echo "⏳ Attempt $$i/$$max_retries: $$name not ready (HTTP $$status)..."; \
				sleep $$delay; i=$$((i+1)); \
			fi; \
		done; \
		if [ $$success -eq 0 ]; then \
			echo "❌ $$name FAILED after $$max_retries attempts (last status: $$status)"; \
			all_healthy=0; \
		fi; \
		echo ""; \
	done; \
	if [ $$all_healthy -eq 0 ]; then \
		echo "❗ One or more services failed health checks. Exiting with error."; \
		exit 1; \
	else \
		echo "🎉 All services passed health checks!"; \
	fi

# ---------------------
# Kafka Topic Utilities
# ---------------------

create-topic:
	python scripts/create_topic.py --broker=kafka:9092 --topic=iot.raw-data.opensensemap

delete-topic:
	python scripts/delete_topic.py --broker=kafka:9092 --topic=iot.raw-data.opensensemap

# ---------------------
# Compose Profiles
# ---------------------

test:
	docker compose --profile test up -d --build

prod:
	docker compose --profile prod up -d

# ---------------------
# CLI Help
# ---------------------

help: ## Show this help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
