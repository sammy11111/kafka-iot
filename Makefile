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

schema-inspector-logs:
	docker compose logs -f schema-inspector

rebuild-service:
	docker compose build $$SERVICE


# ---------------------
# Health Check Commands
# ---------------------

health:
	curl -s http://localhost:8000/health || echo "Gateway down"
	curl -s http://localhost:8001/health || echo "Ingestor down"
	curl -s http://localhost:8002/health || echo "Processor down"


# ---------------------
# Linting and Formatting
# ---------------------

lint:
	flake8 services libs

format:
	black services libs


# ---------------------
# Kafka Topic Utilities
# ---------------------

create-topic:
	python scripts/create_topic.py --broker=kafka:9092 --topic=iot.raw-data.opensensemap

delete-topic:
	python scripts/delete_topic.py --broker=kafka:9092 --topic=iot.raw-data.opensensemap

list-topics:
	python scripts/list_topics.py --broker=kafka:9092


# ---------------------
# Compose Profiles
# ---------------------

test:
	docker compose --profile test up -d --build

prod:
	docker compose --profile prod up -d

# ---------------------
# Dev Setup
# ---------------------

install-deps: ## Install Python dependencies from requirements.txt
	pip install -r requirements.txt

# ---------------------
# CLI Help
# ---------------------

help: ## Show this help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
