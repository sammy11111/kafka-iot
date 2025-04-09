.PHONY: nuke smoketest up down restart dev-up dev-down logs logs-ingestor logs-processor logs-gateway schema-inspector-logs rebuild-service shell status build-base dev-reset install-deps create-topic delete-topic test prod build-no-volumes help

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
	docker compose -f docker-compose.yml -f docker-compose.override.yml up --build --force-recreate

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

prune: ## Remove dangling images, stopped containers, unused networks
	@echo "🧹 Pruning Docker system..."
	docker system prune -f

prune-all: ## Full system prune including volumes (⚠️ destructive!)
	@echo "🔥 Pruning ALL Docker system data, including volumes..."
	docker system prune -a --volumes -f

schema-inspector-logs:
	docker compose logs -f schema-inspector

rebuild-service:
	docker compose build $$SERVICE

shell:
	docker exec -it $$SERVICE /bin/sh

status:
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

build-base: ## Build the shared base Python image used by services
	@echo "🛠️  Building base image with fresh requirements..."
	docker build -f base-images/Dockerfile.dev -t base-python-dev .

dev-reset: ## Tear down, rebuild base, and start full dev stack clean (nuclear reset option)
	docker compose -f docker-compose.yml -f docker-compose.override.yml down -v --remove-orphans
	make build-base
	docker compose -f docker-compose.yml -f docker-compose.override.yml build ${NO_CACHE:+--no-cache}
	@echo "💡 Tip: If issues persist, try 'make prune' to clear dangling images and cache."

nuke: ## 🔥 Full system reset, prune, rebuild, and restart
	@echo "🔥 Shutting everything down and pruning..."
	docker compose down -v --remove-orphans
	docker system prune -a --volumes -f
	@echo "🛠️ Rebuilding base image..."
	make build-base
	@echo "🚀 Building all services..."
	docker compose build --no-cache
	docker compose up -d

# ---------------------
# Dev Setup
# ---------------------

install-deps: ## Install Python dependencies from requirements.txt
	pip install -r requirements.txt

# ---------------------
# Health Check Commands
# ---------------------
smoketest: ## Run full microservice smoke test
	@chmod +x scripts/smoketest.sh
	@scripts/smoketest.sh

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

build-no-volumes: ## Build all services without dev volume mounts
	docker compose -f docker-compose.yml up --build -d
