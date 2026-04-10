.PHONY: up down verify scrape lint fmt

# Start all infrastructure services (MinIO, Nessie) in background
up:
	docker compose up -d

# Stop all infrastructure services
down:
	docker compose down

# Verify that all infrastructure services are healthy and reachable
verify:
	python3 scripts/verify.py

# Initialize Iceberg catalog: create namespaces, tables, seed sources
init:
	python3 scripts/init_catalog.py

# Run Telegram extractor — fetch new messages into Bronze layer
scrape:
	python3 extractors/tg.py scrape

# Check code style with ruff
lint:
	ruff check .

# Auto-format code with ruff
fmt:
	ruff format .
