.PHONY: up down verify scrape lint fmt

# Start all infrastructure services (MinIO, Nessie) in background
up:
	docker compose up -d

# Stop all infrastructure services
down:
	docker compose down

# Verify that all infrastructure services are healthy and reachable
verify:
	python scripts/verify.py

# Run Telegram extractor — fetch new messages into Bronze layer
scrape:
	python extractors/tg.py scrape

# Check code style with ruff
lint:
	ruff check .

# Auto-format code with ruff
fmt:
	ruff format .
