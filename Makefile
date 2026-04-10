.PHONY: up down verify init gen-session scrape scrape-linkedin scrape-hh lint fmt

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

# Authenticate with Telegram once and print TG_SESSION_STRING for .env
gen-session:
	python3 scripts/gen_session.py

# Run Telegram extractor — fetch new messages into Bronze layer
scrape:
	python3 extractors/tg.py scrape

# Run LinkedIn extractor — fetch new hiring posts into Bronze layer
scrape-linkedin:
	python3 extractors/linkedin.py scrape

# Run HH.ru extractor — fetch new vacancies into Bronze layer
scrape-hh:
	python3 extractors/hh.py scrape

# Check code style with ruff
lint:
	ruff check .

# Auto-format code with ruff
fmt:
	ruff format .
