.PHONY: up down verify init init-silver gen-session scrape scrape-linkedin scrape-hh download-jars transform transform-plan test spark-ui lint fmt

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

# Initialize Silver namespace and tables in Iceberg catalog (run after make init)
init-silver:
	python3 scripts/init_catalog.py

# Download Spark JARs to jars/ directory (required before make up)
download-jars:
	cd jars && \
	curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.2/iceberg-spark-runtime-3.4_2.12-1.5.2.jar && \
	curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar && \
	curl -LO https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.4_2.12/0.106.0/nessie-spark-extensions-3.4_2.12-0.106.0.jar && \
	curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Run SQLMesh Silver transformations inside the Spark container
transform:
	docker exec grindhouse-spark python3 /opt/spark/transformations/run.py

# Dry-run: print SQLMesh plan without applying it
transform-plan:
	docker exec grindhouse-spark python3 /opt/spark/transformations/run.py --plan-only

# Run unit tests for dedup logic (DuckDB in-memory, no JVM required)
test:
	pytest transformations/tests/ -v

# Open Spark UI in default browser (available only during active job execution)
spark-ui:
	open http://localhost:4040

# Check code style with ruff
lint:
	ruff check .

# Auto-format code with ruff
fmt:
	ruff format .
