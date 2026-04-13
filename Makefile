.PHONY: up down verify init init-silver init-gold gen-session scrape scrape-linkedin scrape-hh download-jars transform transform-plan test-dbt test spark-ui score score-dry-run rescore export lint fmt

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

# Initialize Gold namespace and sat_vacancy_score table in Iceberg catalog
init-gold:
	python3 scripts/init_catalog.py

# Download Spark JARs to jars/ directory (required before make up)
download-jars:
	cd jars && \
	curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.5.2/iceberg-spark-runtime-3.4_2.12-1.5.2.jar && \
	curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.5.2/iceberg-aws-bundle-1.5.2.jar && \
	curl -LO https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.4_2.12/0.106.0/nessie-spark-extensions-3.4_2.12-0.106.0.jar && \
	curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# Run dbt Silver + Gold transformations inside the Spark container
transform:
	docker exec grindhouse-spark python3 /opt/spark/transformations/run_dbt.py

# Compile dbt models without executing (dry-run — validates SQL + ref graph)
transform-plan:
	docker exec grindhouse-spark python3 /opt/spark/transformations/run_dbt.py --compile

# Run dbt tests (schema.yml assertions) inside the Spark container
test-dbt:
	docker exec grindhouse-spark python3 /opt/spark/transformations/run_dbt.py --test

# Run unit tests for dedup logic (DuckDB in-memory, no JVM required)
test:
	pytest transformations/tests/ -v

# Open Spark UI in default browser (available only during active job execution)
spark-ui:
	open http://localhost:4040

# Score unscored vacancies (published within 30 days) via Claude Code CLI
score:
	python3 scripts/score_vacancies.py

# Dry-run scoring — show which vacancies would be scored without calling Claude
score-dry-run:
	python3 scripts/score_vacancies.py --dry-run

# Re-score all recent vacancies, overwriting existing scores
rescore:
	python3 scripts/score_vacancies.py --rescore

# Remove failed (score=NULL) rows then re-score them
purge-failed:
	python3 scripts/score_vacancies.py --purge-failed

# Export apply/priority_apply vacancies to hire/vacancy/*.md Obsidian notes
export:
	python3 scripts/export_vacancies.py

# Check code style with ruff
lint:
	ruff check .

# Auto-format code with ruff
fmt:
	ruff format .
