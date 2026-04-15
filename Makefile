.PHONY: up down verify init init-silver init-gold gen-session scrape scrape-linkedin scrape-hh download-jars transform transform-plan test-dbt test spark-ui trino-cli score score-dry-run rescore export lint fmt dev-branch dev-merge

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

# Run Telegram extractor — fetch new messages into Bronze layer.
# Pass NESSIE_BRANCH=dev to write to the dev branch instead of main.
scrape:
	NESSIE_BRANCH=$${NESSIE_BRANCH:-main} python3 extractors/tg.py scrape

# Run LinkedIn extractor — fetch new hiring posts into Bronze layer.
# Pass NESSIE_BRANCH=dev to write to the dev branch instead of main.
scrape-linkedin:
	NESSIE_BRANCH=$${NESSIE_BRANCH:-main} python3 extractors/linkedin.py scrape

# Run HH.ru extractor — fetch new vacancies into Bronze layer.
# Pass NESSIE_BRANCH=dev to write to the dev branch instead of main.
scrape-hh:
	NESSIE_BRANCH=$${NESSIE_BRANCH:-main} python3 extractors/hh.py scrape

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

# Run dbt Silver + Gold transformations inside the Spark container.
# Pass NESSIE_BRANCH=dev to write to the dev Nessie branch instead of main.
transform:
	docker exec -e NESSIE_BRANCH=$${NESSIE_BRANCH:-main} grindhouse-spark python3 /opt/spark/transformations/run_dbt.py

# Compile dbt models without executing (dry-run — validates SQL + ref graph)
transform-plan:
	docker exec -e NESSIE_BRANCH=$${NESSIE_BRANCH:-main} grindhouse-spark python3 /opt/spark/transformations/run_dbt.py --compile

# Run dbt tests (schema.yml assertions) inside the Spark container
test-dbt:
	docker exec -e NESSIE_BRANCH=$${NESSIE_BRANCH:-main} grindhouse-spark python3 /opt/spark/transformations/run_dbt.py --test

# Create the dev branch in Nessie from the current state of main (metadata copy only — no data duplication).
# v2 API: name and type go as query params; body is the source reference with its current hash.
dev-branch:
	@HASH=$$(curl -s http://localhost:19120/api/v2/trees | python3 -c "import sys,json; refs=json.load(sys.stdin)['references']; print(next(r['hash'] for r in refs if r['name']=='main'))"); \
	curl -s -X POST "http://localhost:19120/api/v2/trees?name=dev&type=BRANCH" \
	  -H 'Content-Type: application/json' \
	  -d "{\"type\":\"BRANCH\",\"name\":\"main\",\"hash\":\"$$HASH\"}" | python3 -m json.tool

# Merge dev branch into main (requires Nessie UI — hash-based merge not scripted here)
dev-merge:
	@echo "Open Nessie UI to merge: http://localhost:19120"
	@echo "Or use the API: GET /api/v2/trees/dev to get the current hash, then POST /api/v2/trees/main/merge"

# Run unit tests for dedup logic (DuckDB in-memory, no JVM required)
test:
	pytest transformations/tests/ -v

# Open Spark UI in default browser (available only during active job execution)
spark-ui:
	open http://localhost:4040

# Open Trino CLI inside the container (run make up first)
trino-cli:
	docker exec -it grindhouse-trino trino

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

# Sync HR interaction events from hire/vacancy/*.md to Gold Data Vault tables
sync-hr:
	python3 scripts/sync_hr_events.py

# Check code style with ruff
lint:
	ruff check .

# Auto-format code with ruff
fmt:
	ruff format .
