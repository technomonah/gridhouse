# grindhouse — CLAUDE.md

## Project

Job hunting lakehouse for automated vacancy collection from TG, LinkedIn, HH, Habr Career,
AI scoring, and auto-applying via HH API.

## Stack

MinIO + Apache Iceberg + Nessie + Apache Spark + dbt-spark + Airflow + Anthropic SDK

## Architecture

Medallion: Bronze (raw) → Silver (Spark, normalization) → Gold (AI scoring, Data Vault 2.0)

Data Vault hubs: `hub_company`, `hub_vacancy`, `hub_contact`
Data Vault links: `link_vacancy_company`, `link_vacancy_contact`
Data Vault same-as links: `sal_vacancy` — links the same vacancy appearing in multiple sources
Data Vault satellites: `sat_vacancy_details`, `sat_vacancy_score`, `sat_vacancy_source`, `sat_company_details`

Deduplication strategy:
- Silver: technical dedup by content hash (identical text)
- SAL: semantic dedup (same vacancy from different sources, fuzzy match on company + role + stack)

## Candidate context

Nikita, Analytics Engineer / Data Engineer, Almaty. Stack: dbt, ClickHouse, SQL, Python, PostgreSQL.
Target roles: Analytics Engineer, DE with dbt+ClickHouse, BI Engineer.
Vacancy TTL: 30 days from publish date (not applied to cold contacts).

## Rules

### General
This is a learning project. The goal is to apply DE best practices and modern
industry standards — not to find the simplest solution for personal use.
When making decisions, prefer the approach used in production DE environments,
even if it adds complexity.

- Do not automatically agree with proposals — argue if something is technically wrong or excessive.
- All configuration via environment variables, no hardcoded values.
- All sources write to Iceberg via PyIceberg + Nessie catalog.
- Default language: English (code, comments, docstrings, commit messages).

### Documentation
Cover the entire project with documentation. Be concise but thorough.

#### Python — Google style docstrings on every function, class, and module
```python
def scrape_channel(channel: str, since: datetime) -> list[RawMessage]:
    """Fetch new messages from a Telegram channel.

    Args:
        channel: Channel username without @.
        since: Only fetch messages after this timestamp.

    Returns:
        List of raw messages ready to write to Bronze layer.
    """
```

#### SQL (SQLMesh models) — header comment + comments on non-obvious CTEs
```sql
-- Silver model: deduplicate raw TG messages by content hash.
-- Removes reposts and cross-channel duplicates before AI scoring.

WITH deduped AS (
    -- Keep the earliest occurrence of each unique message
    SELECT * FROM ...
)
```

#### YAML (docker-compose, Airflow DAGs) — comment every service/parameter that isn't self-evident
```yaml
services:
  nessie:
    # Iceberg catalog — tracks table metadata and enables time travel.
    # REST API available at http://localhost:19120/api/v1
    image: projectnessie/nessie:latest
```

#### Makefile — one-line comment above every target
```makefile
# Start all infrastructure services (MinIO, Nessie) in background
up:
    docker compose up -d
```

### Knowledge base
Before working with any technology, concept, or pattern for the first time:
1. Check if a note already exists in `vault47/knowledge/`
2. If not — create one before proceeding

Scope: anything we use — DE tools (Iceberg, Spark, Airflow...), Python patterns (OOP, dataclasses, async...), libraries, concepts.
Notes go in a relevant subfolder under `knowledge/` (create if needed).
Use `slug.md` naming, Russian content, concise but thorough.
