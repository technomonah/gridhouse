# grindhouse

Job hunting lakehouse — automated vacancy collection, AI scoring, and applying via modern DE stack.

## Stack

| Role | Tool |
|------|------|
| Object storage | MinIO |
| Table format | Apache Iceberg |
| Catalog | Nessie |
| Processing | Apache Spark |
| Transformations + lineage | SQLMesh |
| DWH structure | Data Vault 2.0 |
| Query engine | Trino |
| Orchestration | Airflow |
| AI scoring | Anthropic SDK (batched) |
| Output | Obsidian vault .md |

## Architecture

```
Sources → Bronze (raw) → Silver (normalized) → Gold (scored, Data Vault) → Obsidian vault
```

- **Bronze** — raw data from sources as-is, with full metadata
- **Silver** — normalization and deduplication without AI (Spark)
- **Gold** — batched AI scoring → Data Vault models in SQLMesh

## Sources

- Telegram — job channels via Telethon
- LinkedIn — hiring posts via Selenium
- HH.ru — vacancies via public API
- Habr Career — IT vacancies

## Getting started

```bash
cp .env.example .env
# fill in .env
make up
```
