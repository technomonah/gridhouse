"""Initialize Iceberg catalog: create namespaces, tables, and seed sources.

Run once after `make up` to prepare the catalog for extractors.
Safe to re-run — uses create_if_not_exists semantics throughout.

Usage:
    python scripts/init_catalog.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

# Raw Telegram messages as received — no transformations applied.
# passed_prefilter=False rows are kept for auditability but skipped in Silver.
BRONZE_TG_MESSAGES_SCHEMA = Schema(
    NestedField(1,  "message_id",       LongType(),      required=True),
    NestedField(2,  "channel",          StringType(),    required=True),
    NestedField(3,  "text",             StringType(),    required=True),
    NestedField(4,  "published_at",     TimestampType(), required=True),
    NestedField(5,  "extracted_at",     TimestampType(), required=True),
    NestedField(6,  "url",              StringType(),    required=True),
    NestedField(7,  "passed_prefilter", BooleanType(),   required=True),
)

# Raw LinkedIn hiring posts. published_at_raw stores relative time as returned
# by LinkedIn ("2 ч.", "1 дн.") — exact timestamps are not available in the DOM.
BRONZE_LINKEDIN_POSTS_SCHEMA = Schema(
    NestedField(1,  "post_id",          StringType(),    required=True),
    NestedField(2,  "post_url",         StringType(),    required=True),
    NestedField(3,  "author",           StringType(),    required=True),
    NestedField(4,  "author_url",       StringType(),    required=True),
    NestedField(5,  "company",          StringType(),    required=True),
    NestedField(6,  "company_url",      StringType(),    required=True),
    NestedField(7,  "text",             StringType(),    required=True),
    NestedField(8,  "published_at_raw", StringType(),    required=True),
    NestedField(9,  "query",            StringType(),    required=True),
    NestedField(10, "extracted_at",     TimestampType(), required=True),
    NestedField(11, "passed_prefilter", BooleanType(),   required=True),
)

# Raw HH.ru vacancies as returned by the public /vacancies API.
# employer_id is nullable — HH allows anonymous job postings.
# salary_* fields are nullable — many vacancies don't disclose compensation.
BRONZE_HH_VACANCIES_SCHEMA = Schema(
    NestedField(1,  "vacancy_id",       StringType(),    required=True),
    NestedField(2,  "vacancy_url",      StringType(),    required=True),
    NestedField(3,  "vacancy_name",     StringType(),    required=True),
    NestedField(4,  "employer_id",      StringType(),    required=False),
    NestedField(5,  "employer_name",    StringType(),    required=True),
    NestedField(6,  "salary_from",      LongType(),      required=False),
    NestedField(7,  "salary_to",        LongType(),      required=False),
    NestedField(8,  "salary_currency",  StringType(),    required=False),
    NestedField(9,  "salary_gross",     BooleanType(),   required=False),
    NestedField(10, "area_id",          StringType(),    required=True),
    NestedField(11, "area_name",        StringType(),    required=True),
    NestedField(12, "experience",       StringType(),    required=True),
    NestedField(13, "employment",       StringType(),    required=True),
    NestedField(14, "schedule",         StringType(),    required=True),
    NestedField(15, "snippet_req",      StringType(),    required=False),
    NestedField(16, "snippet_resp",     StringType(),    required=False),
    NestedField(17, "published_at",     TimestampType(), required=True),
    NestedField(18, "extracted_at",     TimestampType(), required=True),
    NestedField(19, "search_query",     StringType(),    required=True),
    NestedField(20, "passed_prefilter", BooleanType(),   required=True),
)

# ---------------------------------------------------------------------------
# Silver schemas
# ---------------------------------------------------------------------------

# Silver Telegram messages: normalized + content-hash deduped.
# Only passed_prefilter=True rows from Bronze; earliest extracted_at per hash wins.
SILVER_TG_MESSAGES_SCHEMA = Schema(
    NestedField(1,  "source_id",        StringType(),    required=True),
    NestedField(2,  "source",           StringType(),    required=True),
    NestedField(3,  "source_channel",   StringType(),    required=True),
    NestedField(4,  "text",             StringType(),    required=True),
    NestedField(5,  "text_normalized",  StringType(),    required=True),
    NestedField(6,  "content_hash",     StringType(),    required=True),
    NestedField(7,  "published_at",     TimestampType(), required=False),
    NestedField(8,  "extracted_at",     TimestampType(), required=True),
    NestedField(9,  "url",              StringType(),    required=True),
)

# Silver LinkedIn posts: normalized + content-hash deduped.
# published_at is always NULL (exact timestamps unavailable in MHTML snapshots).
# published_at_approx is derived from published_at_raw ("2 ч.", "1 дн.", etc.)
# by subtracting the offset from extracted_at — approximate only.
SILVER_LINKEDIN_POSTS_SCHEMA = Schema(
    NestedField(1,  "source_id",           StringType(),    required=True),
    NestedField(2,  "source",              StringType(),    required=True),
    NestedField(3,  "author",              StringType(),    required=True),
    NestedField(4,  "author_url",          StringType(),    required=True),
    NestedField(5,  "company",             StringType(),    required=True),
    NestedField(6,  "company_url",         StringType(),    required=True),
    NestedField(7,  "text",                StringType(),    required=True),
    NestedField(8,  "text_normalized",     StringType(),    required=True),
    NestedField(9,  "content_hash",        StringType(),    required=True),
    NestedField(10, "published_at",        TimestampType(), required=False),
    NestedField(11, "published_at_approx", TimestampType(), required=False),
    NestedField(12, "extracted_at",        TimestampType(), required=True),
    NestedField(13, "url",                 StringType(),    required=True),
    NestedField(14, "source_query",        StringType(),    required=True),
)

# Silver HH.ru vacancies: normalized + deduped by vacancy_id then content_hash.
# Salary fields retained as nullable — currency normalization is a Gold concern.
SILVER_HH_VACANCIES_SCHEMA = Schema(
    NestedField(1,  "source_id",       StringType(),    required=True),
    NestedField(2,  "source",          StringType(),    required=True),
    NestedField(3,  "url",             StringType(),    required=True),
    NestedField(4,  "vacancy_name",    StringType(),    required=True),
    NestedField(5,  "employer_id",     StringType(),    required=False),
    NestedField(6,  "employer_name",   StringType(),    required=True),
    NestedField(7,  "salary_from",     LongType(),      required=False),
    NestedField(8,  "salary_to",       LongType(),      required=False),
    NestedField(9,  "salary_currency", StringType(),    required=False),
    NestedField(10, "salary_gross",    BooleanType(),   required=False),
    NestedField(11, "area_id",         StringType(),    required=True),
    NestedField(12, "area_name",       StringType(),    required=True),
    NestedField(13, "experience",      StringType(),    required=True),
    NestedField(14, "employment",      StringType(),    required=True),
    NestedField(15, "schedule",        StringType(),    required=True),
    NestedField(16, "snippet_req",     StringType(),    required=True),
    NestedField(17, "snippet_resp",    StringType(),    required=True),
    NestedField(18, "text",            StringType(),    required=True),
    NestedField(19, "text_normalized", StringType(),    required=True),
    NestedField(20, "content_hash",    StringType(),    required=True),
    NestedField(21, "published_at",    TimestampType(), required=True),
    NestedField(22, "extracted_at",    TimestampType(), required=True),
    NestedField(23, "source_query",    StringType(),    required=True),
)

# Silver unified vacancies: cross-source dedup by content_hash.
# Common column subset only — source-specific fields (salary, author) stay in
# per-source Silver tables and are used by Gold Data Vault satellites.
SILVER_VACANCIES_SCHEMA = Schema(
    NestedField(1,  "source_id",           StringType(),    required=True),
    NestedField(2,  "source",              StringType(),    required=True),
    NestedField(3,  "url",                 StringType(),    required=True),
    NestedField(4,  "text",                StringType(),    required=True),
    NestedField(5,  "text_normalized",     StringType(),    required=True),
    NestedField(6,  "content_hash",        StringType(),    required=True),
    NestedField(7,  "published_at",        TimestampType(), required=False),
    NestedField(8,  "published_at_approx", TimestampType(), required=False),
    NestedField(9,  "extracted_at",        TimestampType(), required=True),
)

# Gold Data Vault tables for HR interaction tracking.
# Written by scripts/sync_hr_events.py — NOT managed by dbt.
#
# hub_hr_interaction: one row per unique interaction event (applied, viewed, etc.)
# link_interaction_vacancy: ties each interaction to a vacancy in the pipeline
# sat_interaction_details: descriptive attributes of the interaction
#
# Business key for hub: MD5(vacancy_url || status || updated_date)
# resume_version is derived from the `cv:` frontmatter field slug.
GOLD_HUB_HR_INTERACTION_SCHEMA = Schema(
    NestedField(1, "hub_hr_interaction_hk", StringType(),    required=True),
    NestedField(2, "event_id_bk",           StringType(),    required=True),
    NestedField(3, "load_dts",              TimestampType(), required=True),
    NestedField(4, "rec_src",               StringType(),    required=True),
)

GOLD_LINK_INTERACTION_VACANCY_SCHEMA = Schema(
    NestedField(1, "link_hk",                  StringType(),    required=True),
    NestedField(2, "hub_hr_interaction_hk",    StringType(),    required=True),
    NestedField(3, "hub_vacancy_hk",           StringType(),    required=False),
    NestedField(4, "vacancy_url",              StringType(),    required=True),
    NestedField(5, "load_dts",                 TimestampType(), required=True),
    NestedField(6, "rec_src",                  StringType(),    required=True),
)

GOLD_SAT_INTERACTION_DETAILS_SCHEMA = Schema(
    NestedField(1, "hub_hr_interaction_hk", StringType(),    required=True),
    NestedField(2, "load_dts",              TimestampType(), required=True),
    NestedField(3, "rec_src",               StringType(),    required=True),
    NestedField(4, "hash_diff",             StringType(),    required=True),
    NestedField(5, "event_type",            StringType(),    required=True),
    NestedField(6, "resume_version",        StringType(),    required=False),
    NestedField(7, "occurred_at",           TimestampType(), required=True),
    NestedField(8, "notes",                 StringType(),    required=False),
)

# Gold sat_vacancy_score: AI scoring results written by scripts/score_vacancies.py.
# This table is NOT managed by SQLMesh — pre-created here so score_vacancies.py
# can append to it without needing to create the table on first run.
GOLD_SAT_VACANCY_SCORE_SCHEMA = Schema(
    NestedField(1, "hub_vacancy_hk",     StringType(),    required=True),
    NestedField(2, "load_dts",           TimestampType(), required=True),
    NestedField(3, "rec_src",            StringType(),    required=True),
    NestedField(4, "hash_diff",          StringType(),    required=True),
    NestedField(5, "score",              DoubleType(),    required=False),
    NestedField(6, "reasoning",          StringType(),    required=False),
    NestedField(7, "recommended_action", StringType(),    required=False),
    NestedField(8, "model_version",      StringType(),    required=False),
    NestedField(9, "scored_at",          TimestampType(), required=False),
)

# Registry of all data sources. Extractors read active sources from here
# instead of having channel lists hardcoded in scripts.
SOURCES_SCHEMA = Schema(
    NestedField(1, "source_id",   StringType(),    required=True),
    NestedField(2, "source_type", StringType(),    required=True),
    NestedField(3, "name",        StringType(),    required=True),
    NestedField(4, "config",      StringType(),    required=True),
    NestedField(5, "active",      BooleanType(),   required=True),
    NestedField(6, "added_at",    TimestampType(), required=True),
)

# ---------------------------------------------------------------------------
# Initial TG channels (migrated from tg-vacancy-bot/config.py)
# ---------------------------------------------------------------------------

_TG_CHANNELS = [
    "datasciencejobs",
    "datajobschannel",
    "odsjobs",
    "datajob",
    "datascienceml_jobs",
    "job_analyst_datascience",
    "foranalysts",
]

# Composite boolean queries using LinkedIn's (A OR B) "phrase" syntax.
# Each query covers all 6 hiring signals against one role/technology target.
# Duplicates across queries are harmless — Bronze dedup removes them by post_id.
_SIGNALS = "(нанимаем OR ищу OR ищем OR набираем OR нанимаю OR усиливаем OR приглашаем)"
_LINKEDIN_QUERIES = [
    # Data Engineer — English and common Russian spellings
    f'{_SIGNALS} "data engineer"',
    f'{_SIGNALS} "инженер данных"',
    f'{_SIGNALS} "дата инженер"',
    # Analytics Engineer — English and hyphenated Russian
    f'{_SIGNALS} "analytics engineer"',
    f'{_SIGNALS} "дата-инженер"',
    # Stack signal — dbt as a proxy for DE/AE positions
    f'{_SIGNALS} dbt',
]

# Initial HH.ru searches — two geography variants per query:
#   - Almaty (area=160): office/hybrid/remote within Kazakhstan
#   - Global (no area): remote-only positions worldwide
#
# Russian queries use professional_role=156 (BI-аналитик, аналитик данных)
# instead of free text — "аналитик данных" / "инженер данных" without a role
# filter pulls in lawyers, marketers, and HR (>500 irrelevant results).
# English queries ("Analytics Engineer", "Data Engineer") are precise enough
# on their own and don't need an additional role filter.
_HH_SEARCHES = [
    {"query": "Analytics Engineer",                     "area": "160"},
    {"query": "Data Engineer",                          "area": "160"},
    {"query": "аналитик данных", "professional_role": "156", "area": "160"},
    {"query": "Analytics Engineer",                     "schedule": "remote"},
    {"query": "Data Engineer",                          "schedule": "remote"},
    {"query": "аналитик данных", "professional_role": "156", "schedule": "remote"},
]


def _now() -> datetime:
    """Return current UTC time with microsecond precision."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def init_namespaces(catalog) -> None:
    """Create Bronze, Silver, and metadata namespaces if they don't exist.

    Args:
        catalog: Connected PyIceberg catalog instance.
    """
    for ns in [("bronze",), ("silver",), ("meta",), ("gold",)]:
        if ns not in catalog.list_namespaces():
            catalog.create_namespace(ns)
            print(f"  created namespace: {'.'.join(ns)}")
        else:
            print(f"  namespace exists:  {'.'.join(ns)}")


def init_tables(catalog) -> None:
    """Create Bronze tables if they don't exist.

    Silver tables are managed by SQLMesh/Spark — do NOT pre-create them here.
    SQLMesh writes to sqlmesh__silver.* and creates silver.* as views pointing
    to those tables. Pre-creating silver.* as Iceberg tables blocks SQLMesh.

    Args:
        catalog: Connected PyIceberg catalog instance.
    """
    tables = [
        # Bronze — raw data from extractors, no transformations
        ("bronze.tg_messages",        BRONZE_TG_MESSAGES_SCHEMA),
        ("bronze.linkedin_posts",     BRONZE_LINKEDIN_POSTS_SCHEMA),
        ("bronze.hh_vacancies",       BRONZE_HH_VACANCIES_SCHEMA),
        # Metadata — source registry
        ("meta.sources",              SOURCES_SCHEMA),
        # Gold — AI scoring results (written by scripts/score_vacancies.py, not dbt)
        # dbt-managed tables (hubs, links, satellites) are NOT pre-created here —
        # dbt creates them directly in nessie.gold.* on first make transform.
        ("gold.sat_vacancy_score",           GOLD_SAT_VACANCY_SCORE_SCHEMA),
        # Gold — HR interaction tracking (written by scripts/sync_hr_events.py, not dbt)
        ("gold.hub_hr_interaction",          GOLD_HUB_HR_INTERACTION_SCHEMA),
        ("gold.link_interaction_vacancy",    GOLD_LINK_INTERACTION_VACANCY_SCHEMA),
        ("gold.sat_interaction_details",     GOLD_SAT_INTERACTION_DETAILS_SCHEMA),
    ]
    for identifier, schema in tables:
        try:
            catalog.load_table(identifier)
            print(f"  table exists:  {identifier}")
        except Exception:
            catalog.create_table(identifier, schema=schema)
            print(f"  created table: {identifier}")


def seed_sources(catalog) -> None:
    """Populate meta.sources with initial TG channels if not already present.

    Checks existing source_ids to avoid duplicates on re-runs.

    Args:
        catalog: Connected PyIceberg catalog instance.
    """
    table = patch_table_io(catalog.load_table("meta.sources"))

    # Read existing source_ids to skip already-seeded channels
    existing = set()
    for batch in table.scan(selected_fields=("source_id",)).to_arrow().to_batches():
        for sid in batch.column("source_id").to_pylist():
            existing.add(sid)

    now = _now()
    rows = []
    for channel in _TG_CHANNELS:
        source_id = f"tg_{channel}"
        if source_id in existing:
            print(f"  source exists:  {source_id}")
            continue
        rows.append({
            "source_id":   source_id,
            "source_type": "telegram",
            "name":        f"@{channel}",
            "config":      json.dumps({"channel": channel}),
            "active":      True,
            "added_at":    now,
        })

    # Seed LinkedIn queries
    for query in _LINKEDIN_QUERIES:
        slug = query.lower().replace('"', "").replace(" ", "_")[:40]
        source_id = f"linkedin_{slug}"
        if source_id in existing:
            print(f"  source exists:  {source_id}")
            continue
        rows.append({
            "source_id":   source_id,
            "source_type": "linkedin",
            "name":        query,
            "config":      json.dumps({"query": query}),
            "active":      True,
            "added_at":    now,
        })

    # Seed HH searches
    for search in _HH_SEARCHES:
        slug = search["query"].lower().replace(" ", "_")[:30]
        geo = "almaty" if search.get("area") else "remote"
        role_suffix = f"_r{search['professional_role']}" if search.get("professional_role") else ""
        source_id = f"hh_{slug}_{geo}{role_suffix}"
        if source_id in existing:
            print(f"  source exists:  {source_id}")
            continue
        rows.append({
            "source_id":   source_id,
            "source_type": "hh",
            "name":        f"{search['query']} ({geo})",
            "config":      json.dumps(search),
            "active":      True,
            "added_at":    now,
        })

    if rows:
        arrow_schema = pa.schema([
            pa.field("source_id",   pa.string(),        nullable=False),
            pa.field("source_type", pa.string(),        nullable=False),
            pa.field("name",        pa.string(),        nullable=False),
            pa.field("config",      pa.string(),        nullable=False),
            pa.field("active",      pa.bool_(),         nullable=False),
            pa.field("added_at",    pa.timestamp("us"), nullable=False),
        ])
        arrow_table = pa.table({
            "source_id":   pa.array([r["source_id"]   for r in rows], type=pa.string()),
            "source_type": pa.array([r["source_type"] for r in rows], type=pa.string()),
            "name":        pa.array([r["name"]        for r in rows], type=pa.string()),
            "config":      pa.array([r["config"]      for r in rows], type=pa.string()),
            "active":      pa.array([r["active"]      for r in rows], type=pa.bool_()),
            "added_at":    pa.array([r["added_at"]    for r in rows], type=pa.timestamp("us")),
        }, schema=arrow_schema)
        table.append(arrow_table)
        tg_count = sum(1 for r in rows if r["source_type"] == "telegram")
        li_count = sum(1 for r in rows if r["source_type"] == "linkedin")
        hh_count = sum(1 for r in rows if r["source_type"] == "hh")
        print(f"  seeded {tg_count} TG sources, {li_count} LinkedIn sources, {hh_count} HH sources")


def reset_linkedin_sources(catalog) -> None:
    """Drop and recreate meta.sources, then re-seed with current _LINKEDIN_QUERIES.

    Use this when _LINKEDIN_QUERIES has changed and the old source_ids in
    meta.sources are stale. PyIceberg does not support UPDATE/DELETE, so the
    only way to remove obsolete rows is to drop and recreate the table.

    All non-LinkedIn sources (TG, HH) are re-seeded automatically because
    seed_sources() is idempotent — it skips source_ids already present.

    Args:
        catalog: Connected PyIceberg catalog instance.
    """
    print("Dropping meta.sources...")
    catalog.drop_table("meta.sources")
    catalog.create_table("meta.sources", schema=SOURCES_SCHEMA)
    print("  recreated meta.sources")
    seed_sources(catalog)


def main() -> None:
    """Run full catalog initialization: namespaces → tables → seed data.

    Flags:
        --reset-linkedin: Drop and recreate meta.sources to replace stale
            LinkedIn source_ids after _LINKEDIN_QUERIES has changed.
    """
    import sys
    reset_linkedin = "--reset-linkedin" in sys.argv

    print("Initializing Iceberg catalog...\n")

    catalog = get_catalog()

    print("[Namespaces]")
    init_namespaces(catalog)

    print("\n[Tables]")
    init_tables(catalog)

    print("\n[Sources]")
    if reset_linkedin:
        print("  --reset-linkedin: dropping and recreating meta.sources")
        reset_linkedin_sources(catalog)
    else:
        seed_sources(catalog)

    print("\nDone.")


if __name__ == "__main__":
    main()
