"""Shared utilities for all Bronze-layer extractors.

Provides the RawVacancy dataclass and pre-filter logic used by every
source extractor (Telegram, HH.ru, LinkedIn, Habr Career).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


# ---------------------------------------------------------------------------
# Pre-filter
# ---------------------------------------------------------------------------

# At least one keyword must appear in the text for a record to be flagged
# as relevant. Filters ~90% of noise before AI scoring downstream.
KEYWORDS: frozenset[str] = frozenset({
    "dbt", "airflow", "spark", "dwh", "data vault", "data warehouse",
    "analytics engineer", "data engineer", "etl", "elt",
    "clickhouse", "kafka", "bigquery", "redshift", "snowflake",
    "data platform", "data infrastructure", "pipeline",
    "аналитик данных", "инженер данных", "аналитический инженер",
    "хранилище данных",
    # Hiring signal words — LinkedIn Russian posts
    "нанимаем", "нанимаю", "нанимаемся", "набираем",
    "ищем", "открыта вакансия", "усиливаем", "приглашаем",
})


def passes_prefilter(text: str) -> bool:
    """Check if text contains at least one job-related keyword.

    Args:
        text: Raw text from any vacancy source.

    Returns:
        True if any keyword is found (case-insensitive).
    """
    lowered = text.lower()
    return any(kw in lowered for kw in KEYWORDS)


# ---------------------------------------------------------------------------
# RawVacancy dataclass
# ---------------------------------------------------------------------------

@dataclass
class RawVacancy:
    """Unified representation of a raw vacancy from any source.

    All extractors produce RawVacancy instances before writing to their
    respective Bronze Iceberg tables. The common fields capture the
    intersection across sources; source-specific fields (salary, channel
    name, author, etc.) go into ``extra``.

    Each Bronze table (tg_messages, hh_vacancies, linkedin_posts) keeps
    its own schema — RawVacancy is an in-memory contract, not a single
    unified table.

    Attributes:
        source: Source identifier — 'telegram', 'hh', or 'linkedin'.
        source_id: Unique record ID within the source (always str).
            TG: message_id, HH: vacancy_id, LinkedIn: post_id.
        url: Direct link to the vacancy or post.
        text: Main text content used for pre-filtering and AI scoring.
        published_at: Publication timestamp (UTC, no tzinfo).
            None for LinkedIn — only relative timestamps are available.
        extracted_at: When this record was scraped (UTC, no tzinfo).
        passed_prefilter: True if ``text`` matched at least one keyword.
        extra: Source-specific fields as a plain dict. Examples:
            TG:       {"channel": "datasciencejobs", "message_id": 3212}
            HH:       {"employer_name": "Yandex", "salary_from": 200000, ...}
            LinkedIn: {"author": "Jane Doe", "company": "Acme", ...}
    """

    source: str
    source_id: str
    url: str
    text: str
    published_at: datetime | None
    extracted_at: datetime
    passed_prefilter: bool
    extra: dict = field(default_factory=dict)
