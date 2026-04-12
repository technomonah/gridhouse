"""Unit tests for Silver layer content-hash dedup SQL logic.

Tests the core dedup pattern used in all four Silver models using DuckDB
in-memory. DuckDB executes the same standard SQL as Spark: window functions,
MD5(), LOWER(), TRIM().

Note: These tests verify SQL logic correctness, not Spark execution.
End-to-end integration is verified via `make transform`.
"""

from __future__ import annotations

import duckdb
import pytest


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _run_dedup(db: duckdb.DuckDBPyConnection, rows: list[dict]) -> list[dict]:
    """Load rows into DuckDB and apply the standard content-hash dedup SQL.

    Replicates the PARTITION BY content_hash ORDER BY extracted_at pattern
    used in silver/tg_messages.sql, silver/linkedin_posts.sql, and the
    cross-source dedup in silver/vacancies.sql.

    Args:
        db: In-memory DuckDB connection from the `db` fixture.
        rows: Input rows. Each dict must have keys: source_id, text, extracted_at.

    Returns:
        Surviving rows after dedup as a list of dicts with keys source_id, content_hash.
    """
    db.execute("""
        CREATE OR REPLACE TABLE _input (
            source_id    VARCHAR,
            text         VARCHAR,
            extracted_at TIMESTAMP
        )
    """)
    for row in rows:
        db.execute(
            "INSERT INTO _input VALUES (?, ?, ?::TIMESTAMP)",
            [row["source_id"], row["text"], row["extracted_at"]],
        )
    result = db.execute("""
        SELECT source_id, content_hash
        FROM (
            SELECT
                source_id,
                MD5(LOWER(TRIM(text))) AS content_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY MD5(LOWER(TRIM(text)))
                    ORDER BY extracted_at ASC
                ) AS rn
            FROM _input
        )
        WHERE rn = 1
        ORDER BY source_id
    """).fetchall()
    return [{"source_id": r[0], "content_hash": r[1]} for r in result]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_no_duplicates_passes_through(db):
    """All unique texts survive dedup unchanged."""
    rows = [
        {"source_id": "1", "text": "Data Engineer position at ACME",   "extracted_at": "2024-01-01 10:00:00"},
        {"source_id": "2", "text": "Analytics Engineer role at BigCo",  "extracted_at": "2024-01-01 11:00:00"},
        {"source_id": "3", "text": "ClickHouse expert wanted urgently", "extracted_at": "2024-01-01 12:00:00"},
    ]
    result = _run_dedup(db, rows)
    assert len(result) == 3


def test_exact_duplicate_keeps_earliest(db):
    """Exact duplicate text: row with earlier extracted_at survives."""
    rows = [
        {"source_id": "early", "text": "We are hiring dbt engineer",  "extracted_at": "2024-01-01 08:00:00"},
        {"source_id": "late",  "text": "We are hiring dbt engineer",  "extracted_at": "2024-01-01 12:00:00"},
    ]
    result = _run_dedup(db, rows)
    assert len(result) == 1
    assert result[0]["source_id"] == "early"


def test_case_insensitive_dedup(db):
    """Texts differing only in case produce the same content_hash — treated as duplicates."""
    rows = [
        {"source_id": "1", "text": "Ищем Data Engineer в команду",  "extracted_at": "2024-01-01 10:00:00"},
        {"source_id": "2", "text": "ИЩЕМ DATA ENGINEER В КОМАНДУ",  "extracted_at": "2024-01-01 11:00:00"},
    ]
    result = _run_dedup(db, rows)
    assert len(result) == 1
    assert result[0]["source_id"] == "1"


def test_whitespace_trimmed_for_hash(db):
    """Leading/trailing whitespace is stripped before hashing — treated as duplicates."""
    rows = [
        {"source_id": "padded",  "text": "  Analytics Engineer  ",  "extracted_at": "2024-01-01 10:00:00"},
        {"source_id": "clean",   "text": "Analytics Engineer",      "extracted_at": "2024-01-01 11:00:00"},
    ]
    result = _run_dedup(db, rows)
    assert len(result) == 1
    # padded was extracted earlier — it should win despite leading/trailing spaces
    assert result[0]["source_id"] == "padded"


def test_cross_source_dedup(db):
    """Same text from three different sources: only the earliest survives."""
    rows = [
        {"source_id": "tg_123",  "text": "нанимаем data engineer dbt airflow",  "extracted_at": "2024-01-01 09:00:00"},
        {"source_id": "li_abc",  "text": "нанимаем data engineer dbt airflow",  "extracted_at": "2024-01-01 10:00:00"},
        {"source_id": "hh_456",  "text": "нанимаем data engineer dbt airflow",  "extracted_at": "2024-01-01 11:00:00"},
    ]
    result = _run_dedup(db, rows)
    assert len(result) == 1
    assert result[0]["source_id"] == "tg_123"


def test_empty_input(db):
    """Empty input produces empty output — no errors."""
    result = _run_dedup(db, [])
    assert result == []


def test_multiple_groups_independently_deduped(db):
    """Each unique content_hash group is deduped independently."""
    rows = [
        # Group A — two duplicates
        {"source_id": "a1", "text": "Spark engineer wanted",  "extracted_at": "2024-01-01 08:00:00"},
        {"source_id": "a2", "text": "Spark engineer wanted",  "extracted_at": "2024-01-01 09:00:00"},
        # Group B — unique
        {"source_id": "b1", "text": "ClickHouse DBA needed",  "extracted_at": "2024-01-01 10:00:00"},
        # Group C — three duplicates
        {"source_id": "c1", "text": "dbt analytics engineer", "extracted_at": "2024-01-01 07:00:00"},
        {"source_id": "c2", "text": "dbt analytics engineer", "extracted_at": "2024-01-01 08:00:00"},
        {"source_id": "c3", "text": "dbt analytics engineer", "extracted_at": "2024-01-01 09:00:00"},
    ]
    result = _run_dedup(db, rows)
    surviving_ids = {r["source_id"] for r in result}
    assert len(result) == 3
    assert "a1" in surviving_ids  # earliest in group A
    assert "b1" in surviving_ids  # only row in group B
    assert "c1" in surviving_ids  # earliest in group C
