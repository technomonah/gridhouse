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


def _now() -> datetime:
    """Return current UTC time with microsecond precision."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def init_namespaces(catalog) -> None:
    """Create Bronze and metadata namespaces if they don't exist.

    Args:
        catalog: Connected PyIceberg catalog instance.
    """
    for ns in [("bronze",), ("meta",)]:
        if ns not in catalog.list_namespaces():
            catalog.create_namespace(ns)
            print(f"  created namespace: {'.'.join(ns)}")
        else:
            print(f"  namespace exists:  {'.'.join(ns)}")


def init_tables(catalog) -> None:
    """Create Bronze tables if they don't exist.

    Args:
        catalog: Connected PyIceberg catalog instance.
    """
    tables = [
        ("bronze.tg_messages", BRONZE_TG_MESSAGES_SCHEMA),
        ("meta.sources",       SOURCES_SCHEMA),
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

    if rows:
        arrow_schema = pa.schema([
            pa.field("source_id",   pa.string(),       nullable=False),
            pa.field("source_type", pa.string(),       nullable=False),
            pa.field("name",        pa.string(),       nullable=False),
            pa.field("config",      pa.string(),       nullable=False),
            pa.field("active",      pa.bool_(),        nullable=False),
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
        print(f"  seeded {len(rows)} TG sources")


def main() -> None:
    """Run full catalog initialization: namespaces → tables → seed data."""
    print("Initializing Iceberg catalog...\n")

    catalog = get_catalog()

    print("[Namespaces]")
    init_namespaces(catalog)

    print("\n[Tables]")
    init_tables(catalog)

    print("\n[Sources]")
    seed_sources(catalog)

    print("\nDone.")


if __name__ == "__main__":
    main()
