"""Sync HR interaction events from Obsidian vacancy notes to Gold Data Vault tables.

Reads all hire/vacancy/*.md files, parses YAML frontmatter, and writes interaction
events (applied, resume_viewed, hr_response, rejected, invited) to three Gold
Iceberg tables following Data Vault 2.0 conventions:

  - gold.hub_hr_interaction       — one row per unique event (BK = event_id)
  - gold.link_interaction_vacancy — ties each event to a vacancy hub entry
  - gold.sat_interaction_details  — event type, resume version, notes

Events are identified by (vacancy_url, status, updated_date). The script is
idempotent: repeated runs skip already-logged events.

resume_version is derived from the `cv:` frontmatter field — the stem of the
adapted resume filename (e.g. `ozon-cv` from `resume/by-vacancy/ozon-cv.md`).

Statuses that generate events (all others are skipped):
  applied, resume_viewed, hr_response, rejected, wont-do, invited

Usage:
    python scripts/sync_hr_events.py [--vault-path PATH] [--dry-run]

Flags:
    --vault-path PATH: Path to Obsidian vault root (default: ~/Desktop/vault47).
    --dry-run:         Print which events would be logged without writing.

Environment variables:
    NESSIE_URI          — Nessie REST endpoint (default: http://localhost:19120/iceberg/).
    MINIO_ROOT_USER     — MinIO access key.
    MINIO_ROOT_PASSWORD — MinIO secret key.
    S3_ENDPOINT         — MinIO S3 endpoint (default: http://localhost:9000).
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pyarrow as pa
import yaml

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_VAULT_PATH = Path.home() / "Desktop" / "vault47"
VACANCY_DIR = "hire/vacancy"
REC_SRC = "scripts.sync_hr_events"

# Statuses that are considered loggable events. 'new' and 'to_apply' are skipped
# because they represent intent, not an actual interaction with an employer.
STATUS_MAP: dict[str, str] = {
    "applied":        "applied",
    "resume_viewed":  "resume_viewed",
    "hr_response":    "hr_response",
    "rejected":       "rejected",
    "wont-do":        "rejected",
    "invited":        "invited",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _md5(value: str) -> str:
    """Return hex MD5 digest of a UTF-8 string.

    Args:
        value: Input string to hash.

    Returns:
        32-character lowercase hex string.
    """
    return hashlib.md5(value.encode("utf-8")).hexdigest()


def _now() -> datetime:
    """Return current UTC time without timezone info (Iceberg TimestampType)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_frontmatter(path: Path) -> Optional[dict]:
    """Parse YAML frontmatter from a Markdown file.

    Expects frontmatter delimited by `---` at the start of the file.
    Returns None if the file has no valid frontmatter block.

    Args:
        path: Path to the Markdown file.

    Returns:
        Dict of frontmatter fields, or None if not parseable.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None

    # Match ---\n...\n--- at start of file
    match = re.match(r"^---\r?\n(.*?)\r?\n---", text, re.DOTALL)
    if not match:
        return None

    try:
        return yaml.safe_load(match.group(1)) or {}
    except yaml.YAMLError:
        return None


def extract_resume_version(cv_field: Optional[str]) -> Optional[str]:
    """Extract resume version slug from the `cv:` frontmatter field.

    Handles plain paths and Obsidian wikilinks (``[[filename]]``).
    Takes the file stem as the version identifier.
    E.g. ``resume/by-vacancy/ozon-cv.md`` → ``ozon-cv``.
    E.g. ``[[plata-manakov-cv.pdf]]`` → ``plata-manakov-cv``.

    Args:
        cv_field: Value of the ``cv:`` frontmatter field, or None.

    Returns:
        Stem of the cv filename, or None if field is absent.
    """
    if not cv_field:
        return None
    value = str(cv_field).strip()
    # Strip Obsidian wikilink syntax: [[name]] or [[name|alias]]
    wikilink = re.match(r"^\[\[([^\]|]+)", value)
    if wikilink:
        value = wikilink.group(1).strip()
    return Path(value).stem or None


def occurred_at_from_frontmatter(fm: dict) -> Optional[datetime]:
    """Convert the `updated:` frontmatter field to a datetime.

    Args:
        fm: Parsed frontmatter dict.

    Returns:
        Datetime at midnight UTC, or None if field is absent/invalid.
    """
    updated = fm.get("updated")
    if updated is None:
        return None
    # yaml.safe_load parses YYYY-MM-DD as a date object
    if hasattr(updated, "year"):
        return datetime(updated.year, updated.month, updated.day)
    try:
        from datetime import date
        d = datetime.strptime(str(updated), "%Y-%m-%d")
        return d
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_existing_event_ids(catalog) -> set[str]:
    """Load all existing event_id_bk values from gold.hub_hr_interaction.

    Used to skip already-logged events on subsequent runs (idempotency).

    Args:
        catalog: Connected PyIceberg catalog instance.

    Returns:
        Set of event_id_bk strings already present in the hub table.
    """
    try:
        table = patch_table_io(catalog.load_table("gold.hub_hr_interaction"))
        rows = table.scan(selected_fields=("event_id_bk",)).to_arrow().to_pylist()
        return {r["event_id_bk"] for r in rows}
    except Exception:
        # Table might be empty or not yet initialized — treat as empty set
        return set()


def _normalize_url(url: str) -> str:
    """Normalize a vacancy URL for cross-domain matching.

    HH posts the same vacancy on hh.ru and hh.kz — the scraper uses hh.kz
    (Almaty account) while the candidate may have applied via hh.ru. Strip
    the domain and keep only the path so both resolve to the same key.

    E.g. ``https://hh.kz/vacancy/123456`` and ``https://hh.ru/vacancy/123456``
    both become ``/vacancy/123456``.

    Args:
        url: Raw vacancy URL.

    Returns:
        Path-only string for HH URLs; original URL for other sources.
    """
    m = re.match(r"https?://(?:[\w-]+\.)*hh\.[a-z]+(/vacancy/\d+)", url)
    return m.group(1) if m else url


def load_vacancy_url_to_hk(catalog) -> dict[str, str]:
    """Build a lookup map from normalized vacancy URL to hub_vacancy_hk.

    Used to resolve the FK in link_interaction_vacancy. Vacancies that exist
    in Obsidian but were never scraped (e.g. direct applications) won't be
    present — their hub_vacancy_hk will be NULL.

    HH URLs are normalized (domain stripped) to match across hh.ru / hh.kz.

    Args:
        catalog: Connected PyIceberg catalog instance.

    Returns:
        Dict mapping normalized url → hub_vacancy_hk.
    """
    try:
        table = patch_table_io(catalog.load_table("gold.sat_vacancy_details"))
        rows = table.scan(selected_fields=("hub_vacancy_hk", "url")).to_arrow().to_pylist()
        return {_normalize_url(r["url"]): r["hub_vacancy_hk"] for r in rows if r.get("url")}
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Scanning
# ---------------------------------------------------------------------------

def scan_vacancy_files(vault_path: Path) -> list[dict]:
    """Scan hire/vacancy/*.md and extract loggable interaction events.

    Each file may produce at most one event (the current status). Files with
    status 'new' or 'to_apply' are skipped.

    Args:
        vault_path: Root path of the Obsidian vault.

    Returns:
        List of dicts with keys: vacancy_url, status, event_type,
        resume_version, occurred_at, notes, event_id_bk.
    """
    vacancy_dir = vault_path / VACANCY_DIR
    if not vacancy_dir.exists():
        print(f"[warn] vacancy directory not found: {vacancy_dir}")
        return []

    events = []
    for md_file in sorted(vacancy_dir.glob("*.md")):
        fm = parse_frontmatter(md_file)
        if not fm:
            continue

        status = str(fm.get("status", "")).strip()
        event_type = STATUS_MAP.get(status)
        if not event_type:
            # 'new', 'to_apply', or unknown — not an interaction event
            continue

        vacancy_url = str(fm.get("link", "")).strip()
        if not vacancy_url:
            print(f"  [warn] {md_file.name}: missing `link:` field, skipping")
            continue

        occurred_at = occurred_at_from_frontmatter(fm)
        if not occurred_at:
            print(f"  [warn] {md_file.name}: missing or invalid `updated:` field, skipping")
            continue

        resume_version = extract_resume_version(fm.get("cv"))
        notes = fm.get("hr_notes")
        if notes:
            notes = str(notes).strip() or None

        # Business key: deterministic from (url, status, date) — stable across runs
        event_id_bk = _md5(f"{vacancy_url}||{status}||{occurred_at.date()}")

        events.append({
            "vacancy_url":    vacancy_url,
            "status":         status,
            "event_type":     event_type,
            "resume_version": resume_version,
            "occurred_at":    occurred_at,
            "notes":          notes,
            "event_id_bk":    event_id_bk,
            "file":           md_file.name,
        })

    return events


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------

def write_events(catalog, new_events: list[dict], url_to_hk: dict[str, str]) -> None:
    """Write new interaction events to the three Gold Data Vault tables.

    Appends atomically to:
      - gold.hub_hr_interaction
      - gold.link_interaction_vacancy
      - gold.sat_interaction_details

    Args:
        catalog: Connected PyIceberg catalog instance.
        new_events: List of event dicts to write (already deduplicated).
        url_to_hk: Lookup map from vacancy URL to hub_vacancy_hk.
    """
    now = _now()

    hub_rows: list[dict] = []
    link_rows: list[dict] = []
    sat_rows: list[dict] = []

    for ev in new_events:
        hub_hk = _md5(ev["event_id_bk"])
        hub_vacancy_hk = url_to_hk.get(_normalize_url(ev["vacancy_url"]))  # None if not in pipeline

        # link_hk: MD5 of driving key + hub_vacancy_hk (or url if hk unknown)
        link_partner = hub_vacancy_hk or ev["vacancy_url"]
        link_hk = _md5(f"{hub_hk}||{link_partner}")

        # hash_diff tracks changes to descriptive attributes
        rv = ev["resume_version"] or ""
        notes = ev["notes"] or ""
        hash_diff = _md5(f"{ev['event_type']}||{rv}||{notes}")

        hub_rows.append({
            "hub_hr_interaction_hk": hub_hk,
            "event_id_bk":           ev["event_id_bk"],
            "load_dts":              now,
            "rec_src":               REC_SRC,
        })
        link_rows.append({
            "link_hk":                 link_hk,
            "hub_hr_interaction_hk":   hub_hk,
            "hub_vacancy_hk":          hub_vacancy_hk,
            "vacancy_url":             ev["vacancy_url"],
            "load_dts":                now,
            "rec_src":                 REC_SRC,
        })
        sat_rows.append({
            "hub_hr_interaction_hk": hub_hk,
            "load_dts":              now,
            "rec_src":               REC_SRC,
            "hash_diff":             hash_diff,
            "event_type":            ev["event_type"],
            "resume_version":        ev["resume_version"],
            "occurred_at":           ev["occurred_at"],
            "notes":                 ev["notes"],
        })

    def _append_hub(tbl, rows: list[dict]) -> None:
        schema = pa.schema([
            pa.field("hub_hr_interaction_hk", pa.string(),        nullable=False),
            pa.field("event_id_bk",           pa.string(),        nullable=False),
            pa.field("load_dts",              pa.timestamp("us"), nullable=False),
            pa.field("rec_src",               pa.string(),        nullable=False),
        ])
        arrow = pa.table({
            "hub_hr_interaction_hk": pa.array([r["hub_hr_interaction_hk"] for r in rows], pa.string()),
            "event_id_bk":           pa.array([r["event_id_bk"]           for r in rows], pa.string()),
            "load_dts":              pa.array([r["load_dts"]              for r in rows], pa.timestamp("us")),
            "rec_src":               pa.array([r["rec_src"]               for r in rows], pa.string()),
        }, schema=schema)
        tbl.append(arrow)

    def _append_link(tbl, rows: list[dict]) -> None:
        schema = pa.schema([
            pa.field("link_hk",                pa.string(),        nullable=False),
            pa.field("hub_hr_interaction_hk",  pa.string(),        nullable=False),
            pa.field("hub_vacancy_hk",         pa.string(),        nullable=True),
            pa.field("vacancy_url",            pa.string(),        nullable=False),
            pa.field("load_dts",               pa.timestamp("us"), nullable=False),
            pa.field("rec_src",                pa.string(),        nullable=False),
        ])
        arrow = pa.table({
            "link_hk":               pa.array([r["link_hk"]               for r in rows], pa.string()),
            "hub_hr_interaction_hk": pa.array([r["hub_hr_interaction_hk"] for r in rows], pa.string()),
            "hub_vacancy_hk":        pa.array([r["hub_vacancy_hk"]        for r in rows], pa.string()),
            "vacancy_url":           pa.array([r["vacancy_url"]           for r in rows], pa.string()),
            "load_dts":              pa.array([r["load_dts"]              for r in rows], pa.timestamp("us")),
            "rec_src":               pa.array([r["rec_src"]               for r in rows], pa.string()),
        }, schema=schema)
        tbl.append(arrow)

    def _append_sat(tbl, rows: list[dict]) -> None:
        schema = pa.schema([
            pa.field("hub_hr_interaction_hk", pa.string(),        nullable=False),
            pa.field("load_dts",              pa.timestamp("us"), nullable=False),
            pa.field("rec_src",               pa.string(),        nullable=False),
            pa.field("hash_diff",             pa.string(),        nullable=False),
            pa.field("event_type",            pa.string(),        nullable=False),
            pa.field("resume_version",        pa.string(),        nullable=True),
            pa.field("occurred_at",           pa.timestamp("us"), nullable=False),
            pa.field("notes",                 pa.string(),        nullable=True),
        ])
        arrow = pa.table({
            "hub_hr_interaction_hk": pa.array([r["hub_hr_interaction_hk"] for r in rows], pa.string()),
            "load_dts":              pa.array([r["load_dts"]              for r in rows], pa.timestamp("us")),
            "rec_src":               pa.array([r["rec_src"]               for r in rows], pa.string()),
            "hash_diff":             pa.array([r["hash_diff"]             for r in rows], pa.string()),
            "event_type":            pa.array([r["event_type"]            for r in rows], pa.string()),
            "resume_version":        pa.array([r["resume_version"]        for r in rows], pa.string()),
            "occurred_at":           pa.array([r["occurred_at"]           for r in rows], pa.timestamp("us")),
            "notes":                 pa.array([r["notes"]                 for r in rows], pa.string()),
        }, schema=schema)
        tbl.append(arrow)

    hub_table  = patch_table_io(catalog.load_table("gold.hub_hr_interaction"))
    link_table = patch_table_io(catalog.load_table("gold.link_interaction_vacancy"))
    sat_table  = patch_table_io(catalog.load_table("gold.sat_interaction_details"))

    _append_hub(hub_table, hub_rows)
    _append_link(link_table, link_rows)
    _append_sat(sat_table, sat_rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    """Sync HR interaction events from vacancy MD files to Gold Iceberg tables."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--vault-path",
        type=Path,
        default=DEFAULT_VAULT_PATH,
        help="Path to Obsidian vault root",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print events that would be logged without writing to Iceberg",
    )
    args = parser.parse_args()

    vault_path: Path = args.vault_path
    dry_run: bool = args.dry_run

    print(f"Scanning {vault_path / VACANCY_DIR} ...\n")

    catalog = get_catalog()

    # Load existing event IDs for deduplication
    existing_ids = load_existing_event_ids(catalog)
    print(f"  existing events in hub: {len(existing_ids)}")

    # Load vacancy URL → hub_vacancy_hk for FK resolution
    url_to_hk = load_vacancy_url_to_hk(catalog)
    print(f"  vacancy pipeline size:  {len(url_to_hk)} URLs\n")

    # Scan MD files
    all_events = scan_vacancy_files(vault_path)
    print(f"  found {len(all_events)} loggable events in vacancy notes")

    # Deduplicate
    new_events = [ev for ev in all_events if ev["event_id_bk"] not in existing_ids]
    print(f"  new events to log:      {len(new_events)}\n")

    if not new_events:
        print("Synced 0 new events.")
        return

    if dry_run:
        print("[dry-run] would log:")
        for ev in new_events:
            hk_tag = "in-pipeline" if ev["vacancy_url"] in url_to_hk else "NOT in pipeline"
            print(
                f"  {ev['file']}: {ev['event_type']}"
                f" | resume={ev['resume_version'] or '(none)'}"
                f" | {hk_tag}"
            )
        return

    write_events(catalog, new_events, url_to_hk)

    # Summary by event type
    by_type: dict[str, int] = {}
    for ev in new_events:
        by_type[ev["event_type"]] = by_type.get(ev["event_type"], 0) + 1
    summary = ", ".join(f"{v}× {k}" for k, v in sorted(by_type.items()))
    print(f"Synced {len(new_events)} new event(s): {summary}")


if __name__ == "__main__":
    main()
