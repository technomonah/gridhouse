"""Export scored vacancies to Obsidian markdown notes.

Reads from Gold Iceberg tables (via PyIceberg + Nessie catalog), filters to
vacancies with recommended_action IN ('apply', 'priority_apply'), and writes
one .md file per vacancy to the hire/vacancy/ directory of the Obsidian vault.

Files are named <employer_slug>-<role_slug>.md (lowercase, hyphens). Existing
files are skipped — export is idempotent.

After the dbt migration, tables are accessed directly as:
  - gold.hub_vacancy       — hub_vacancy_hk + content_hash_bk
  - gold.sat_vacancy_details — url, text, published_at
  - gold.sat_vacancy_source  — salary, area, experience, employment, schedule
  - gold.sat_vacancy_score   — score, reasoning, recommended_action
  - gold.hub_company         — hub_company_hk + company_name_bk
  - gold.sat_company_details — area_name (company-level)
  - gold.link_vacancy_company — hub_vacancy_hk → hub_company_hk
  - silver.hh_vacancies      — vacancy_name (title not stored in gold)

Usage:
    python scripts/export_vacancies.py [--vault-path PATH] [--dry-run]

Flags:
    --vault-path PATH: Path to Obsidian vault root (default: ~/Desktop/vault47).
    --dry-run:         Print which files would be created without writing them.

Environment variables:
    NESSIE_URI          — Nessie REST endpoint (default: http://localhost:19120/iceberg/).
    MINIO_ROOT_USER     — MinIO access key.
    MINIO_ROOT_PASSWORD — MinIO secret key.
    S3_ENDPOINT         — MinIO S3 endpoint (default: http://localhost:9000).
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_VAULT_PATH = Path.home() / "Desktop" / "vault47"
VACANCY_DIR = "hire/vacancy"
ACTIONS_TO_EXPORT = {"apply", "priority_apply"}


# ---------------------------------------------------------------------------
# Slug helpers
# ---------------------------------------------------------------------------

def slugify(text: str, max_words: int = 4) -> str:
    """Convert text to a lowercase hyphen-separated slug.

    Args:
        text: Input string (company name, vacancy title, etc.).
        max_words: Maximum number of words to include in the slug.

    Returns:
        Lowercase hyphen-separated slug, e.g. 'yandex-data-engineer'.
    """
    # Lowercase and replace non-alphanumeric characters with spaces
    text = text.lower()
    text = re.sub(r"[^\w\s-]", " ", text)
    text = re.sub(r"[\s_-]+", " ", text).strip()
    words = text.split()[:max_words]
    return "-".join(words) if words else "unknown"


def make_filename(employer_name: str | None, vacancy_name: str | None, hub_vacancy_hk: str = "") -> str:
    """Build output filename from employer and vacancy names.

    For vacancies without both employer and title (e.g. TG posts), appends a
    4-char hash suffix to avoid filename collisions.

    Args:
        employer_name: Company name (may be None for anonymous postings).
        vacancy_name: Vacancy title (may be None for TG posts without title).
        hub_vacancy_hk: Hub vacancy hash key — used as a disambiguation suffix
            when both employer_name and vacancy_name are None.

    Returns:
        Filename without directory, e.g. 'yandex-data-engineer.md' or
        'unknown-vacancy-a3f9.md' for anonymous TG posts.
    """
    employer_slug = slugify(employer_name or "unknown", max_words=2)
    role_slug = slugify(vacancy_name or "vacancy", max_words=3)
    base = f"{employer_slug}-{role_slug}"
    # Add hash suffix to avoid collisions when both name fields are absent
    if not employer_name and not vacancy_name and hub_vacancy_hk:
        base = f"{base}-{hub_vacancy_hk[:4]}"
    return f"{base}.md"


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_export_data(catalog) -> list[dict]:
    """Load all data needed for export from Gold Iceberg tables.

    Joins hub_vacancy → sat_vacancy_score (filter apply/priority_apply) →
    sat_vacancy_details → sat_vacancy_source → link_vacancy_company →
    hub_company + sat_company_details → silver.hh_vacancies (for vacancy_name).

    Args:
        catalog: Connected PyIceberg catalog instance.

    Returns:
        List of dicts, one per vacancy to export. Each dict contains:
        hub_vacancy_hk, content_hash_bk, score, reasoning, recommended_action,
        url, text, published_at, salary_from, salary_to, salary_currency,
        salary_gross, area_name, experience, employment, schedule,
        employer_name, vacancy_name.
    """
    def load(table_name: str, fields: tuple[str, ...]) -> list[dict]:
        """Load selected fields from an Iceberg table, return as list of dicts."""
        try:
            tbl = patch_table_io(catalog.load_table(table_name))
            return tbl.scan(selected_fields=fields).to_arrow().to_pylist()
        except Exception as exc:
            print(f"  [warn] could not load {table_name}: {exc}")
            return []

    # Load all relevant tables
    hub_rows      = load("gold.hub_vacancy",        ("hub_vacancy_hk", "content_hash_bk"))
    score_rows    = load("gold.sat_vacancy_score",  ("hub_vacancy_hk", "score", "reasoning", "recommended_action"))
    detail_rows   = load("gold.sat_vacancy_details", ("hub_vacancy_hk", "url", "text", "published_at", "published_at_approx"))
    source_rows   = load("gold.sat_vacancy_source",  ("hub_vacancy_hk", "salary_from", "salary_to",
                                                       "salary_currency", "salary_gross",
                                                       "area_name", "experience", "employment", "schedule"))
    link_rows     = load("gold.link_vacancy_company", ("hub_vacancy_hk", "hub_company_hk"))
    company_rows  = load("gold.hub_company",          ("hub_company_hk", "company_name_bk"))
    hh_rows       = load("silver.hh_vacancies",       ("content_hash", "vacancy_name"))

    # Build lookup indexes — deduplicate scores by taking highest score per hk
    score_map: dict[str, dict] = {}
    for r in score_rows:
        hk = r["hub_vacancy_hk"]
        existing = score_map.get(hk)
        if existing is None or (r.get("score") or 0) > (existing.get("score") or 0):
            score_map[hk] = r
    detail_map  = {r["hub_vacancy_hk"]: r for r in detail_rows}
    source_map  = {r["hub_vacancy_hk"]: r for r in source_rows}
    link_map    = {r["hub_vacancy_hk"]: r["hub_company_hk"] for r in link_rows}
    company_map = {r["hub_company_hk"]: r["company_name_bk"] for r in company_rows}
    hh_name_map = {r["content_hash"]: r["vacancy_name"] for r in hh_rows}

    results = []
    for hub in hub_rows:
        hk = hub["hub_vacancy_hk"]
        score_data = score_map.get(hk, {})

        # Only export apply + priority_apply
        action = score_data.get("recommended_action")
        if action not in ACTIONS_TO_EXPORT:
            continue

        detail = detail_map.get(hk, {})
        source = source_map.get(hk, {})
        company_hk = link_map.get(hk)
        employer_name = company_map.get(company_hk) if company_hk else None

        # Vacancy title: from hh_vacancies by content_hash_bk, else None (TG/LinkedIn)
        vacancy_name = hh_name_map.get(hub["content_hash_bk"])

        results.append({
            "hub_vacancy_hk":     hk,
            "content_hash_bk":    hub["content_hash_bk"],
            "score":              score_data.get("score"),
            "reasoning":          score_data.get("reasoning"),
            "recommended_action": action,
            "url":                detail.get("url"),
            "text":               detail.get("text", ""),
            "published_at":       detail.get("published_at") or detail.get("published_at_approx"),
            "salary_from":        source.get("salary_from"),
            "salary_to":          source.get("salary_to"),
            "salary_currency":    source.get("salary_currency"),
            "salary_gross":       source.get("salary_gross"),
            "area_name":          source.get("area_name"),
            "experience":         source.get("experience"),
            "employment":         source.get("employment"),
            "schedule":           source.get("schedule"),
            "employer_name":      employer_name,
            "vacancy_name":       vacancy_name,
        })

    return results


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------

def _fmt_salary(salary_from, salary_to, currency, gross) -> str:
    """Format salary range as a human-readable string.

    Args:
        salary_from: Lower bound (int or None).
        salary_to: Upper bound (int or None).
        currency: Currency code string (e.g. 'RUB') or None.
        gross: True if gross (before tax), False if net, None if unknown.

    Returns:
        Formatted salary string, e.g. '100 000–150 000 RUB (gross)' or 'Not specified'.
    """
    if salary_from is None and salary_to is None:
        return "Not specified"

    parts = []
    if salary_from and salary_to:
        parts.append(f"{salary_from:,}–{salary_to:,}".replace(",", " "))
    elif salary_from:
        parts.append(f"from {salary_from:,}".replace(",", " "))
    elif salary_to:
        parts.append(f"up to {salary_to:,}".replace(",", " "))

    if currency:
        parts.append(currency)
    if gross is True:
        parts.append("(gross)")
    elif gross is False:
        parts.append("(net)")

    return " ".join(parts)


def _fmt_date(ts) -> str:
    """Convert a timestamp or date to YYYY-MM-DD string.

    Args:
        ts: datetime, date, or ISO string. May be None.

    Returns:
        YYYY-MM-DD string, or 'unknown' if ts is None.
    """
    if ts is None:
        return "unknown"
    if isinstance(ts, datetime):
        return ts.strftime("%Y-%m-%d")
    if isinstance(ts, date):
        return ts.isoformat()
    try:
        return datetime.fromisoformat(str(ts)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(ts)


def _extract_tech_stack(text: str | None) -> list[str]:
    """Extract a rough tech-stack list from vacancy text using keyword matching.

    This is a simple heuristic — not an AI call. Covers the most common
    technologies relevant to Analytics Engineer / Data Engineer roles.

    Args:
        text: Raw vacancy text. May be None.

    Returns:
        List of matched technology keywords found in the text.
    """
    if not text:
        return []

    keywords = [
        "dbt", "ClickHouse", "Spark", "Airflow", "Kafka", "Flink",
        "PostgreSQL", "MySQL", "MongoDB", "Redis", "Elasticsearch",
        "Python", "SQL", "Scala", "Java", "Go",
        "Kubernetes", "Docker", "Terraform", "Ansible",
        "AWS", "GCP", "Azure", "S3", "Iceberg",
        "Trino", "Presto", "Hive", "Hadoop",
        "Tableau", "Superset", "Grafana", "Metabase",
        "DataBricks", "Databricks", "dbt Cloud", "Snowflake",
        "FastAPI", "Django", "Flask",
        "GitLab", "GitHub",
    ]

    text_lower = text.lower()
    found = []
    seen = set()
    for kw in keywords:
        if kw.lower() in text_lower and kw not in seen:
            found.append(kw)
            seen.add(kw)

    return found[:10]  # cap at 10 to keep frontmatter readable


def render_vacancy_md(vacancy: dict) -> str:
    """Render a vacancy dict as an Obsidian markdown note.

    Args:
        vacancy: Dict with all vacancy fields (from load_export_data).

    Returns:
        Full markdown content string ready to write to disk.
    """
    employer = vacancy.get("employer_name") or "Unknown Company"
    role = vacancy.get("vacancy_name") or "Vacancy"
    url = vacancy.get("url") or ""
    score = vacancy.get("score")
    action = vacancy.get("recommended_action", "")
    reasoning = vacancy.get("reasoning") or ""
    text = vacancy.get("text") or ""
    pub_date = _fmt_date(vacancy.get("published_at"))
    today = date.today().isoformat()

    salary = _fmt_salary(
        vacancy.get("salary_from"),
        vacancy.get("salary_to"),
        vacancy.get("salary_currency"),
        vacancy.get("salary_gross"),
    )
    area = vacancy.get("area_name") or "Not specified"
    experience = vacancy.get("experience") or "Not specified"
    employment = vacancy.get("employment") or ""
    schedule = vacancy.get("schedule") or ""

    # Employment + schedule in one line
    work_format_parts = [p for p in [employment, schedule] if p]
    work_format = ", ".join(work_format_parts) if work_format_parts else "Not specified"

    tech_stack = _extract_tech_stack(text)
    tech_stack_yaml = "\n".join(f"  - {t}" for t in tech_stack) if tech_stack else "  []"

    score_str = f"{score:.1f}" if score is not None else "N/A"

    # Truncate text to first 3000 chars for the note
    text_preview = text[:3000] + ("..." if len(text) > 3000 else "")

    lines = [
        "---",
        "type: vacancy",
        f"link: {url}",
        "status: new",
        f"score: {score_str}",
        "tech-stack:",
        tech_stack_yaml,
        f"updated: {today}",
        "---",
        "",
        f"# {employer} — {role}",
        "",
        f"**Company:** {employer}",
        f"**Location:** {area}",
        f"**Salary:** {salary}",
        f"**Experience:** {experience}",
        f"**Employment:** {work_format}",
        f"**Score:** {score_str}/10 — {action}",
        f"**Published:** {pub_date}",
        "",
        "## AI Assessment",
        "",
        reasoning,
        "",
        "## Vacancy Text",
        "",
        text_preview,
        "",
        "# Сопроводительное",
        "",
    ]

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run export: load vacancies → filter → write Obsidian notes."""
    parser = argparse.ArgumentParser(
        description="Export apply/priority_apply vacancies to Obsidian markdown notes"
    )
    parser.add_argument(
        "--vault-path", type=Path, default=DEFAULT_VAULT_PATH,
        help=f"Path to Obsidian vault root (default: {DEFAULT_VAULT_PATH})"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print which files would be created without writing them"
    )
    args = parser.parse_args()

    vacancy_dir = args.vault_path / VACANCY_DIR
    if not vacancy_dir.exists():
        print(f"[error] vacancy directory not found: {vacancy_dir}")
        sys.exit(1)

    catalog = get_catalog()
    vacancies = load_export_data(catalog)
    print(f"Found {len(vacancies)} vacancies to export (apply/priority_apply)")

    # Build a set of normalized HH vacancy IDs already present on disk.
    # This prevents creating hh.ru duplicates when the same vacancy exists
    # under an hh.kz / almaty.hh.kz URL in a previously exported file.
    existing_hh_ids: set[str] = set()
    for md in vacancy_dir.glob("*.md"):
        try:
            text = md.read_text(encoding="utf-8")
            m = re.search(r"^link:\s*(.+)$", text, re.MULTILINE)
            if m:
                hh_m = re.search(r"hh\.[a-z]+(/vacancy/\d+)", m.group(1))
                if hh_m:
                    existing_hh_ids.add(hh_m.group(1))
        except OSError:
            continue

    created = 0
    skipped = 0
    for vac in vacancies:
        employer = vac.get("employer_name")
        role = vac.get("vacancy_name")
        filename = make_filename(employer, role, vac.get("hub_vacancy_hk", ""))
        out_path = vacancy_dir / filename

        # Check by filename first (fast path)
        if out_path.exists():
            skipped += 1
            continue

        # Check by normalized HH vacancy ID to avoid hh.ru vs hh.kz duplicates
        url = vac.get("url") or ""
        hh_m = re.search(r"hh\.[a-z]+(/vacancy/\d+)", url)
        if hh_m and hh_m.group(1) in existing_hh_ids:
            skipped += 1
            continue

        content = render_vacancy_md(vac)

        if args.dry_run:
            print(f"  [dry-run] would create: {filename}")
            created += 1
            continue

        out_path.write_text(content, encoding="utf-8")
        # Track the new file's HH ID so subsequent vacancies in this run
        # don't create a duplicate either
        if hh_m:
            existing_hh_ids.add(hh_m.group(1))
        print(f"  created: {filename}")
        created += 1

    action_word = "Would create" if args.dry_run else "Created"
    print(f"\n{action_word} {created} files, skipped {skipped} existing.")


if __name__ == "__main__":
    main()
