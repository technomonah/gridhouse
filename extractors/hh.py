"""HH.ru extractor — Bronze layer.

Fetches vacancies from HH.ru public REST API (no OAuth required for search)
and writes raw records to the bronze.hh_vacancies Iceberg table.

Incrementality: deduplication by vacancy_id. On each run, existing IDs are
loaded upfront; vacancies already in the table are skipped. This is more
reliable than date-based cursors because HH API caps pagination at 2000
results per query regardless of date filters.

Pre-filter: same keyword matching as other extractors. Both passing and
non-passing vacancies are written to Bronze (passed_prefilter flag).

Usage:
    python extractors/hh.py scrape              # all active HH queries from meta.sources
    python extractors/hh.py scrape "dbt"        # run a one-off query (no area/schedule filter)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io
from extractors.common import RawVacancy, passes_prefilter

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Required by HH.ru TOS — identify your application.
# Format: "AppName/1.0 (contact@example.com)"
HH_USER_AGENT = os.environ.get("HH_USER_AGENT", "grindhouse-extractor/1.0")

# HH API returns up to 100 results per page (hard limit).
HH_PER_PAGE = int(os.environ.get("HH_PER_PAGE", "100"))

# Safety cap — HH API silently caps total results at 2000 (20 pages × 100).
HH_MAX_PAGES = int(os.environ.get("HH_MAX_PAGES", "20"))

# Seconds to sleep between page requests to avoid rate limiting.
HH_PAGE_DELAY = float(os.environ.get("HH_PAGE_DELAY", "0.5"))

HH_API_BASE = "https://api.hh.ru"

# ---------------------------------------------------------------------------
# Iceberg helpers
# ---------------------------------------------------------------------------

def get_seen_vacancy_ids(catalog) -> set[str]:
    """Load all vacancy_ids already stored in bronze.hh_vacancies.

    Used for deduplication — vacancies already in the table are skipped
    on subsequent runs without re-writing identical records.

    Args:
        catalog: Connected PyIceberg catalog.

    Returns:
        Set of vacancy_id strings already present in the table.
    """
    table = patch_table_io(catalog.load_table("bronze.hh_vacancies"))
    scan = table.scan(selected_fields=("vacancy_id",))
    arrow = scan.to_arrow()
    if arrow.num_rows == 0:
        return set()
    return set(arrow.column("vacancy_id").to_pylist())


def write_vacancies(catalog, vacancies: list[RawVacancy]) -> None:
    """Append a batch of raw vacancies to bronze.hh_vacancies.

    Args:
        catalog: Connected PyIceberg catalog.
        vacancies: List of RawVacancy instances from the HH extractor.
    """
    if not vacancies:
        return
    table = patch_table_io(catalog.load_table("bronze.hh_vacancies"))
    arrow_schema = pa.schema([
        pa.field("vacancy_id",       pa.string(),         nullable=False),
        pa.field("vacancy_url",      pa.string(),         nullable=False),
        pa.field("vacancy_name",     pa.string(),         nullable=False),
        pa.field("employer_id",      pa.string(),         nullable=True),
        pa.field("employer_name",    pa.string(),         nullable=False),
        pa.field("salary_from",      pa.int64(),          nullable=True),
        pa.field("salary_to",        pa.int64(),          nullable=True),
        pa.field("salary_currency",  pa.string(),         nullable=True),
        pa.field("salary_gross",     pa.bool_(),          nullable=True),
        pa.field("area_id",          pa.string(),         nullable=False),
        pa.field("area_name",        pa.string(),         nullable=False),
        pa.field("experience",       pa.string(),         nullable=False),
        pa.field("employment",       pa.string(),         nullable=False),
        pa.field("schedule",         pa.string(),         nullable=False),
        pa.field("snippet_req",      pa.string(),         nullable=True),
        pa.field("snippet_resp",     pa.string(),         nullable=True),
        pa.field("published_at",     pa.timestamp("us"),  nullable=False),
        pa.field("extracted_at",     pa.timestamp("us"),  nullable=False),
        pa.field("search_query",     pa.string(),         nullable=False),
        pa.field("passed_prefilter", pa.bool_(),          nullable=False),
    ])
    arrow_table = pa.table({
        "vacancy_id":       pa.array([v.source_id                  for v in vacancies], type=pa.string()),
        "vacancy_url":      pa.array([v.url                        for v in vacancies], type=pa.string()),
        "vacancy_name":     pa.array([v.extra["vacancy_name"]      for v in vacancies], type=pa.string()),
        "employer_id":      pa.array([v.extra["employer_id"]       for v in vacancies], type=pa.string()),
        "employer_name":    pa.array([v.extra["employer_name"]     for v in vacancies], type=pa.string()),
        "salary_from":      pa.array([v.extra["salary_from"]       for v in vacancies], type=pa.int64()),
        "salary_to":        pa.array([v.extra["salary_to"]         for v in vacancies], type=pa.int64()),
        "salary_currency":  pa.array([v.extra["salary_currency"]   for v in vacancies], type=pa.string()),
        "salary_gross":     pa.array([v.extra["salary_gross"]      for v in vacancies], type=pa.bool_()),
        "area_id":          pa.array([v.extra["area_id"]           for v in vacancies], type=pa.string()),
        "area_name":        pa.array([v.extra["area_name"]         for v in vacancies], type=pa.string()),
        "experience":       pa.array([v.extra["experience"]        for v in vacancies], type=pa.string()),
        "employment":       pa.array([v.extra["employment"]        for v in vacancies], type=pa.string()),
        "schedule":         pa.array([v.extra["schedule"]          for v in vacancies], type=pa.string()),
        "snippet_req":      pa.array([v.extra["snippet_req"]       for v in vacancies], type=pa.string()),
        "snippet_resp":     pa.array([v.extra["snippet_resp"]      for v in vacancies], type=pa.string()),
        "published_at":     pa.array([v.published_at               for v in vacancies], type=pa.timestamp("us")),
        "extracted_at":     pa.array([v.extracted_at               for v in vacancies], type=pa.timestamp("us")),
        "search_query":     pa.array([v.extra["search_query"]      for v in vacancies], type=pa.string()),
        "passed_prefilter": pa.array([v.passed_prefilter           for v in vacancies], type=pa.bool_()),
    }, schema=arrow_schema)
    table.append(arrow_table)


def load_active_hh_searches(catalog) -> list[dict]:
    """Load active HH.ru search configs from meta.sources.

    Args:
        catalog: Connected PyIceberg catalog.

    Returns:
        List of config dicts with keys: query, area (optional), schedule (optional).
    """
    table = patch_table_io(catalog.load_table("meta.sources"))
    from pyiceberg.expressions import And, EqualTo
    scan = table.scan(
        row_filter=And(EqualTo("source_type", "hh"), EqualTo("active", True)),
        selected_fields=("config",),
    )
    searches = []
    for config_str in scan.to_arrow().column("config").to_pylist():
        searches.append(json.loads(config_str))
    return searches


# ---------------------------------------------------------------------------
# HH API
# ---------------------------------------------------------------------------

def _parse_vacancy(item: dict, search_query: str, extracted_at: datetime) -> RawVacancy:
    """Parse a single vacancy item from HH API search response.

    Args:
        item: Raw vacancy dict from HH /vacancies response.
        search_query: The search query that produced this result.
        extracted_at: Timestamp of this extraction run.

    Returns:
        RawVacancy instance with HH-specific fields in ``extra``.
    """
    salary = item.get("salary") or {}
    # salary_range supersedes salary in newer API versions
    salary_range = item.get("salary_range") or salary

    employer = item.get("employer") or {}
    area = item.get("area") or {}
    experience = item.get("experience") or {}
    employment = item.get("employment") or {}
    schedule = item.get("schedule") or {}
    snippet = item.get("snippet") or {}

    # Combine name + snippets for pre-filter check
    prefilter_text = " ".join(filter(None, [
        item.get("name", ""),
        snippet.get("requirement", ""),
        snippet.get("responsibility", ""),
    ]))

    published_raw = item.get("published_at", "")
    try:
        published_at = datetime.fromisoformat(published_raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except (ValueError, AttributeError):
        published_at = extracted_at

    salary_from = salary_range.get("from")
    salary_to = salary_range.get("to")
    vacancy_id = str(item["id"])

    return RawVacancy(
        source="hh",
        source_id=vacancy_id,
        url=item.get("alternate_url", ""),
        text=prefilter_text,
        published_at=published_at,
        extracted_at=extracted_at,
        passed_prefilter=passes_prefilter(prefilter_text),
        extra={
            "vacancy_name":    item.get("name", ""),
            "employer_id":     str(employer["id"]) if employer.get("id") else None,
            "employer_name":   employer.get("name", ""),
            "salary_from":     int(salary_from) if salary_from is not None else None,
            "salary_to":       int(salary_to) if salary_to is not None else None,
            "salary_currency": salary_range.get("currency"),
            "salary_gross":    salary_range.get("gross"),
            "area_id":         str(area.get("id", "")),
            "area_name":       area.get("name", ""),
            "experience":      experience.get("id", ""),
            "employment":      employment.get("id", ""),
            "schedule":        schedule.get("id", ""),
            "snippet_req":     snippet.get("requirement"),
            "snippet_resp":    snippet.get("responsibility"),
            "search_query":    search_query,
        },
    )


def fetch_vacancies(search_config: dict) -> list[dict]:
    """Fetch all pages of vacancies for a single search config from HH API.

    Paginates through results until no more pages or HH_MAX_PAGES is reached.
    HH API silently caps at 2000 total results (20 pages × 100) per query.

    Args:
        search_config: Dict with keys:
            - query (str): Search text.
            - area (str, optional): HH area code (e.g. "160" for Almaty).
            - schedule (str, optional): e.g. "remote".

    Returns:
        List of parsed vacancy dicts ready to write to Bronze.
    """
    query = search_config["query"]
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    params: dict = {
        "text":            query,
        "per_page":        HH_PER_PAGE,
        "enable_snippets": "true",
    }
    if search_config.get("area"):
        params["area"] = search_config["area"]
    if search_config.get("schedule"):
        params["schedule"] = search_config["schedule"]
    if search_config.get("professional_role"):
        params["professional_role"] = search_config["professional_role"]

    headers = {"User-Agent": HH_USER_AGENT}
    results = []

    for page in range(HH_MAX_PAGES):
        params["page"] = page
        try:
            resp = requests.get(
                f"{HH_API_BASE}/vacancies",
                params=params,
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [warn] page {page}: request failed: {e}")
            break

        data = resp.json()
        items = data.get("items", [])
        total_pages = data.get("pages", 0)

        for item in items:
            results.append(_parse_vacancy(item, query, extracted_at))

        print(f"    page {page + 1}/{total_pages}: got {len(items)} vacancies")

        if page + 1 >= total_pages:
            break

        time.sleep(HH_PAGE_DELAY)

    return results


# ---------------------------------------------------------------------------
# Scrape
# ---------------------------------------------------------------------------

def scrape_search(catalog, search_config: dict, seen_ids: set[str]) -> tuple[int, int]:
    """Fetch new vacancies for one search config and write to Bronze.

    Skips vacancy IDs already present in bronze.hh_vacancies.

    Args:
        catalog: Connected PyIceberg catalog.
        search_config: Dict with query, area, schedule keys.
        seen_ids: Set of vacancy_ids already in the table (mutated in-place).

    Returns:
        Tuple of (total_new_written, passed_prefilter_count).
    """
    query = search_config["query"]
    area = search_config.get("area", "global")
    schedule = search_config.get("schedule", "any")
    role = search_config.get("professional_role", "")
    role_str = f"  role={role}" if role else ""
    print(f"  query={query!r}  area={area}  schedule={schedule}{role_str}")

    all_vacancies = fetch_vacancies(search_config)

    new_vacancies = [v for v in all_vacancies if v.source_id not in seen_ids]
    passed = sum(1 for v in new_vacancies if v.passed_prefilter)

    if new_vacancies:
        write_vacancies(catalog, new_vacancies)
        for v in new_vacancies:
            seen_ids.add(v.source_id)

    skipped = len(all_vacancies) - len(new_vacancies)
    print(f"  fetched={len(all_vacancies)}, new={len(new_vacancies)}, skipped={skipped}, passed_prefilter={passed}")
    return len(new_vacancies), passed


def cmd_scrape(searches: list[dict]) -> None:
    """Scrape all given search configs and write raw vacancies to Bronze.

    Args:
        searches: List of search config dicts (query, area, schedule).
    """
    catalog = get_catalog()
    seen_ids = get_seen_vacancy_ids(catalog)
    print(f"Loaded {len(seen_ids)} existing vacancy IDs for deduplication.\n")

    total_written = 0
    total_passed = 0

    for search_config in searches:
        written, passed = scrape_search(catalog, search_config, seen_ids)
        total_written += written
        total_passed += passed
        print()

    print(f"Done. written={total_written}, passed_prefilter={total_passed}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI entry point for the HH.ru extractor."""
    args = sys.argv[1:]

    if not args or args[0] == "scrape":
        if len(args) > 1:
            # One-off query passed as CLI argument — no area/schedule filter
            searches = [{"query": " ".join(args[1:])}]
        else:
            catalog = get_catalog()
            searches = load_active_hh_searches(catalog)
            if not searches:
                print("No active HH searches found in meta.sources. Run `make init` first.")
                sys.exit(1)

        print(f"Scraping {len(searches)} HH search(es)...\n")
        cmd_scrape(searches)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
