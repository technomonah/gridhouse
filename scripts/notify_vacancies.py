"""Notify about new high-score vacancies via Telegram bot.

Reads vacancies scored as ``priority_apply`` from Gold Iceberg tables and
sends a Telegram message for each one scored within the last NOTIFY_WINDOW_MINUTES
minutes. Deduplicates via a local ``.notified_hks`` JSON file — safe to run
on every hourly pipeline tick without producing duplicate alerts.

Usage:
    python scripts/notify_vacancies.py [--dry-run] [--window-minutes N]

Flags:
    --dry-run:          Print which vacancies would be notified without sending.
    --window-minutes N: Look-back window in minutes (default: 70, covers 1-hour runs).

Environment variables:
    TG_ALERT_BOT_TOKEN  — Telegram bot token from BotFather.
    TG_ALERT_CHAT_ID    — Telegram chat_id to send alerts to (your personal account).
    NESSIE_URI          — Nessie REST endpoint (default: http://localhost:19120/iceberg/).
    MINIO_ROOT_USER     — MinIO access key.
    MINIO_ROOT_PASSWORD — MinIO secret key.
    S3_ENDPOINT         — MinIO S3 endpoint (default: http://localhost:9000).

Setup:
    1. Create a bot via @BotFather → /newbot → copy token → set TG_ALERT_BOT_TOKEN.
    2. Send /start to your bot in Telegram.
    3. Fetch your chat_id: curl https://api.telegram.org/bot<TOKEN>/getUpdates
    4. Set TG_ALERT_CHAT_ID to the "id" field from the chat object.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Telegram bot credentials — must be set in environment.
TG_ALERT_BOT_TOKEN = os.environ.get("TG_ALERT_BOT_TOKEN", "")
TG_ALERT_CHAT_ID   = os.environ.get("TG_ALERT_CHAT_ID", "")

# Look-back window for "new" vacancies — slightly over 1 hour to handle
# pipeline run-time variance between hourly ticks.
DEFAULT_WINDOW_MINUTES = 70

# Path to deduplication file — stores hub_vacancy_hk of already-notified vacancies.
# Placed next to this script so it survives restarts but stays local.
NOTIFIED_HKS_FILE = Path(__file__).parent / ".notified_hks"

TG_API_BASE = "https://api.telegram.org"

# ---------------------------------------------------------------------------
# Deduplication helpers
# ---------------------------------------------------------------------------


def load_notified_hks() -> set[str]:
    """Load set of hub_vacancy_hks already notified from the local JSON file.

    Returns:
        Set of hk strings. Empty set if file does not exist or is malformed.
    """
    if not NOTIFIED_HKS_FILE.exists():
        return set()
    try:
        return set(json.loads(NOTIFIED_HKS_FILE.read_text()))
    except (json.JSONDecodeError, OSError):
        return set()


def save_notified_hks(hks: set[str]) -> None:
    """Persist the set of notified hks to the local JSON file.

    Args:
        hks: Full set of hub_vacancy_hks (existing + newly notified).
    """
    NOTIFIED_HKS_FILE.write_text(json.dumps(sorted(hks)))


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


def load_new_priority_vacancies(catalog, window_minutes: int) -> list[dict]:
    """Load priority_apply vacancies scored within the last window_minutes.

    Joins sat_vacancy_score (filter priority_apply + recent scored_at) with
    sat_vacancy_details (url), sat_vacancy_source (salary), hub_company +
    sat_company_details (company name), and silver.hh_vacancies (title).

    Args:
        catalog: Connected PyIceberg catalog.
        window_minutes: Only return vacancies scored within this many minutes.

    Returns:
        List of dicts with keys: hub_vacancy_hk, score, reasoning,
        recommended_action, url, employer_name, vacancy_name,
        salary_from, salary_to, salary_currency.
    """
    cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=window_minutes)

    def load(table_name: str, fields: tuple[str, ...]) -> list[dict]:
        """Load selected fields from an Iceberg table."""
        try:
            tbl = patch_table_io(catalog.load_table(table_name))
            return tbl.scan(selected_fields=fields).to_arrow().to_pylist()
        except Exception as exc:
            print(f"  [warn] could not load {table_name}: {exc}")
            return []

    score_rows  = load("gold.sat_vacancy_score",   ("hub_vacancy_hk", "score", "reasoning",
                                                     "recommended_action", "scored_at"))
    detail_rows = load("gold.sat_vacancy_details",  ("hub_vacancy_hk", "url"))
    source_rows = load("gold.sat_vacancy_source",   ("hub_vacancy_hk", "salary_from",
                                                     "salary_to", "salary_currency"))
    hub_rows    = load("gold.hub_vacancy",          ("hub_vacancy_hk", "content_hash_bk"))
    link_rows   = load("gold.link_vacancy_company", ("hub_vacancy_hk", "hub_company_hk"))
    company_rows= load("gold.hub_company",          ("hub_company_hk", "company_name_bk"))
    hh_rows     = load("silver.hh_vacancies",       ("content_hash", "vacancy_name"))

    # Build lookup maps
    detail_map  = {r["hub_vacancy_hk"]: r for r in detail_rows}
    source_map  = {r["hub_vacancy_hk"]: r for r in source_rows}
    hub_map     = {r["hub_vacancy_hk"]: r["content_hash_bk"] for r in hub_rows}
    link_map    = {r["hub_vacancy_hk"]: r["hub_company_hk"]  for r in link_rows}
    company_map = {r["hub_company_hk"]: r["company_name_bk"] for r in company_rows}
    hh_name_map = {r["content_hash"]:   r["vacancy_name"]    for r in hh_rows}

    # Deduplicate scores — keep highest score per hk
    score_map: dict[str, dict] = {}
    for r in score_rows:
        hk = r["hub_vacancy_hk"]
        existing = score_map.get(hk)
        if existing is None or (r.get("score") or 0) > (existing.get("score") or 0):
            score_map[hk] = r

    results = []
    for hk, score_data in score_map.items():
        if score_data.get("recommended_action") != "priority_apply":
            continue

        scored_at = score_data.get("scored_at")
        if scored_at is None:
            continue
        # Normalize to naive UTC datetime for comparison
        if hasattr(scored_at, "tzinfo") and scored_at.tzinfo is not None:
            scored_at = scored_at.replace(tzinfo=None)
        if scored_at < cutoff:
            continue

        detail = detail_map.get(hk, {})
        source = source_map.get(hk, {})
        company_hk = link_map.get(hk)
        employer_name = company_map.get(company_hk) if company_hk else None
        content_hash = hub_map.get(hk)
        vacancy_name = hh_name_map.get(content_hash) if content_hash else None

        results.append({
            "hub_vacancy_hk":     hk,
            "score":              score_data.get("score"),
            "reasoning":          score_data.get("reasoning") or "",
            "recommended_action": score_data.get("recommended_action"),
            "url":                detail.get("url") or "",
            "employer_name":      employer_name or "Unknown Company",
            "vacancy_name":       vacancy_name or "Vacancy",
            "salary_from":        source.get("salary_from"),
            "salary_to":          source.get("salary_to"),
            "salary_currency":    source.get("salary_currency"),
        })

    return results


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


def _fmt_salary(salary_from, salary_to, currency) -> str:
    """Format salary range as a compact string.

    Args:
        salary_from: Lower bound (int or None).
        salary_to: Upper bound (int or None).
        currency: Currency code (e.g. 'RUB') or None.

    Returns:
        Formatted string, e.g. '100 000–150 000 RUB' or 'не указана'.
    """
    if salary_from is None and salary_to is None:
        return "не указана"
    parts = []
    if salary_from and salary_to:
        parts.append(f"{salary_from:,}–{salary_to:,}".replace(",", " "))
    elif salary_from:
        parts.append(f"от {salary_from:,}".replace(",", " "))
    elif salary_to:
        parts.append(f"до {salary_to:,}".replace(",", " "))
    if currency:
        parts.append(currency)
    return " ".join(parts)


def esc(text: str) -> str:
    """Escape special MarkdownV2 characters.

    Args:
        text: Plain text string.

    Returns:
        String with all Telegram MarkdownV2 special characters escaped.
    """
    for ch in r"\_*[]()~`>#+-=|{}.!":
        text = text.replace(ch, f"\\{ch}")
    return text


def build_vacancy_block(vacancy: dict) -> str:
    """Render a single vacancy as a MarkdownV2 block for use inside a digest.

    Args:
        vacancy: Dict with vacancy fields from load_new_priority_vacancies.

    Returns:
        Formatted block string (no leading/trailing newlines).
    """
    score = vacancy["score"]
    score_str = f"{score:.1f}" if score is not None else "N/A"
    employer = vacancy["employer_name"]
    role = vacancy["vacancy_name"]
    url = vacancy["url"]
    salary = _fmt_salary(vacancy["salary_from"], vacancy["salary_to"], vacancy["salary_currency"])
    reasoning = vacancy["reasoning"]

    lines = [
        f"*{esc(employer)} — {esc(role)}*",
        f"Score: *{esc(score_str)}/10* · {esc(salary)}",
        f"_{esc(reasoning[:200])}_",
    ]
    if url:
        lines.append(esc(url))
    return "\n".join(lines)


VACANCIES_PER_MESSAGE = 5


def build_digest(vacancies: list[dict]) -> list[str]:
    """Build MarkdownV2 digest messages, VACANCIES_PER_MESSAGE vacancies each.

    Args:
        vacancies: List of vacancy dicts sorted by score descending.

    Returns:
        List of MarkdownV2 message strings ready to send.
    """
    separator = "\n\n" + "\\-" * 16 + "\n\n"
    total = len(vacancies)
    messages = []

    for i in range(0, total, VACANCIES_PER_MESSAGE):
        chunk = vacancies[i:i + VACANCIES_PER_MESSAGE]
        part = i // VACANCIES_PER_MESSAGE + 1
        parts_total = (total + VACANCIES_PER_MESSAGE - 1) // VACANCIES_PER_MESSAGE
        header = f"🔥 *priority\\_apply* {esc(f'({part}/{parts_total})')} — {esc(str(total))} вакансий\n\n"
        body = separator.join(build_vacancy_block(v) for v in chunk)
        messages.append(header + body)

    return messages


def send_telegram(message: str, dry_run: bool = False) -> bool:
    """Send a message via Telegram Bot API.

    Args:
        message: MarkdownV2-formatted message text.
        dry_run: If True, print the message instead of sending.

    Returns:
        True on success, False on failure.
    """
    if dry_run:
        print(f"[dry-run] would send:\n{message}\n")
        return True

    if not TG_ALERT_BOT_TOKEN or not TG_ALERT_CHAT_ID:
        print("[error] TG_ALERT_BOT_TOKEN and TG_ALERT_CHAT_ID must be set.")
        return False

    url = f"{TG_API_BASE}/bot{TG_ALERT_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_ALERT_CHAT_ID,
        "text": message,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except requests.RequestException as exc:
        print(f"[error] Telegram send failed: {exc}")
        return False


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run notify: load new priority_apply vacancies → send Telegram alerts."""
    parser = argparse.ArgumentParser(
        description="Send Telegram alerts for new priority_apply vacancies"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print which vacancies would be notified without sending"
    )
    parser.add_argument(
        "--window-minutes", type=int, default=DEFAULT_WINDOW_MINUTES,
        help=f"Look-back window in minutes (default: {DEFAULT_WINDOW_MINUTES})"
    )
    args = parser.parse_args()

    catalog = get_catalog()
    vacancies = load_new_priority_vacancies(catalog, args.window_minutes)
    print(f"Found {len(vacancies)} new priority_apply vacancies in last {args.window_minutes}m")

    if not vacancies:
        print("Nothing to notify.")
        return

    notified_hks = load_notified_hks()

    # Filter out already-notified vacancies
    new_vacancies = [v for v in vacancies if v["hub_vacancy_hk"] not in notified_hks]
    skipped = len(vacancies) - len(new_vacancies)

    if not new_vacancies:
        print(f"Nothing new to notify (skipped {skipped} already-notified).")
        return

    # Sort by score descending so the best match is at the top
    new_vacancies.sort(key=lambda v: v["score"] or 0, reverse=True)

    messages = build_digest(new_vacancies)
    print(f"Sending {len(new_vacancies)} vacancies in {len(messages)} message(s)...")

    success = all(send_telegram(msg, dry_run=args.dry_run) for msg in messages)

    if success and not args.dry_run:
        new_hks = {v["hub_vacancy_hk"] for v in new_vacancies}
        save_notified_hks(notified_hks | new_hks)
        for v in new_vacancies:
            print(f"  {v['score']:.1f} {v['employer_name']} — {v['vacancy_name']}")

    action_word = "Would notify" if args.dry_run else "Notified"
    print(f"\n{action_word} {len(new_vacancies)} vacancies, skipped {skipped} already-notified.")


if __name__ == "__main__":
    main()
