"""Score vacancies using Claude Code CLI subprocess.

Reads unscored vacancies from gold.hub_vacancy (dbt-created Iceberg table),
joins to silver.vacancies for text, filters to those published within the last
30 days, submits chunks of 20 vacancies per Claude Code CLI call, and writes
results to gold.sat_vacancy_score via PyIceberg.

After the dbt migration, all tables are available directly as nessie.silver.*
and nessie.gold.* — no sqlmesh hash-suffix resolution needed.

Usage:
    python scripts/score_vacancies.py [--dry-run] [--rescore] [--purge-failed]

Flags:
    --dry-run:      Print vacancies that would be scored without calling Claude.
    --rescore:      Re-score all vacancies, ignoring existing sat_vacancy_score rows.
    --purge-failed: Remove failed (score=NULL) rows, then score them again.

Environment variables:
    NESSIE_URI          — Nessie REST endpoint (default: http://localhost:19120/iceberg/).
    MINIO_ROOT_USER     — MinIO access key.
    MINIO_ROOT_PASSWORD — MinIO secret key.
    S3_ENDPOINT         — MinIO S3 endpoint (default: http://localhost:9000).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNK_SIZE = 20
TEXT_TRUNCATE = 500
TTL_DAYS = 30
MODEL_VERSION = "claude-code-subprocess"

SCORING_PROMPT_TEMPLATE = """\
IMPORTANT: Respond with a raw JSON array only. No markdown. No prose. No code fences. \
Start your response with [ and end with ]. Nothing before or after.

Task: score job vacancies for this candidate:
- Role: Analytics Engineer / Data Engineer
- Location: Almaty, Kazakhstan (open to remote)
- Stack: dbt, ClickHouse, SQL, Python, PostgreSQL
- Target roles: Analytics Engineer, Data Engineer with dbt+ClickHouse, BI Engineer
- Experience: mid-level

Score 0-10 (0=irrelevant, 10=perfect). recommended_action: skip(0-3), review(4-6), apply(7-8), priority_apply(9-10).

Output format — JSON array, one object per vacancy:
[{{"hub_vacancy_hk":"<copy from input>","score":<0-10 float>,"reasoning":"<2 sentences>","recommended_action":"<skip|review|apply|priority_apply>"}}]

Input vacancies:
{vacancies_json}"""

# ---------------------------------------------------------------------------
# PyIceberg Arrow schema for sat_vacancy_score
# ---------------------------------------------------------------------------

SAT_SCORE_ARROW_SCHEMA = pa.schema([
    pa.field("hub_vacancy_hk",     pa.string(),        nullable=False),
    pa.field("load_dts",           pa.timestamp("us"), nullable=False),
    pa.field("rec_src",            pa.string(),        nullable=False),
    pa.field("hash_diff",          pa.string(),        nullable=False),
    pa.field("score",              pa.float64(),       nullable=True),
    pa.field("reasoning",          pa.string(),        nullable=True),
    pa.field("recommended_action", pa.string(),        nullable=True),
    pa.field("model_version",      pa.string(),        nullable=True),
    pa.field("scored_at",          pa.timestamp("us"), nullable=True),
])


def _now() -> datetime:
    """Return current UTC time without tzinfo (PyIceberg timestamp convention)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hash_diff(score: float | None, reasoning: str | None, action: str | None) -> str:
    """Compute hash_diff over scoring attributes for change detection.

    Args:
        score: Numeric score 0-10, or None if scoring failed.
        reasoning: Scoring rationale text, or None.
        action: Recommended action string, or None.

    Returns:
        MD5 hex string over concatenated scoring attributes.
    """
    raw = "||".join([
        str(score)  if score    is not None else "^^",
        reasoning   if reasoning is not None else "^^",
        action      if action   is not None else "^^",
        MODEL_VERSION,
    ])
    return hashlib.md5(raw.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_unscored_vacancies(catalog, rescore: bool) -> list[dict]:
    """Load recent vacancies not yet present in sat_vacancy_score.

    Applies two filters:
    1. published_at (or published_at_approx) within the last TTL_DAYS days.
       Vacancies without any publish date are included — better to over-score
       than miss a fresh vacancy with missing metadata.
    2. hub_vacancy_hk not already in sat_vacancy_score (skipped if --rescore).

    After the dbt migration, tables are accessed directly as gold.hub_vacancy
    and silver.vacancies (Nessie namespace, no SQLMesh hash-suffix workaround).

    Args:
        catalog: Connected PyIceberg catalog instance.
        rescore: If True, return all recent vacancies regardless of existing scores.

    Returns:
        List of dicts with keys: hub_vacancy_hk, text.
    """
    cutoff = _now() - timedelta(days=TTL_DAYS)

    # Load hub_vacancy for hub_vacancy_hk → content_hash_bk mapping
    hub_table = patch_table_io(catalog.load_table("gold.hub_vacancy"))
    hub_rows = hub_table.scan(
        selected_fields=("hub_vacancy_hk", "content_hash_bk")
    ).to_arrow().to_pylist()

    # Load silver.vacancies for text and publish timestamps
    vac_table = patch_table_io(catalog.load_table("silver.vacancies"))
    vac_rows = vac_table.scan(
        selected_fields=("content_hash", "text", "published_at", "published_at_approx")
    ).to_arrow().to_pylist()

    # Build lookup: content_hash → vacancy metadata
    vac_map = {row["content_hash"]: row for row in vac_rows}

    # Load already-scored hub keys (empty set if --rescore or table is empty)
    scored_keys: set[str] = set()
    if not rescore:
        try:
            score_table = patch_table_io(catalog.load_table("gold.sat_vacancy_score"))
            scored_rows = score_table.scan(
                selected_fields=("hub_vacancy_hk",)
            ).to_arrow().to_pylist()
            scored_keys = {row["hub_vacancy_hk"] for row in scored_rows}
        except Exception:
            # Table may be empty — treat as no existing scores
            pass

    result = []
    for hub_row in hub_rows:
        hk = hub_row["hub_vacancy_hk"]
        if hk in scored_keys:
            continue

        vac = vac_map.get(hub_row["content_hash_bk"])
        if not vac or not vac.get("text"):
            continue

        # TTL filter: keep if within 30 days, or if publish date is unknown
        pub = vac.get("published_at") or vac.get("published_at_approx")
        if pub is not None:
            # PyIceberg returns timestamps as datetime objects
            pub_dt = pub if isinstance(pub, datetime) else datetime.fromisoformat(str(pub))
            pub_dt = pub_dt.replace(tzinfo=None)  # strip tz for comparison
            if pub_dt < cutoff:
                continue

        result.append({"hub_vacancy_hk": hk, "text": vac["text"]})

    return result


# ---------------------------------------------------------------------------
# Scoring via Claude Code CLI
# ---------------------------------------------------------------------------

def score_chunk(vacancies: list[dict]) -> list[dict]:
    """Score a chunk of vacancies using Claude Code CLI.

    Calls `claude -p <prompt> --output-format json` as a subprocess.
    The prompt asks Claude to return a JSON array with one object per vacancy.

    Args:
        vacancies: List of dicts with keys hub_vacancy_hk and text.

    Returns:
        List of scored dicts with keys hub_vacancy_hk, score, reasoning,
        recommended_action. Returns partial results if Claude returns fewer
        items than expected.
    """
    payload = [
        {"hub_vacancy_hk": v["hub_vacancy_hk"], "text": v["text"][:TEXT_TRUNCATE]}
        for v in vacancies
    ]
    prompt = SCORING_PROMPT_TEMPLATE.format(
        vacancies_json=json.dumps(payload, ensure_ascii=False, indent=2)
    )

    result = subprocess.run(
        ["claude", "-p", prompt, "--output-format", "json"],
        capture_output=True,
        text=True,
        timeout=120,
    )

    if result.returncode != 0:
        print(f"  [warn] claude exited {result.returncode}: {result.stderr[:200]}")
        return _fallback_results(vacancies, f"claude exit {result.returncode}")

    # claude --output-format json wraps the response in {"type": "result", "result": "..."}
    try:
        outer = json.loads(result.stdout)
        raw_text = outer.get("result", result.stdout)
    except json.JSONDecodeError:
        raw_text = result.stdout

    # Extract JSON array from the response text (strip any surrounding prose)
    try:
        start = raw_text.index("[")
        end = raw_text.rindex("]") + 1
        scored = json.loads(raw_text[start:end])
    except (ValueError, json.JSONDecodeError) as exc:
        print(f"  [warn] failed to parse response: {exc}")
        return _fallback_results(vacancies, f"parse error: {exc}")

    return scored


def _fallback_results(vacancies: list[dict], reason: str) -> list[dict]:
    """Return null-score records for vacancies that could not be scored.

    Args:
        vacancies: Original vacancy dicts for this chunk.
        reason: Error description stored in reasoning field.

    Returns:
        List of score dicts with None score and error message in reasoning.
    """
    return [
        {
            "hub_vacancy_hk":     v["hub_vacancy_hk"],
            "score":              None,
            "reasoning":          f"scoring failed: {reason}",
            "recommended_action": None,
        }
        for v in vacancies
    ]


# ---------------------------------------------------------------------------
# Maintenance helpers
# ---------------------------------------------------------------------------

def purge_failed_scores(catalog) -> int:
    """Remove rows with score=NULL from sat_vacancy_score by overwriting the table.

    PyIceberg does not support DELETE — we read all rows, filter out failures,
    and overwrite the table with only successfully scored rows.

    Args:
        catalog: Connected PyIceberg catalog instance.

    Returns:
        Number of rows removed.
    """
    table = patch_table_io(catalog.load_table("gold.sat_vacancy_score"))
    arrow = table.scan().to_arrow()
    all_rows = arrow.to_pylist()

    ok_rows = [r for r in all_rows if r["score"] is not None]
    removed = len(all_rows) - len(ok_rows)

    if removed == 0:
        print("No failed rows to purge.")
        return 0

    # Overwrite table with only valid rows
    ok_arrow = pa.table(
        {col: pa.array([r[col] for r in ok_rows], type=SAT_SCORE_ARROW_SCHEMA.field(col).type)
         for col in SAT_SCORE_ARROW_SCHEMA.names},
        schema=SAT_SCORE_ARROW_SCHEMA,
    )
    table.overwrite(ok_arrow)
    print(f"  purged {removed} failed rows, {len(ok_rows)} valid rows remain")
    return removed


# ---------------------------------------------------------------------------
# Write results
# ---------------------------------------------------------------------------

def write_scores(catalog, scored_rows: list[dict]) -> None:
    """Append score results to gold.sat_vacancy_score.

    Args:
        catalog: Connected PyIceberg catalog instance.
        scored_rows: List of scored dicts from score_chunk calls.
    """
    now = _now()
    rows = []
    for item in scored_rows:
        score = item.get("score")
        reasoning = item.get("reasoning")
        action = item.get("recommended_action")
        rows.append({
            "hub_vacancy_hk":     item["hub_vacancy_hk"],
            "load_dts":           now,
            "rec_src":            "claude_code_subprocess",
            "hash_diff":          _hash_diff(score, reasoning, action),
            "score":              float(score) if score is not None else None,
            "reasoning":          reasoning,
            "recommended_action": action,
            "model_version":      MODEL_VERSION,
            "scored_at":          now,
        })

    if not rows:
        print("No rows to write.")
        return

    arrow_table = pa.table(
        {col: pa.array([r[col] for r in rows], type=SAT_SCORE_ARROW_SCHEMA.field(col).type)
         for col in SAT_SCORE_ARROW_SCHEMA.names},
        schema=SAT_SCORE_ARROW_SCHEMA,
    )

    table = patch_table_io(catalog.load_table("gold.sat_vacancy_score"))
    table.append(arrow_table)
    print(f"  wrote {len(rows)} score rows to gold.sat_vacancy_score")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run scoring pipeline: load unscored vacancies → chunk → score → write."""
    parser = argparse.ArgumentParser(
        description="Score vacancies via Claude Code CLI subprocess"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print vacancies that would be scored without calling Claude"
    )
    parser.add_argument(
        "--rescore", action="store_true",
        help="Re-score all recent vacancies, ignoring existing scores"
    )
    parser.add_argument(
        "--purge-failed", action="store_true",
        help="Remove failed (score=NULL) rows from sat_vacancy_score, then score them"
    )
    args = parser.parse_args()

    catalog = get_catalog()

    if args.purge_failed:
        print("Purging failed score rows...")
        purge_failed_scores(catalog)

    vacancies = load_unscored_vacancies(catalog, rescore=args.rescore)
    print(f"Found {len(vacancies)} vacancies to score (published within {TTL_DAYS} days)")

    if not vacancies:
        print("Nothing to score.")
        return

    if args.dry_run:
        print(f"\nFirst {min(5, len(vacancies))} vacancies:")
        for v in vacancies[:5]:
            print(f"  {v['hub_vacancy_hk']}: {v['text'][:80]}...")
        return

    # Process in chunks of CHUNK_SIZE
    all_scored: list[dict] = []
    chunks = [vacancies[i:i + CHUNK_SIZE] for i in range(0, len(vacancies), CHUNK_SIZE)]
    for idx, chunk in enumerate(chunks, 1):
        print(f"Scoring chunk {idx}/{len(chunks)} ({len(chunk)} vacancies)...")
        scored = score_chunk(chunk)
        all_scored.extend(scored)

    write_scores(catalog, all_scored)
    print(f"\nDone. Scored {len(all_scored)} vacancies.")


if __name__ == "__main__":
    main()
