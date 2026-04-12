"""LinkedIn extractor — Bronze layer.

Two-stage pipeline: fetch → parse.

  fetch  — navigates LinkedIn search pages, saves fully-rendered MHTML
           snapshots to the MinIO landing zone. No parsing, no Iceberg writes.
  parse  — reads saved MHTML files from MinIO, extracts post cards, writes
           RawVacancy records to bronze.linkedin_posts.
  scrape — convenience command that runs fetch then parse (full cycle).

Landing zone path: s3://warehouse/landing/linkedin/YYYY-MM-DD/HHMMSS_{slug}.mhtml

Authentication: uses a cookies file (linkedin-cookies.json) exported from
a logged-in browser session via Cookie-Editor extension.

Incrementality: parse deduplicates by post_id against existing
bronze.linkedin_posts records. On re-run, already-seen posts are skipped.

Pre-filter: uses shared keyword filter from common.py to flag relevant posts.

Note: LinkedIn relative timestamps ("2 ч.", "1 дн.") are not available in
static MHTML — published_at_raw will be empty for MHTML-parsed posts.

Usage:
    python extractors/linkedin.py fetch              # all active LinkedIn queries
    python extractors/linkedin.py fetch "dbt hiring" # specific query
    python extractors/linkedin.py parse              # parse today's landing files
    python extractors/linkedin.py parse 2025-04-12   # parse specific date
    python extractors/linkedin.py scrape             # fetch + parse (full cycle)
"""

from __future__ import annotations

import email as email_lib
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote, quote_plus, unquote

import boto3
import pyarrow as pa

sys.path.insert(0, str(Path(__file__).parent.parent))
from extractors.catalog import get_catalog, patch_table_io
from extractors.common import RawVacancy, passes_prefilter

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
except ImportError:
    print("Установи зависимости: pip3 install selenium")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_GRINDHOUSE_ROOT = Path(__file__).parent.parent

COOKIES_FILE = Path(
    os.environ.get("LINKEDIN_COOKIES_FILE", str(_GRINDHOUSE_ROOT / "linkedin-cookies.json"))
)
SESSION_DIR = Path(
    os.environ.get("LINKEDIN_SESSION_DIR", str(_GRINDHOUSE_ROOT / "linkedin-session"))
)
CHROME_BIN = os.environ.get(
    "LINKEDIN_CHROME_BIN", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
)
QUERY_LIMIT = int(os.environ.get("LINKEDIN_QUERY_LIMIT", "20"))

MINIO_ENDPOINT   = os.environ.get("MINIO_ENDPOINT",    "http://localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY",  "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY",  "minioadmin")
MINIO_BUCKET     = os.environ.get("MINIO_BUCKET",      "warehouse")

LANDING_PREFIX = "landing/linkedin"

# ---------------------------------------------------------------------------
# Iceberg helpers
# ---------------------------------------------------------------------------


def load_active_linkedin_queries(catalog) -> list[dict]:
    """Load active LinkedIn query configs from meta.sources.

    Each returned dict has at minimum a ``query`` key plus optional scroll
    tuning parameters. Defaults are applied for missing keys:
      - scroll_steps: 6  (scroll increments per round)
      - max_rounds:   10 (maximum scroll rounds before stopping)
      - max_age_days: 365 (skip posts older than this many days)

    Args:
        catalog: Connected PyIceberg catalog.

    Returns:
        List of query config dicts.
    """
    table = patch_table_io(catalog.load_table("meta.sources"))
    from pyiceberg.expressions import And, EqualTo

    scan = table.scan(
        row_filter=And(EqualTo("source_type", "linkedin"), EqualTo("active", True)),
        selected_fields=("config",),
    )
    queries = []
    for config_str in scan.to_arrow().column("config").to_pylist():
        cfg = json.loads(config_str)
        cfg.setdefault("scroll_steps", 6)
        cfg.setdefault("max_rounds", 10)
        cfg.setdefault("max_age_days", 365)
        queries.append(cfg)
    return queries


def get_seen_post_ids(catalog) -> set[str]:
    """Return set of post_ids already stored in bronze.linkedin_posts.

    Used for deduplication — already-seen posts are skipped on re-run.

    Args:
        catalog: Connected PyIceberg catalog.

    Returns:
        Set of post_id strings.
    """
    table = patch_table_io(catalog.load_table("bronze.linkedin_posts"))
    scan = table.scan(selected_fields=("post_id",))
    arrow = scan.to_arrow()
    if arrow.num_rows == 0:
        return set()
    return set(arrow.column("post_id").to_pylist())


def write_posts(catalog, posts: list[RawVacancy]) -> None:
    """Append a batch of LinkedIn posts to bronze.linkedin_posts.

    Args:
        catalog: Connected PyIceberg catalog.
        posts: List of RawVacancy instances from the LinkedIn extractor.
    """
    if not posts:
        return
    table = patch_table_io(catalog.load_table("bronze.linkedin_posts"))
    arrow_schema = pa.schema([
        pa.field("post_id",          pa.string(),         nullable=False),
        pa.field("post_url",         pa.string(),         nullable=False),
        pa.field("author",           pa.string(),         nullable=False),
        pa.field("author_url",       pa.string(),         nullable=False),
        pa.field("company",          pa.string(),         nullable=False),
        pa.field("company_url",      pa.string(),         nullable=False),
        pa.field("text",             pa.string(),         nullable=False),
        pa.field("published_at_raw", pa.string(),         nullable=False),
        pa.field("query",            pa.string(),         nullable=False),
        pa.field("extracted_at",     pa.timestamp("us"),  nullable=False),
        pa.field("passed_prefilter", pa.bool_(),          nullable=False),
    ])
    arrow_table = pa.table(
        {
            "post_id":          pa.array([p.source_id                  for p in posts], type=pa.string()),
            "post_url":         pa.array([p.url                        for p in posts], type=pa.string()),
            "author":           pa.array([p.extra["author"]            for p in posts], type=pa.string()),
            "author_url":       pa.array([p.extra["author_url"]        for p in posts], type=pa.string()),
            "company":          pa.array([p.extra["company"]           for p in posts], type=pa.string()),
            "company_url":      pa.array([p.extra["company_url"]       for p in posts], type=pa.string()),
            "text":             pa.array([p.text                       for p in posts], type=pa.string()),
            "published_at_raw": pa.array([p.extra["published_at_raw"]  for p in posts], type=pa.string()),
            "query":            pa.array([p.extra["query"]             for p in posts], type=pa.string()),
            "extracted_at":     pa.array([p.extracted_at               for p in posts], type=pa.timestamp("us")),
            "passed_prefilter": pa.array([p.passed_prefilter           for p in posts], type=pa.bool_()),
        },
        schema=arrow_schema,
    )
    table.append(arrow_table)


# ---------------------------------------------------------------------------
# MinIO helpers
# ---------------------------------------------------------------------------


def _get_minio_client():
    """Create a boto3 S3 client pointed at the local MinIO instance.

    Returns:
        boto3 S3 client configured from environment variables.
    """
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def _save_mhtml_to_minio(mhtml_bytes: bytes, query: str, ts: datetime) -> str:
    """Upload an MHTML snapshot to the MinIO landing zone.

    Path pattern: landing/linkedin/YYYY-MM-DD/HHMMSS_{slug}.mhtml

    Args:
        mhtml_bytes: Raw MHTML content from CDP captureSnapshot.
        query: LinkedIn search query string (used in filename slug).
        ts: Timestamp of the snapshot (UTC).

    Returns:
        Full S3 URI of the uploaded object.
    """
    slug = re.sub(r"[^\w]", "_", query, flags=re.UNICODE)[:40].strip("_")
    date_str = ts.strftime("%Y-%m-%d")
    time_str = ts.strftime("%H%M%S")
    key = f"{LANDING_PREFIX}/{date_str}/{time_str}_{slug}.mhtml"
    client = _get_minio_client()
    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=mhtml_bytes,
        ContentType="message/rfc822",
        Metadata={"query": quote(query)},
    )
    return f"s3://{MINIO_BUCKET}/{key}"


def _list_mhtml_from_minio(date_str: str) -> list[dict]:
    """List MHTML landing files for a given date.

    Args:
        date_str: Date in YYYY-MM-DD format.

    Returns:
        List of dicts with keys: key (S3 key), query (from object metadata).
    """
    prefix = f"{LANDING_PREFIX}/{date_str}/"
    client = _get_minio_client()
    response = client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    results = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        try:
            meta = client.head_object(Bucket=MINIO_BUCKET, Key=key)
            query = unquote(meta.get("Metadata", {}).get("query", ""))
        except Exception:
            query = ""
        results.append({"key": key, "query": query})
    return results


def _read_mhtml_from_minio(key: str) -> bytes:
    """Download an MHTML file from MinIO.

    Args:
        key: S3 object key.

    Returns:
        Raw MHTML bytes.
    """
    client = _get_minio_client()
    response = client.get_object(Bucket=MINIO_BUCKET, Key=key)
    return response["Body"].read()


# ---------------------------------------------------------------------------
# Selenium helpers
# ---------------------------------------------------------------------------


def _human_sleep(lo: float, hi: float) -> None:
    """Sleep for a random duration with Gaussian distribution.

    Args:
        lo: Minimum sleep seconds.
        hi: Maximum sleep seconds.
    """
    mid = (lo + hi) / 2
    std = (hi - lo) / 4
    t = max(lo, min(hi, random.gauss(mid, std)))
    time.sleep(t)


def _human_scroll(driver, steps: int = 4) -> None:
    """Scroll the page in a human-like pattern with random pauses.

    Args:
        driver: Selenium WebDriver instance.
        steps: Number of scroll steps.
    """
    scroll_js = """
        const el = document.querySelector('main') || document.scrollingElement || document.documentElement;
        el.scrollBy(0, arguments[0]);
    """
    for _ in range(steps):
        delta = random.randint(250, 450)
        driver.execute_script(scroll_js, delta)
        _human_sleep(0.5, 1.5)
        if random.random() < 0.15:
            driver.execute_script(scroll_js, -random.randint(60, 150))
            _human_sleep(0.3, 0.8)


def make_driver() -> webdriver.Chrome:
    """Create a Selenium Chrome WebDriver with anti-detection settings.

    Uses a persistent profile directory so cookies and session survive
    across runs. Chrome binary and session dir are configured via env vars.

    Returns:
        Configured Chrome WebDriver instance.
    """
    SESSION_DIR.mkdir(parents=True, exist_ok=True)
    opts = Options()
    opts.binary_location = CHROME_BIN
    opts.add_argument(f"--user-data-dir={SESSION_DIR}")
    opts.add_argument("--start-maximized")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)

    # chromedriver path — selenium manager auto-downloads if not set
    chromedriver = Path.home() / ".cache/selenium/chromedriver/mac-arm64/146.0.7680.165/chromedriver"
    service = Service(executable_path=str(chromedriver)) if chromedriver.exists() else Service()

    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    })
    return driver


def inject_cookies(driver) -> None:
    """Load LinkedIn session cookies and verify the session is active.

    Reads cookies from COOKIES_FILE, injects them into the browser, then
    navigates to the feed to confirm the session is valid.

    Args:
        driver: Selenium WebDriver instance.

    Raises:
        SystemExit: If cookies file is missing or session is invalid.
    """
    if not COOKIES_FILE.exists():
        print(f"[!] Cookies file not found: {COOKIES_FILE}")
        print("    Export cookies from linkedin.com via Cookie-Editor extension.")
        sys.exit(1)

    cookies = json.loads(COOKIES_FILE.read_text())

    driver.get("https://www.linkedin.com")
    _human_sleep(2, 3)

    for c in cookies:
        cookie = {
            "name":   c["name"],
            "value":  c["value"],
            "domain": c["domain"],
            "path":   c.get("path", "/"),
            "secure": c.get("secure", True),
        }
        if c.get("expirationDate"):
            cookie["expiry"] = int(c["expirationDate"])
        try:
            driver.add_cookie(cookie)
        except Exception:
            pass  # some cookies may not apply — not critical

    driver.get("https://www.linkedin.com/feed/")
    _human_sleep(3, 5)

    if "feed" in driver.current_url or "mynetwork" in driver.current_url:
        print("  ✓ LinkedIn: session active")
    else:
        print(f"  [!] Login failed, current URL: {driver.current_url}")
        print("  Refresh cookies: export from linkedin.com via Cookie-Editor → replace linkedin-cookies.json")
        driver.quit()
        sys.exit(1)


# ---------------------------------------------------------------------------
# Age parsing helpers
# ---------------------------------------------------------------------------

# Maps Russian relative-time tokens found in LinkedIn post text to timedeltas.
# LinkedIn only exposes relative timestamps ("2 нед.", "4 мес.", "1 год") in
# the rendered DOM — no ISO dates are available without additional requests.
_AGE_PATTERNS: list[tuple[str, object]] = [
    (r"(\d+)\s*мин",   lambda n: timedelta(minutes=n)),
    (r"(\d+)\s*ч\.",   lambda n: timedelta(hours=n)),
    (r"вчера",         lambda _: timedelta(days=1)),
    (r"(\d+)\s*нед\.", lambda n: timedelta(weeks=n)),
    (r"(\d+)\s*мес\.", lambda n: timedelta(days=n * 30)),
    (r"(\d+)\s*лет",   lambda n: timedelta(days=n * 365)),
    (r"(\d+)\s*год",   lambda n: timedelta(days=n * 365)),
]


def _parse_relative_age(text: str) -> timedelta | None:
    """Parse a LinkedIn relative timestamp from post card plain text.

    LinkedIn renders post age as a Russian relative time string embedded in
    the card text after the author name and title bullet, e.g.:
      "Кирилл Дикалин • Lead Data Engineer 2 нед. • Отслеживать"

    Args:
        text: Plain text of a post card (HTML tags already stripped).

    Returns:
        Approximate age as timedelta, or None if no pattern matched.
    """
    # Focus search on the first 200 chars — timestamp appears near the top
    snippet = text[:200]
    for pattern, to_delta in _AGE_PATTERNS:
        m = re.search(pattern, snippet)
        if m:
            n = int(m.group(1)) if m.lastindex else 0
            return to_delta(n)
    return None


# ---------------------------------------------------------------------------
# MHTML capture (fetch stage)
# ---------------------------------------------------------------------------


def _capture_mhtml(driver) -> bytes:
    """Capture a full-page MHTML snapshot via Chrome DevTools Protocol.

    Uses Page.captureSnapshot which serializes the fully-rendered DOM
    including all JavaScript-rendered content. Unlike page_source or
    outerHTML, this captures computed text visible in the browser.

    On large pages Chrome occasionally fails to generate MHTML (typically
    after many scroll rounds when the DOM is very large). In that case a
    short pause and one retry are attempted before raising.

    Args:
        driver: Selenium WebDriver instance with a loaded page.

    Returns:
        MHTML content as UTF-8 encoded bytes.

    Raises:
        Exception: If both the initial attempt and the retry fail.
    """
    try:
        result = driver.execute_cdp_cmd("Page.captureSnapshot", {"format": "mhtml"})
        return result["data"].encode("utf-8")
    except Exception as exc:
        if "Failed to generate MHTML" not in str(exc):
            raise
        # DOM may be too large — wait briefly and retry once
        time.sleep(3)
        result = driver.execute_cdp_cmd("Page.captureSnapshot", {"format": "mhtml"})
        return result["data"].encode("utf-8")


def fetch_query(
    driver,
    query_cfg: dict,
    seen_ids: set[str],
    ts: datetime,
) -> str | None:
    """Navigate to a LinkedIn search page, scroll iteratively, save MHTML.

    Each scroll round takes a new MHTML snapshot and checks early-stop
    conditions before scrolling further:

      1. No new cards loaded vs previous round — reached end of feed.
      2. A card older than max_age_days found — feed is sorted by date,
         remaining posts will only be older.
      3. All visible cards already in Bronze (seen_ids) — nothing new to store.

    The final snapshot (last round that added cards) is saved to MinIO.

    Args:
        driver: Selenium WebDriver instance (logged in).
        query_cfg: Query config dict with keys: query, scroll_steps,
            max_rounds, max_age_days.
        seen_ids: Set of post_ids already in bronze.linkedin_posts.
        ts: Timestamp used for the landing zone filename.

    Returns:
        S3 URI of the saved MHTML file, or None if skipped (all seen).
    """
    query        = query_cfg["query"]
    scroll_steps = query_cfg["scroll_steps"]
    max_rounds   = query_cfg["max_rounds"]
    max_age_days = query_cfg["max_age_days"]

    url = (
        "https://www.linkedin.com/search/results/content/"
        f"?keywords={quote_plus(query)}&sortBy=date_posted"
    )
    print(f"  → {url}")
    driver.get(url)
    _human_sleep(5, 9)

    all_cards: dict[str, dict] = {}  # post_id → card dict, accumulates across rounds
    last_mhtml: bytes | None = None
    stop_reason = ""

    for round_n in range(max_rounds):
        if round_n > 0:
            try:
                _human_scroll(driver, steps=scroll_steps)
            except Exception:
                print("  [!] Scroll failed — LinkedIn may have blocked the session.")
                break
            _human_sleep(2, 4)

        try:
            mhtml_bytes = _capture_mhtml(driver)
        except Exception as exc:
            print(f"  [!] captureSnapshot failed at round {round_n}: {exc}")
            stop_reason = "snapshot error"
            break

        cards = _parse_mhtml(mhtml_bytes)

        # Track which post_ids are new in this specific round (not seen before)
        ids_before = set(all_cards.keys())
        for card in cards:
            all_cards[card["post_id"]] = card
        ids_after = set(all_cards.keys())
        new_in_round = ids_after - ids_before

        new_count = len(new_in_round)
        print(f"  round {round_n}: {len(all_cards)} cards (+{new_count} new)")

        last_mhtml = mhtml_bytes

        # Stop: feed exhausted — DOM loaded no new cards after scroll
        if new_count == 0 and round_n > 0:
            stop_reason = "feed exhausted"
            break

        # Stop: hit the boundary of a previous run — a card from this round
        # already exists in Bronze, meaning we've scrolled back to already-seen posts.
        overlap = new_in_round & seen_ids
        if overlap:
            stop_reason = f"reached {len(overlap)} previously seen post(s)"
            break

        # Stop: posts older than TTL appeared in this round
        new_cards = [all_cards[pid] for pid in new_in_round]
        too_old = [
            c for c in new_cards
            if (age := _parse_relative_age(c["text"])) and age.days > max_age_days
        ]
        if too_old:
            stop_reason = f"post older than {max_age_days} days found"
            break

    else:
        stop_reason = f"reached max_rounds={max_rounds}"

    print(f"  stopped: {stop_reason}")

    if not all_cards:
        print("  no cards found, skipping save")
        return None

    # Nothing new vs Bronze — no point saving MHTML
    new_ids = {pid for pid in all_cards if pid not in seen_ids}
    if not new_ids:
        print("  0 new post_ids — skipping MHTML save")
        return None

    s3_uri = _save_mhtml_to_minio(last_mhtml, query, ts)
    size_kb = len(last_mhtml) // 1024
    print(f"  ✓ Saved {size_kb} KB → {s3_uri}")
    return s3_uri


# ---------------------------------------------------------------------------
# MHTML parsing (parse stage)
# ---------------------------------------------------------------------------


def _extract_cards(html_str: str) -> list[dict]:
    """Extract post card data from a LinkedIn search results HTML string.

    Finds all role="listitem" elements and extracts post URL, author,
    company, and text from each card's HTML window.

    Args:
        html_str: Full HTML string from a LinkedIn search results page.

    Returns:
        List of dicts with keys: post_id, post_url, author, author_url,
        company, company_url, text.
    """
    cards = []
    seen_ids: set[str] = set()

    for m in re.finditer(r'<[^>]+role="listitem"[^>]*>', html_str):
        start = m.start()
        # Use a generous window — cards can be 4-8 KB of HTML
        card_html = html_str[start:start + 10_000]

        # Extract post feed URL
        url_m = re.search(
            r'href="(https://www\.linkedin\.com/feed/update/[^"?]+)', card_html
        )
        post_url = url_m.group(1) if url_m else ""

        # Fallback ID from componentkey attribute
        ck_m = re.search(r'componentkey="([^"]+)"', card_html)
        ck = ck_m.group(1) if ck_m else ""

        post_id = post_url or (f"ck:{ck}" if ck else "")
        if not post_id or post_id in seen_ids:
            continue
        seen_ids.add(post_id)

        # Author profile URL — extract from href, name from stripped card text later
        author_m = re.search(
            r'href="(https://www\.linkedin\.com/in/[^"?/]+(?:/[^"?]*)?)"',
            card_html,
        )
        author_url = author_m.group(1).split("?")[0].rstrip("/") if author_m else ""
        # Author name: LinkedIn renders "Публикация в ленте {Name} \u2022" in plain text.
        # Use full card_html so no tags are truncated mid-attribute.
        author = ""
        card_text_plain = re.sub(r"<[^>]+>", " ", card_html)
        card_text_plain = re.sub(r"\s+", " ", card_text_plain).strip()
        feed_m = re.search(r"Публикация в ленте\s+(.{2,60}?)\s+\u2022", card_text_plain)
        if feed_m:
            author = feed_m.group(1).strip()

        # Company link
        company_m = re.search(
            r'href="(https://www\.linkedin\.com/company/[^"?]+)[^"]*"[^>]*>\s*([^<]{2,80}?)\s*<',
            card_html,
        )
        company_url = company_m.group(1) if company_m else ""
        company = company_m.group(2).strip() if company_m else ""

        # Strip all HTML tags to get plain text
        text = re.sub(r"<[^>]+>", " ", card_html)
        text = re.sub(r"\s+", " ", text).strip()

        cards.append({
            "post_id":     post_id,
            "post_url":    post_url or post_id,
            "author":      author,
            "author_url":  author_url,
            "company":     company,
            "company_url": company_url,
            "text":        text,
        })

    return cards


def _parse_mhtml(mhtml_bytes: bytes) -> list[dict]:
    """Parse an MHTML snapshot and extract LinkedIn post cards.

    Decodes the MHTML multipart structure, finds the primary HTML part,
    and delegates card extraction to _extract_cards.

    Args:
        mhtml_bytes: Raw MHTML content from MinIO or captureSnapshot.

    Returns:
        List of post card dicts (see _extract_cards).
    """
    msg = email_lib.message_from_bytes(mhtml_bytes)
    html_part = None
    for part in msg.walk():
        if part.get_content_type() == "text/html":
            payload = part.get_payload(decode=True)
            if payload:
                html_part = payload.decode("utf-8", errors="replace")
                break
    if not html_part:
        return []
    return _extract_cards(html_part)


# ---------------------------------------------------------------------------
# Stage commands
# ---------------------------------------------------------------------------


def cmd_fetch(queries: list[dict]) -> list[str]:
    """Fetch MHTML snapshots for all queries and save to MinIO landing zone.

    Loads seen post_ids from Bronze once upfront so each fetch_query call
    can apply early-stop logic (skip queries where all posts already seen).

    Args:
        queries: List of query config dicts (query, scroll_steps, max_rounds,
            max_age_days). Use load_active_linkedin_queries() to build this.

    Returns:
        List of S3 URIs for saved MHTML files (skipped queries excluded).
    """
    ts = datetime.now(timezone.utc).replace(tzinfo=None)
    saved: list[str] = []

    catalog = get_catalog()
    seen_ids = get_seen_post_ids(catalog)
    print(f"Already in Bronze: {len(seen_ids)} posts\n")

    driver = make_driver()
    try:
        inject_cookies(driver)
        for i, query_cfg in enumerate(queries):
            query = query_cfg["query"]
            print(f"\n[{i + 1}/{len(queries)}] Fetching: «{query}»")
            try:
                uri = fetch_query(driver, query_cfg, seen_ids, ts)
                if uri:
                    saved.append(uri)
            except Exception as exc:
                print(f"  [!] Failed: {exc}")
            if i < len(queries) - 1:
                wait = random.uniform(8, 15)
                print(f"  ⏸ Pause {wait:.0f}s...")
                time.sleep(wait)
    finally:
        driver.quit()

    print(f"\nFetch done. {len(saved)}/{len(queries)} snapshots saved.")
    return saved


def cmd_parse(date_str: str | None = None) -> int:
    """Parse MHTML landing files for a date and write posts to Bronze.

    Args:
        date_str: Date in YYYY-MM-DD format. Defaults to today (UTC).

    Returns:
        Total number of new posts written to bronze.linkedin_posts.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    print(f"Parsing landing zone for date: {date_str}")
    files = _list_mhtml_from_minio(date_str)
    if not files:
        print(f"  No MHTML files found under {LANDING_PREFIX}/{date_str}/")
        return 0

    print(f"  Found {len(files)} file(s)")
    catalog = get_catalog()
    seen_ids = get_seen_post_ids(catalog)
    print(f"  Already in Bronze: {len(seen_ids)} posts\n")

    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
    total_new = 0

    for f in files:
        key = f["key"]
        query = f["query"]
        print(f"  Parsing: {key}")
        try:
            mhtml_bytes = _read_mhtml_from_minio(key)
            cards = _parse_mhtml(mhtml_bytes)
        except Exception as exc:
            print(f"    [!] Failed to parse: {exc}")
            continue

        batch: list[RawVacancy] = []
        for card in cards:
            post_id = card["post_id"]
            if post_id in seen_ids:
                continue
            seen_ids.add(post_id)
            text = card["text"]
            batch.append(RawVacancy(
                source="linkedin",
                source_id=post_id,
                url=card["post_url"],
                text=text,
                published_at=None,
                extracted_at=extracted_at,
                passed_prefilter=passes_prefilter(text),
                extra={
                    "author":           card["author"],
                    "author_url":       card["author_url"],
                    "company":          card["company"],
                    "company_url":      card["company_url"],
                    "published_at_raw": "",  # not available in static MHTML
                    "query":            query,
                },
            ))

        write_posts(catalog, batch)
        print(f"    ✓ Written: {len(batch)} new posts (cards found: {len(cards)})")
        total_new += len(batch)

    print(f"\nParse done. Total new posts written: {total_new}")
    return total_new


def cmd_scrape(queries: list[dict]) -> None:
    """Run fetch then parse — full extraction cycle.

    Args:
        queries: List of query config dicts (see load_active_linkedin_queries).
    """
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Scraping {len(queries)} LinkedIn query/queries...\n")
    cmd_fetch(queries)
    print()
    cmd_parse(date_str)


def _queries_from_args(args: list[str]) -> list[dict]:
    """Build query config dicts from CLI positional arguments.

    When the user passes query strings on the command line, wrap each in a
    minimal config dict using default scroll parameters.

    Args:
        args: Raw query strings from sys.argv.

    Returns:
        List of query config dicts with default scroll parameters.
    """
    return [
        {"query": q, "scroll_steps": 6, "max_rounds": 10, "max_age_days": 365}
        for q in args
    ]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for the LinkedIn extractor."""
    args = sys.argv[1:]

    hour = datetime.now().hour
    if hour < 8 or hour >= 23:
        print(f"[!] Current time {hour}:xx — outside working hours (08:00–23:00).")
        sys.exit(0)

    if not args:
        print(__doc__)
        sys.exit(1)

    cmd = args[0]

    if cmd == "fetch":
        if len(args) > 1:
            queries = _queries_from_args(args[1:])
        else:
            catalog = get_catalog()
            queries = load_active_linkedin_queries(catalog)
            if not queries:
                print("No active LinkedIn queries in meta.sources. Run `make init` first.")
                sys.exit(1)
        cmd_fetch(queries)

    elif cmd == "parse":
        date_str = args[1] if len(args) > 1 else None
        cmd_parse(date_str)

    elif cmd == "scrape":
        if len(args) > 1:
            queries = _queries_from_args(args[1:])
        else:
            catalog = get_catalog()
            queries = load_active_linkedin_queries(catalog)
            if not queries:
                print("No active LinkedIn queries in meta.sources. Run `make init` first.")
                sys.exit(1)
        cmd_scrape(queries)

    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
