"""LinkedIn extractor — Bronze layer.

Scrapes LinkedIn search results for hiring posts and writes raw records
to the bronze.linkedin_posts Iceberg table.

Authentication: uses a cookies file (linkedin-cookies.json) exported from
a logged-in browser session via Cookie-Editor extension. No API key required.

Incrementality: deduplicates by post_id against existing bronze.linkedin_posts
records. On re-run, already-seen posts are skipped.

Pre-filter: reuses the same keyword filter as the Telegram extractor to flag
posts relevant for AI scoring downstream.

Note: LinkedIn returns relative timestamps only ("2 ч.", "1 дн.") — exact
publish time is not available without additional requests, so published_at_raw
stores the raw string as-is.

Usage:
    python extractors/linkedin.py scrape              # all active LinkedIn queries
    python extractors/linkedin.py scrape "dbt hiring" # specific query
"""

from __future__ import annotations

import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

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

_GRIDHOUSE_ROOT = Path(__file__).parent.parent

COOKIES_FILE = Path(
    os.environ.get("LINKEDIN_COOKIES_FILE", str(_GRIDHOUSE_ROOT / "linkedin-cookies.json"))
)
SESSION_DIR = Path(
    os.environ.get("LINKEDIN_SESSION_DIR", str(_GRIDHOUSE_ROOT / "linkedin-session"))
)
CHROME_BIN = os.environ.get(
    "LINKEDIN_CHROME_BIN", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
)
QUERY_LIMIT = int(os.environ.get("LINKEDIN_QUERY_LIMIT", "20"))

# ---------------------------------------------------------------------------
# Iceberg helpers
# ---------------------------------------------------------------------------


def load_active_linkedin_queries(catalog) -> list[str]:
    """Load active LinkedIn query strings from meta.sources.

    Args:
        catalog: Connected PyIceberg catalog.

    Returns:
        List of query strings.
    """
    table = patch_table_io(catalog.load_table("meta.sources"))
    from pyiceberg.expressions import And, EqualTo

    scan = table.scan(
        row_filter=And(EqualTo("source_type", "linkedin"), EqualTo("active", True)),
        selected_fields=("config",),
    )
    queries = []
    for config_str in scan.to_arrow().column("config").to_pylist():
        config = json.loads(config_str)
        queries.append(config["query"])
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
# Scraping
# ---------------------------------------------------------------------------


def _patch_fetch_interceptor(driver) -> None:
    """Inject fetch interceptor to capture post URNs from LinkedIn API responses.

    LinkedIn posts may lack direct feed URLs in the DOM. This interceptor
    captures the URN from the feedUpdateControlMenuRequest API response,
    which is triggered when the post's action menu is opened.

    Args:
        driver: Selenium WebDriver instance.
    """
    driver.execute_script("""
        if (!window.__fetchPatched) {
            window.__menuUrn = null;
            const _origFetch = window.fetch;
            window.fetch = async function(...args) {
                const res = await _origFetch(...args);
                const url = (typeof args[0] === 'string' ? args[0] : '');
                if (url.includes('feedUpdateControlMenuRequest')) {
                    const clone = res.clone();
                    clone.text().then(text => {
                        const m = text.match(/urn%3Ali%3A(?:share|ugcPost|activity)%3A([0-9]+)/);
                        if (m) window.__menuUrn = 'urn:li:' + decodeURIComponent(m[0]).split(':').slice(2).join(':');
                    });
                }
                return res;
            };
            window.__fetchPatched = true;
        }
    """)


def _resolve_post_urls(driver, posts: list[RawVacancy]) -> list[RawVacancy]:
    """Resolve surrogate ck:-prefixed post IDs to real LinkedIn feed URLs.

    For posts without a direct /feed/update/ link in the DOM, clicks the
    action menu button to trigger an API call whose response contains the
    post URN, then builds the real URL from it.

    Args:
        driver: Selenium WebDriver instance.
        posts: List of RawVacancy instances, some may have url starting with "ck:".

    Returns:
        Same list with ck: URLs replaced by real feed URLs where possible.
    """
    for row in posts:
        if not row.url.startswith("ck:"):
            continue
        try:
            ck = row.url[3:]
            driver.execute_script("window.__menuUrn = null;")
            driver.execute_script("""
                const ck = arguments[0];
                const card = document.querySelector('[componentkey="' + ck + '"]');
                if (!card) return;
                const menuBtn = Array.from(card.querySelectorAll('button'))
                    .find(b => (b.getAttribute('aria-label') || '').includes('меню управления'));
                if (menuBtn) menuBtn.click();
            """, ck)
            time.sleep(1.2)
            driver.execute_script(
                "document.dispatchEvent(new KeyboardEvent('keydown', {key: 'Escape', bubbles: true}));"
            )
            urn = driver.execute_script("return window.__menuUrn || null;")
            if urn:
                real_url = f"https://www.linkedin.com/feed/update/{urn}/"
                row.url = real_url
                row.source_id = real_url
            _human_sleep(0.5, 1.0)
        except Exception:
            pass
    return posts


def scrape_query(
    driver,
    catalog,
    query: str,
    seen_ids: set[str],
    limit: int,
) -> int:
    """Scrape LinkedIn search results for a query and write new posts to Bronze.

    Scrolls the search results page, extracts post data via JavaScript,
    deduplicates against seen_ids, then appends new records to Iceberg.

    Args:
        driver: Selenium WebDriver instance (logged in).
        catalog: Connected PyIceberg catalog.
        query: LinkedIn search query string.
        seen_ids: Set of already-stored post_ids for deduplication.
        limit: Maximum number of new posts to collect.

    Returns:
        Number of new posts written.
    """
    url = (
        "https://www.linkedin.com/search/results/content/"
        f"?keywords={quote_plus(query)}&sortBy=date_posted"
    )
    print(f"  → {url}")
    driver.get(url)
    _human_sleep(4, 8)

    _patch_fetch_interceptor(driver)

    # Initial scroll to trigger lazy rendering
    try:
        driver.execute_script(
            "const el = document.querySelector('main') || document.scrollingElement || document.documentElement;"
            " el.scrollBy(0, 400);"
        )
    except Exception:
        print("  [!] Window closed — LinkedIn may have blocked the session.")
        return 0
    _human_sleep(3, 5)

    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)
    batch: list[RawVacancy] = []
    total_new = 0
    scroll_rounds = 0

    while total_new < limit and scroll_rounds < 8:
        posts_data = driver.execute_script("""
            const results = [];
            const cards = Array.from(document.querySelectorAll('[role="listitem"][componentkey]'));

            for (const card of cards) {
                try {
                    const links = Array.from(card.querySelectorAll('a[href]'));

                    const feedLink = links.find(a => a.href && a.href.includes('/feed/update/'));
                    const postUrl = feedLink ? feedLink.href.split('?')[0] : '';
                    const postId = postUrl || ('ck:' + (card.getAttribute('componentkey') || ''));
                    if (!postId) continue;

                    const profileLink = links.find(a =>
                        a.href && a.href.includes('linkedin.com/in/') && a.innerText.trim().length > 1
                    );
                    const authorUrl = profileLink ? profileLink.href.split('?')[0] : '';
                    const author = profileLink ? profileLink.innerText.trim().split('\\n')[0] : '';

                    const dateEl = Array.from(card.querySelectorAll('p, div')).find(e => {
                        const t = (e.innerText || '').trim();
                        return t.length < 20 && (
                            t.includes('мин') || t.includes('ч.') || t.includes('дн.') ||
                            t.includes('нед.') || t.includes('мес.') || t.includes('год')
                        );
                    });
                    const date = dateEl ? dateEl.innerText.trim() : '';

                    const textEls = Array.from(card.querySelectorAll('p, span[dir="ltr"]'));
                    const textEl = textEls.sort((a,b) => b.innerText.length - a.innerText.length)[0];
                    const text = textEl ? textEl.innerText.trim() : card.innerText.slice(0, 1000);

                    const companyLink = links.find(a => a.href && a.href.includes('linkedin.com/company/'));
                    const company = companyLink ? companyLink.innerText.trim() : '';
                    const companyUrl = companyLink ? companyLink.href.split('?')[0] : '';

                    results.push({ author, authorUrl, company, companyUrl, date, text, postId, postUrl: postId });
                } catch(e) {}
            }

            const seen = new Set();
            return results.filter(r => {
                if (seen.has(r.postId)) return false;
                seen.add(r.postId);
                return true;
            });
        """)

        for p in posts_data:
            post_id = p.get("postId", "").strip()
            if not post_id or post_id in seen_ids:
                continue
            seen_ids.add(post_id)
            text = p.get("text", "")
            batch.append(RawVacancy(
                source="linkedin",
                source_id=post_id,
                url=p.get("postUrl", post_id),
                text=text,
                published_at=None,
                extracted_at=extracted_at,
                passed_prefilter=passes_prefilter(text),
                extra={
                    "author":           p.get("author", ""),
                    "author_url":       p.get("authorUrl", ""),
                    "company":          p.get("company", ""),
                    "company_url":      p.get("companyUrl", ""),
                    "published_at_raw": p.get("date", ""),
                    "query":            query,
                },
            ))
            total_new += 1
            if total_new >= limit:
                break

        if total_new >= limit:
            break

        _human_scroll(driver, steps=random.randint(3, 5))
        scroll_rounds += 1
        _human_sleep(2.5, 5.0)

    # Resolve surrogate ck: IDs to real feed URLs
    batch = _resolve_post_urls(driver, batch)

    write_posts(catalog, batch)
    print(f"  ✓ Written: {total_new} new posts")
    return total_new


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def cmd_scrape(queries: list[str]) -> None:
    """Scrape all given queries and write new posts to Bronze.

    Args:
        queries: List of LinkedIn search query strings.
    """
    catalog = get_catalog()
    seen_ids = get_seen_post_ids(catalog)
    print(f"Already in Bronze: {len(seen_ids)} posts\n")

    driver = make_driver()
    try:
        inject_cookies(driver)
        total = 0
        for i, query in enumerate(queries):
            print(f"\n[{i + 1}/{len(queries)}] Query: «{query}»")
            written = scrape_query(driver, catalog, query, seen_ids, QUERY_LIMIT)
            total += written
            if i < len(queries) - 1:
                wait = random.uniform(8, 15)
                print(f"  ⏸ Pause {wait:.0f}s...")
                time.sleep(wait)
    finally:
        driver.quit()

    print(f"\nDone. Total new posts written: {total}")


def main() -> None:
    """CLI entry point for the LinkedIn extractor."""
    args = sys.argv[1:]

    if not args or args[0] == "scrape":
        if len(args) > 1:
            queries = args[1:]
        else:
            catalog = get_catalog()
            queries = load_active_linkedin_queries(catalog)
            if not queries:
                print("No active LinkedIn queries in meta.sources. Run `make init` first.")
                sys.exit(1)

        hour = datetime.now().hour
        if hour < 8 or hour >= 23:
            print(f"[!] Current time {hour}:xx — outside working hours (08:00–23:00).")
            sys.exit(0)

        print(f"Scraping {len(queries)} LinkedIn query/queries...\n")
        cmd_scrape(queries)
    else:
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
