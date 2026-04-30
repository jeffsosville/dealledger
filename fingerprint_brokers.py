#!/usr/bin/env python3
"""
fingerprint_brokers.py  —  V7 domain fingerprinting
Reads broker_sources where strategy_status = 'pending',
fetches each URL, sends HTML to Claude API,
writes pattern config to domain_fingerprints,
updates broker_sources.strategy_status = 'fingerprinted'.

Usage:
  export SUPABASE_URL="https://kqckuedsyyosmccushyd.supabase.co"
  export SUPABASE_SERVICE_ROLE_KEY="..."
  export ANTHROPIC_API_KEY="..."

  # Tier 2 only (50+ listings, fastest, ~$0.40)
  python3 fingerprint_brokers.py --priority 30

  # All brokers (~$9 via batch)
  python3 fingerprint_brokers.py --all

  # Single broker test
  python3 fingerprint_brokers.py --url https://synergybb.com/businesses-for-sale/
"""

import os, sys, re, json, time, argparse, hashlib, logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests as req_lib

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

FETCH_TIMEOUT   = 15    # seconds per URL fetch
MAX_HTML_CHARS  = 40000 # chars sent to Claude (keeps tokens ~10k)
MAX_WORKERS     = 4     # concurrent fingerprint workers
RETRY_SLEEP     = 2     # seconds between retries

CLAUDE_MODEL    = "claude-haiku-4-5-20251001"  # cheapest, fast, good enough for HTML analysis

# ── System prompt for Claude ──────────────────────────────────────────────────
SYSTEM_PROMPT = """You are a web scraping architect. You will be given HTML from a business broker listing page.
Your job is to analyze the HTML and return a JSON fingerprint so an automated scraper can extract listings.

Return ONLY valid JSON, no markdown, no explanation. Use null for fields you cannot determine.

Required fields:
{
  "platform": "wordpress|nextjs|custom|asp|squarespace|wix|unknown",
  "has_json_ld": true|false,
  "has_next_data": true|false,
  "has_wp_json": true|false,
  "has_listing_api": true|false,
  "listing_api_url": "url or null",
  "container_selector": "CSS selector for the listing card container",
  "detail_link_selector": "CSS selector for links to individual listing pages",
  "title_selector": "CSS selector for listing title within each card",
  "price_selector": "CSS selector for asking price within each card",
  "location_selector": "CSS selector for location/city/state within each card",
  "pagination_selector": "CSS selector for next page link or null if single page",
  "next_page_selector": "CSS selector or URL pattern for next page",
  "total_count_selector": "CSS selector for total listing count text",
  "render_mode": "requests|playwright",
  "pagination_mode": "none|next_link|numbered|api_offset|state_nav|infinite_scroll",
  "proxy_required": false,
  "confidence": 0.0-1.0,
  "scraper_notes": "brief notes on any quirks or special handling needed"
}

Hints:
- If you see __NEXT_DATA__ in the HTML, platform is nextjs and render_mode is requests
- If you see wp-content or wp-json, platform is wordpress
- If you see .aspx URLs, platform is asp and render_mode is playwright
- If the page has JSON-LD with @type BusinessForSale or Product, has_json_ld = true
- If listings load via JavaScript with no HTML cards visible, render_mode = playwright
- confidence should reflect how certain you are the selectors will work (0.9 = very confident)
"""

# ── Supabase helpers ──────────────────────────────────────────────────────────
def sb_get(path: str, params: dict = None) -> list:
    r = req_lib.get(f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=SUPABASE_HEADERS, params=params, timeout=10)
    r.raise_for_status()
    return r.json()


def sb_upsert(table: str, row: dict) -> bool:
    r = req_lib.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=row, timeout=10,
    )
    return r.status_code in (200, 201)


def sb_patch(table: str, match: dict, update: dict) -> bool:
    params = {k: f"eq.{v}" for k, v in match.items()}
    r = req_lib.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPABASE_HEADERS,
        params=params,
        json=update, timeout=10,
    )
    return r.status_code in (200, 204)


# ── Fetch helpers ─────────────────────────────────────────────────────────────
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
}


def fetch_html(url: str, timeout: int = FETCH_TIMEOUT) -> tuple[str, int]:
    """Returns (html, status_code). html is empty string on failure."""
    try:
        r = req_lib.get(url, headers=BROWSER_HEADERS, timeout=timeout,
                        allow_redirects=True)
        return r.text, r.status_code
    except req_lib.exceptions.Timeout:
        return "", 408
    except req_lib.exceptions.TooManyRedirects:
        return "", 310
    except Exception as e:
        log.debug(f"Fetch error {url}: {e}")
        return "", 0


def truncate_html(html: str, max_chars: int = MAX_HTML_CHARS) -> str:
    """Keep head + first chunk of body for token efficiency."""
    if len(html) <= max_chars:
        return html
    # Always include <head> for meta/script detection
    head_end = html.find("</head>")
    if head_end > 0 and head_end < max_chars // 2:
        head = html[:head_end + 7]
        body_budget = max_chars - len(head)
        body_start = html.find("<body")
        if body_start > 0:
            return head + html[body_start: body_start + body_budget]
    return html[:max_chars]


# ── Claude API ────────────────────────────────────────────────────────────────
def call_claude(html: str, url: str) -> dict | None:
    """Send HTML to Claude, parse JSON fingerprint."""
    if not ANTHROPIC_KEY:
        log.error("ANTHROPIC_API_KEY not set")
        return None

    prompt = f"URL: {url}\n\nHTML:\n{truncate_html(html)}"

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 1000,
        "system": SYSTEM_PROMPT,
        "messages": [{"role": "user", "content": prompt}],
    }

    try:
        r = req_lib.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=payload, timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"Claude API error {r.status_code}: {r.text[:200]}")
            return None

        text = r.json()["content"][0]["text"].strip()

        # Strip markdown code fences if present
        text = re.sub(r"^```(?:json)?\n?", "", text)
        text = re.sub(r"\n?```$", "", text)

        return json.loads(text)

    except json.JSONDecodeError as e:
        log.warning(f"Claude returned invalid JSON: {e}")
        return None
    except Exception as e:
        log.warning(f"Claude call failed: {e}")
        return None


# ── Core fingerprint logic ────────────────────────────────────────────────────
def fingerprint_url(url: str, broker_id: str | None = None) -> dict:
    """
    Full fingerprint pipeline for one URL.
    Returns result dict with keys: success, domain, fingerprint, error_type, error_msg
    """
    domain = urlparse(url).netloc.lower().replace("www.", "")
    result = {"url": url, "domain": domain, "broker_id": broker_id, "success": False}

    # 1. Fetch HTML
    html, status = fetch_html(url)

    if status == 403:
        result["error_type"] = "HTTP_403"
        result["error_msg"] = "Access forbidden"
        return result
    if status == 408:
        result["error_type"] = "TIMEOUT"
        result["error_msg"] = "Request timed out"
        return result
    if status == 0 or not html:
        result["error_type"] = "FETCH_ERROR"
        result["error_msg"] = f"HTTP {status}, empty response"
        return result
    if status >= 400:
        result["error_type"] = f"HTTP_{status}"
        result["error_msg"] = f"HTTP {status}"
        return result

    # Quick pre-checks before calling Claude
    js_signals = [
        "__NEXT_DATA__", "window.__APP_STATE__", "ReactDOM",
        "ng-version", "data-reactroot",
    ]
    needs_js = any(sig in html for sig in js_signals)

    # 2. Call Claude
    fp = call_claude(html, url)
    if not fp:
        result["error_type"] = "CLAUDE_FAILED"
        result["error_msg"] = "Claude API returned no usable response"
        return result

    # 3. Apply pre-check overrides
    if needs_js and fp.get("render_mode") == "requests":
        # Trust our signal over Claude if we see strong JS indicators
        if "__NEXT_DATA__" in html:
            fp["platform"] = "nextjs"
            # nextjs is actually scrapable with requests via __NEXT_DATA__
        elif "ReactDOM" in html and "container_selector" not in fp:
            fp["render_mode"] = "playwright"

    # 4. Build DB row
    now = datetime.now(timezone.utc).isoformat()
    db_row = {
        "domain":                domain,
        "platform":              fp.get("platform"),
        "has_json_ld":           fp.get("has_json_ld", False),
        "has_next_data":         fp.get("has_next_data", False),
        "has_wp_json":           fp.get("has_wp_json", False),
        "has_listing_api":       fp.get("has_listing_api", False),
        "listing_api_url":       fp.get("listing_api_url"),
        "container_selector":    fp.get("container_selector"),
        "detail_link_selector":  fp.get("detail_link_selector"),
        "title_selector":        fp.get("title_selector"),
        "price_selector":        fp.get("price_selector"),
        "location_selector":     fp.get("location_selector"),
        "pagination_selector":   fp.get("pagination_selector"),
        "next_page_selector":    fp.get("next_page_selector"),
        "total_count_selector":  fp.get("total_count_selector"),
        "render_mode":           fp.get("render_mode", "requests"),
        "proxy_required":        fp.get("proxy_required", False),
        "confidence":            fp.get("confidence"),
        "fingerprint_json":      json.dumps(fp),
        "last_verified_at":      now,
        "last_success_at":       now,
    }

    result["success"]     = True
    result["fingerprint"] = db_row
    result["fp_raw"]      = fp
    return result


# ── Load brokers from Supabase ────────────────────────────────────────────────
def load_pending_brokers(max_priority: int = 100, limit: int = 500) -> list[dict]:
    """Load brokers with strategy_status = 'pending', ordered by priority."""
    params = {
        "select": "id,company_name,listing_url,domain,priority,active_listings_est,strategy_type",
        "strategy_status": "eq.pending",
        "order": "priority.asc,active_listings_est.desc.nullslast",
        "limit": str(limit),
    }
    if max_priority < 100:
        params["priority"] = f"lte.{max_priority}"

    try:
        brokers = sb_get("broker_sources", params)
        log.info(f"Loaded {len(brokers)} pending brokers (priority ≤ {max_priority})")
        return brokers
    except Exception as e:
        log.error(f"Failed to load brokers: {e}")
        return []


# ── Process one broker ────────────────────────────────────────────────────────
def process_broker(broker: dict) -> dict:
    """Fingerprint one broker. Returns summary dict."""
    url      = broker.get("listing_url", "")
    name     = broker.get("company_name", url)
    b_id     = broker.get("id")
    listings = broker.get("active_listings_est") or 0

    log.info(f"  [{listings:4d}] {name[:45]:<45} {url[:55]}")

    result = fingerprint_url(url, b_id)

    now = datetime.now(timezone.utc).isoformat()

    if result["success"]:
        # Write fingerprint
        fp_ok = sb_upsert("domain_fingerprints", result["fingerprint"])

        # Update broker_sources
        broker_update = {
            "strategy_status":      "fingerprinted",
            "last_fingerprinted_at": now,
            "last_success_at":      now,
            "last_http_status":     200,
            "consecutive_failures": 0,
        }
        # Carry strategy hints from fingerprint back to broker
        fp = result.get("fp_raw", {})
        if fp.get("render_mode"):
            broker_update["render_mode"] = fp["render_mode"]
        if fp.get("platform") and fp["platform"] not in ("unknown", None):
            broker_update["strategy_type"] = fp["platform"]

        sb_patch("broker_sources", {"id": b_id}, broker_update)

        conf = fp.get("confidence", 0) or 0
        log.info(f"    ✓ {fp.get('platform','?')} | {fp.get('render_mode','?')} | confidence={conf:.2f}")
        return {"broker": name, "url": url, "success": True, "confidence": conf,
                "platform": fp.get("platform"), "render_mode": fp.get("render_mode")}
    else:
        err_type = result.get("error_type", "UNKNOWN")
        err_msg  = result.get("error_msg", "")

        sb_patch("broker_sources", {"id": b_id}, {
            "strategy_status":    "failed",
            "last_failure_at":    now,
            "last_error_type":    err_type,
            "last_error_message": err_msg[:500],
            "consecutive_failures": 1,
        })

        log.warning(f"    ✗ {err_type}: {err_msg}")
        return {"broker": name, "url": url, "success": False,
                "error_type": err_type, "error_msg": err_msg}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Fingerprint broker listing URLs")
    parser.add_argument("--priority", type=int, default=30,
                        help="Max priority to process (default: 30 = 50+ listings)")
    parser.add_argument("--all",      action="store_true",
                        help="Process all pending brokers regardless of priority")
    parser.add_argument("--limit",    type=int, default=500,
                        help="Max brokers to process in this run")
    parser.add_argument("--workers",  type=int, default=MAX_WORKERS,
                        help="Concurrent workers (default: 4)")
    parser.add_argument("--url",      type=str,
                        help="Fingerprint a single URL (skips DB, prints result)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Show which brokers would be processed, don't run")
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)
    if not ANTHROPIC_KEY:
        print("ERROR: Set ANTHROPIC_API_KEY")
        sys.exit(1)

    # Single URL mode
    if args.url:
        log.info(f"Single URL mode: {args.url}")
        result = fingerprint_url(args.url)
        print("\n" + json.dumps(result.get("fp_raw") or result, indent=2))
        return

    max_priority = 100 if args.all else args.priority
    brokers = load_pending_brokers(max_priority=max_priority, limit=args.limit)

    if not brokers:
        log.info("No pending brokers found.")
        return

    if args.dry_run:
        print(f"\nDRY RUN — would fingerprint {len(brokers)} brokers:\n")
        for b in brokers[:20]:
            print(f"  P{b['priority']:3d}  {str(b.get('active_listings_est','?')):>5}  "
                  f"{str(b['company_name'])[:40]:40s}  {b['listing_url'][:60]}")
        if len(brokers) > 20:
            print(f"  ... and {len(brokers)-20} more")
        # Estimate cost
        tokens_in  = len(brokers) * 8000
        tokens_out = len(brokers) * 300
        cost_std   = (tokens_in / 1e6 * 1.0) + (tokens_out / 1e6 * 5.0)
        cost_batch = cost_std * 0.5
        print(f"\n  Estimated cost: ${cost_std:.2f} standard | ${cost_batch:.2f} batch")
        return

    # Run fingerprinting
    log.info(f"\nStarting fingerprint run: {len(brokers)} brokers, {args.workers} workers\n")
    started_at = time.time()

    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_broker, b): b for b in brokers}
        for future in as_completed(futures):
            try:
                r = future.result()
                results.append(r)
            except Exception as e:
                broker = futures[future]
                log.error(f"Worker error for {broker.get('listing_url')}: {e}")
                results.append({"success": False, "error_type": "WORKER_ERROR"})
            # Small delay to avoid hammering Claude API
            time.sleep(0.5)

    # Summary
    elapsed   = time.time() - started_at
    succeeded = sum(1 for r in results if r.get("success"))
    failed    = len(results) - succeeded
    avg_conf  = sum(r.get("confidence") or 0 for r in results if r.get("success")) / max(succeeded, 1)

    platforms = {}
    for r in results:
        if r.get("success"):
            p = r.get("platform") or "unknown"
            platforms[p] = platforms.get(p, 0) + 1

    print(f"\n{'='*60}")
    print(f"  FINGERPRINT RUN COMPLETE")
    print(f"  Brokers processed: {len(results)}")
    print(f"  Succeeded:         {succeeded}")
    print(f"  Failed:            {failed}")
    print(f"  Avg confidence:    {avg_conf:.2f}")
    print(f"  Elapsed:           {elapsed:.0f}s ({elapsed/max(len(results),1):.1f}s/broker)")
    print(f"\n  Platform distribution:")
    for p, c in sorted(platforms.items(), key=lambda x: -x[1]):
        print(f"    {p:20s}: {c}")

    if failed:
        print(f"\n  Failed brokers:")
        for r in results:
            if not r.get("success"):
                print(f"    {r.get('error_type','?'):20s}  {r.get('url','')[:60]}")

    # Estimated cost
    tokens_used = len(results) * 8300
    cost = tokens_used / 1e6 * 1.0  # Haiku input rate
    print(f"\n  Estimated API cost: ~${cost:.2f}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
