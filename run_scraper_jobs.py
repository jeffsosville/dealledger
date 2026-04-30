#!/usr/bin/env python3
"""
run_scraper_jobs.py  —  V7 config-driven scraper
Reads broker_sources + domain_fingerprints where strategy_status = 'fingerprinted',
scrapes listings using the selectors Claude identified,
upserts results to listings_broker.

Usage:
  export SUPABASE_URL="https://kqckuedsyyosmccushyd.supabase.co"
  export SUPABASE_SERVICE_ROLE_KEY="..."

  # Scrape Tier 2 (50+ listings, requests-mode only, safe to run now)
  python3 run_scraper_jobs.py --priority 30 --render requests

  # Scrape all fingerprinted brokers
  python3 run_scraper_jobs.py --all

  # Single broker test
  python3 run_scraper_jobs.py --url https://goldstarbbaz.com/CurrentListings.aspx

  # Dry run
  python3 run_scraper_jobs.py --priority 30 --dry-run
"""

import os, sys, re, json, time, hashlib, argparse, logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin

import requests as req_lib
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

SUPABASE_HEADERS = {
    "apikey": SUPABASE_SERVICE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    "Content-Type": "application/json",
}

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

MAX_PAGES       = 30    # safety cap on pagination
PAGE_SLEEP      = 1.5   # seconds between pages
BROKER_SLEEP    = 2.0   # seconds between brokers
FETCH_TIMEOUT   = 20
MAX_WORKERS     = 3
BATCH_SIZE      = 200   # upsert batch size


# ── Supabase helpers ──────────────────────────────────────────────────────────
def sb_get(path: str, params: dict = None) -> list:
    r = req_lib.get(f"{SUPABASE_URL}/rest/v1/{path}",
                    headers=SUPABASE_HEADERS, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def sb_upsert(table: str, rows: list[dict]) -> tuple[int, int]:
    if not rows: return 0, 0
    r = req_lib.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
        json=rows, timeout=30,
    )
    if r.status_code in (200, 201):
        return len(rows), 0
    # Fallback: row by row
    ok = err = 0
    for row in rows:
        r2 = req_lib.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers={**SUPABASE_HEADERS, "Prefer": "resolution=merge-duplicates"},
            json=[row], timeout=15,
        )
        if r2.status_code in (200, 201): ok += 1
        else: err += 1
    return ok, err


def sb_patch(table: str, match: dict, update: dict) -> bool:
    params = {k: f"eq.{v}" for k, v in match.items()}
    r = req_lib.patch(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=SUPABASE_HEADERS,
        params=params, json=update, timeout=10,
    )
    return r.status_code in (200, 204)


# ── Fetch ─────────────────────────────────────────────────────────────────────
def fetch_html(url: str) -> tuple[str, int]:
    try:
        r = req_lib.get(url, headers=BROWSER_HEADERS,
                        timeout=FETCH_TIMEOUT, allow_redirects=True)
        return r.text, r.status_code
    except req_lib.exceptions.Timeout:
        return "", 408
    except Exception as e:
        log.debug(f"Fetch error {url}: {e}")
        return "", 0


# ── Price parsing ─────────────────────────────────────────────────────────────
def parse_price(text: str) -> int | None:
    if not text: return None
    # Remove everything except digits and dots
    cleaned = re.sub(r"[^0-9.]", "", str(text).replace(",", ""))
    try:
        val = float(cleaned)
        # Handle shorthand: 1.5M, 500K
        if "m" in text.lower(): val *= 1_000_000
        elif "k" in text.lower(): val *= 1_000
        return int(val) if val > 0 else None
    except:
        return None


# ── Location parsing ──────────────────────────────────────────────────────────
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

def parse_location(text: str) -> tuple[str | None, str | None]:
    """Returns (city, state)."""
    if not text: return None, None
    text = text.strip()
    # Match "City, ST" pattern
    m = re.search(r"([A-Za-z\s]+),\s*([A-Z]{2})", text)
    if m:
        city  = m.group(1).strip()
        state = m.group(2).strip()
        if state in US_STATES:
            return city, state
    # Match just state abbreviation
    m2 = re.search(r"\b([A-Z]{2})\b", text)
    if m2 and m2.group(1) in US_STATES:
        return None, m2.group(1)
    return None, None


# ── Listing ID generation ─────────────────────────────────────────────────────
def make_listing_id(broker_domain: str, listing_url: str, title: str) -> str:
    key = f"{broker_domain}|{listing_url or title or ''}".lower()
    return f"v7:{hashlib.md5(key.encode()).hexdigest()[:16]}"


# ── HTML scraper ──────────────────────────────────────────────────────────────
def scrape_with_selectors(
    start_url: str,
    fingerprint: dict,
    broker_name: str,
    broker_domain: str,
    account_id: str,
) -> list[dict]:
    """
    Scrape listings using CSS selectors from domain_fingerprint.
    Handles pagination automatically.
    """
    container_sel   = fingerprint.get("container_selector")
    title_sel       = fingerprint.get("title_selector")
    price_sel       = fingerprint.get("price_selector")
    location_sel    = fingerprint.get("location_selector")
    detail_link_sel = fingerprint.get("detail_link_selector")
    next_page_sel   = fingerprint.get("next_page_selector") or fingerprint.get("pagination_selector")
    pagination_mode = fingerprint.get("pagination_mode", "next_link")

    if not container_sel:
        log.warning(f"  No container_selector for {broker_domain} — skipping")
        return []

    listings = []
    visited  = set()
    url      = start_url
    page_num = 0
    now      = datetime.now(timezone.utc).isoformat()

    while url and url not in visited and page_num < MAX_PAGES:
        visited.add(url)
        page_num += 1

        html, status = fetch_html(url)
        if not html or status >= 400:
            log.warning(f"  Page {page_num} fetch failed: {status} {url[:60]}")
            break

        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select(container_sel)

        if not cards:
            log.debug(f"  Page {page_num}: no cards found with selector '{container_sel}'")
            break

        log.info(f"  Page {page_num}: {len(cards)} cards @ {url[:70]}")

        for card in cards:
            # Title
            title = None
            if title_sel:
                el = card.select_one(title_sel)
                title = el.get_text(strip=True) if el else None
            if not title:
                # fallback: h2, h3, first link text
                for tag in ["h2", "h3", "h4", ".title", ".listing-title"]:
                    el = card.select_one(tag)
                    if el:
                        title = el.get_text(strip=True)
                        break

            # Price
            price_text = None
            if price_sel:
                el = card.select_one(price_sel)
                price_text = el.get_text(strip=True) if el else None
            if not price_text:
                for pattern in [r"\$[\d,]+", r"asking.{0,20}\$[\d,]+"]:
                    m = re.search(pattern, card.get_text(), re.I)
                    if m:
                        price_text = m.group(0)
                        break

            # Location
            location_text = None
            if location_sel:
                el = card.select_one(location_sel)
                location_text = el.get_text(strip=True) if el else None

            # Detail URL
            detail_url = None
            if detail_link_sel:
                el = card.select_one(detail_link_sel)
                if el and el.get("href"):
                    detail_url = urljoin(url, el["href"])
            if not detail_url:
                el = card.select_one("a[href]")
                if el:
                    detail_url = urljoin(url, el["href"])

            if not title and not detail_url:
                continue

            city, state = parse_location(location_text or "")
            price = parse_price(price_text)
            listing_id = make_listing_id(broker_domain, detail_url or "", title or "")

            listings.append({
                "id":           listing_id,
                "broker_name":  account_id or broker_name,
                "listing_url":  detail_url or url,
                "title":        (title or "")[:500],
                "price":        price,
                "cash_flow":    None,
                "revenue":      None,
                "location_city": city,
                "location_state": state,
                "location_raw": location_text,
                "description":  None,
                "source":       "broker_direct",
                "trust_tier":   "direct",
                "is_active":    True,
                "scraped_at":   now,
                "last_seen":    now,
            })

        # Pagination
        next_url = None
        if next_page_sel and pagination_mode not in ("none", "single_page"):
            el = soup.select_one(next_page_sel)
            if el and el.get("href"):
                next_url = urljoin(url, el["href"])
                if next_url == url:
                    next_url = None
        elif pagination_mode == "numbered":
            # Try appending page number
            if page_num == 1:
                test_url = re.sub(r"[?&]page=\d+", "", url)
                sep = "&" if "?" in test_url else "?"
                next_url = f"{test_url}{sep}page=2"
            elif page_num > 1:
                next_url = re.sub(r"page=\d+", f"page={page_num+1}", url)

        url = next_url
        if url:
            time.sleep(PAGE_SLEEP)

    return listings


# ── Load from DB ──────────────────────────────────────────────────────────────
def load_scrape_targets(
    max_priority: int = 100,
    render_filter: str | None = None,
    min_confidence: float = 0.0,
    limit: int = 200,
) -> list[dict]:
    """Load fingerprinted brokers with their domain fingerprints."""

    # Get fingerprinted broker_sources
    bs_params = {
        "select": "id,account_id,company_name,listing_url,domain,priority,active_listings_est,strategy_status",
        "strategy_status": "eq.fingerprinted",
        "order": "priority.asc,active_listings_est.desc.nullslast",
        "limit": str(limit),
    }
    if max_priority < 100:
        bs_params["priority"] = f"lte.{max_priority}"

    brokers = sb_get("broker_sources", bs_params)
    if not brokers:
        return []

    # Get domain fingerprints
    domains = list({b["domain"] for b in brokers if b.get("domain")})
    if not domains:
        return []

    # Fetch fingerprints in batches
    fingerprints = {}
    for i in range(0, len(domains), 50):
        batch = domains[i:i+50]
        domain_filter = f"in.({','.join(batch)})"
        fp_params = {"select": "*", "domain": domain_filter}
        rows = sb_get("domain_fingerprints", fp_params)
        for fp in rows:
            fingerprints[fp["domain"]] = fp

    # Join
    targets = []
    for b in brokers:
        domain = b.get("domain")
        fp = fingerprints.get(domain)
        if not fp:
            continue

        # Filter by render mode
        if render_filter and fp.get("render_mode") != render_filter:
            continue

        # Filter by confidence
        conf = fp.get("confidence") or 0
        if conf < min_confidence:
            continue

        # Skip if no container selector
        if not fp.get("container_selector"):
            continue

        targets.append({**b, "fingerprint": fp})

    log.info(f"Loaded {len(targets)} scrape targets "
             f"(priority≤{max_priority}, render={render_filter or 'any'}, "
             f"confidence≥{min_confidence})")
    return targets


# ── Process one broker ────────────────────────────────────────────────────────
def process_broker(target: dict) -> dict:
    name      = target.get("company_name", "?")
    url       = target.get("listing_url", "")
    domain    = target.get("domain", "")
    acct      = target.get("account_id", "")
    fp        = target.get("fingerprint", {})
    b_id      = target.get("id")
    listings_est = target.get("active_listings_est") or 0

    log.info(f"  [{listings_est:4d}] {name[:45]:<45} {url[:55]}")

    try:
        listings = scrape_with_selectors(url, fp, name, domain, acct)
    except Exception as e:
        log.error(f"  Scrape error: {e}")
        sb_patch("broker_sources", {"id": b_id}, {
            "strategy_status": "failed",
            "last_error_type": "SCRAPE_ERROR",
            "last_error_message": str(e)[:500],
        })
        return {"broker": name, "success": False, "error": str(e)}

    if not listings:
        log.warning(f"  No listings found for {domain}")
        sb_patch("broker_sources", {"id": b_id}, {
            "last_error_type": "EMPTY_RESULTS",
            "last_listing_count": 0,
        })
        return {"broker": name, "success": True, "found": 0}

    # Upsert to listings_broker
    total_ok = total_err = 0
    for i in range(0, len(listings), BATCH_SIZE):
        batch = listings[i: i + BATCH_SIZE]
        ok, err = sb_upsert("listings_broker", batch)
        total_ok  += ok
        total_err += err

    now = datetime.now(timezone.utc).isoformat()
    sb_patch("broker_sources", {"id": b_id}, {
        "strategy_status":   "stable",
        "last_success_at":   now,
        "last_listing_count": len(listings),
        "consecutive_failures": 0,
    })

    log.info(f"  ✓ {len(listings)} listings found, {total_ok} upserted, {total_err} errors")
    return {"broker": name, "success": True, "found": len(listings), "upserted": total_ok}


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Scrape broker listings using fingerprint configs")
    parser.add_argument("--priority",    type=int,   default=30)
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--render",      type=str,   default="requests",
                        help="Filter by render_mode: requests | playwright | any")
    parser.add_argument("--confidence",  type=float, default=0.3,
                        help="Min fingerprint confidence (default: 0.3)")
    parser.add_argument("--limit",       type=int,   default=200)
    parser.add_argument("--workers",     type=int,   default=MAX_WORKERS)
    parser.add_argument("--url",         type=str,   help="Scrape a single URL (uses DB fingerprint)")
    parser.add_argument("--dry-run",     action="store_true")
    args = parser.parse_args()

    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        sys.exit(1)

    render_filter = None if args.render == "any" else args.render
    max_priority  = 100 if args.all else args.priority

    targets = load_scrape_targets(
        max_priority=max_priority,
        render_filter=render_filter,
        min_confidence=args.confidence,
        limit=args.limit,
    )

    if not targets:
        log.info("No targets found. Run fingerprint_brokers.py first, or lower --confidence.")
        return

    if args.dry_run:
        print(f"\nDRY RUN — would scrape {len(targets)} brokers:\n")
        for t in targets:
            fp = t.get("fingerprint", {})
            print(f"  P{t['priority']:3d}  {str(t.get('active_listings_est','?')):>5}  "
                  f"conf={fp.get('confidence') or 0:.2f}  "
                  f"{str(t['company_name'])[:40]:40s}  "
                  f"{fp.get('render_mode','?'):10s}  "
                  f"{t['listing_url'][:50]}")
        return

    log.info(f"\nStarting scrape: {len(targets)} brokers, {args.workers} workers\n")
    started = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_broker, t): t for t in targets}
        for future in as_completed(futures):
            try:
                r = future.result()
                results.append(r)
            except Exception as e:
                t = futures[future]
                log.error(f"Worker error {t.get('listing_url')}: {e}")
                results.append({"success": False})
            time.sleep(0.3)

    elapsed   = time.time() - started
    succeeded = sum(1 for r in results if r.get("success"))
    total_found    = sum(r.get("found", 0) for r in results)
    total_upserted = sum(r.get("upserted", 0) for r in results)

    print(f"\n{'='*60}")
    print(f"  SCRAPE RUN COMPLETE")
    print(f"  Brokers:    {len(results)} processed, {succeeded} succeeded")
    print(f"  Listings:   {total_found} found, {total_upserted} upserted")
    print(f"  Elapsed:    {elapsed:.0f}s")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
