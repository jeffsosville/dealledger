#!/usr/bin/env python3
"""
broker_scraper.py

Scrapes all non-RE broker sites from DealLedger's broker list.
Writes direct listings to DealLedger Supabase → listings_broker table.

Usage:
    python3 scrapers/broker_scraper.py
    python3 scrapers/broker_scraper.py --skip-re --workers 8
    python3 scrapers/broker_scraper.py --test --limit 20
    python3 scrapers/broker_scraper.py --broker sunbelt

Schema target: DealLedger Supabase (kqckuedsyyosmccushyd)
Table: listings_broker
"""

import os, re, sys, time, json, hashlib, argparse, logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL  = os.environ.get("SUPABASE_URL",  "https://kqckuedsyyosmccushyd.supabase.co")
SUPABASE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "")
PROXY         = "2e675ba5977dd3336e3d__cr.us:719577c3bc6fb269@gw.dataimpulse.com:823"
PROXY_URL     = f"http://{PROXY}"

# ── RE broker exclusion patterns ──────────────────────────────────────────────
RE_PATTERNS = [
    r"real.?estate", r"realty", r"realtor", r"property.?management",
    r"commercial.?re", r"residential.?re", r"land.?broker",
    r"mortgage", r"title.?company", r"home.?sales", r"apartment",
    r"condo", r"housing", r"reit", r"rei\b",
]

def is_re_broker(name: str, url: str) -> bool:
    text = f"{name} {url}".lower()
    return any(re.search(p, text) for p in RE_PATTERNS)

# ── Known specialized broker scrapers (the Big 5 + others) ───────────────────
SPECIALIZED = {
    "sunbelt": {
        "domains": ["sunbeltnetwork.com"],
        "listing_api": "https://www.sunbeltnetwork.com/api/listings/search",
        "params": {"pageSize": 100, "pageNumber": 1},
    },
    "transworld": {
        "domains": ["transworldma.com", "tworld.com"],
        "listing_api": None,  # HTML scrape
        "list_url": "https://www.tworld.com/businesses-for-sale/",
    },
    "murphy": {
        "domains": ["murphybusiness.com"],
        "listing_api": "https://www.murphybusiness.com/api/listings",
        "params": {"take": 100, "skip": 0},
    },
    "vr": {
        "domains": ["vrbusinessbrokers.com"],
        "listing_api": None,
        "list_url": "https://www.vrbusinessbrokers.com/businesses-for-sale/",
    },
    "fcbb": {
        "domains": ["fcbb.com"],
        "listing_api": None,
        "list_url": "https://www.fcbb.com/listings/",
    },
}

# ── Supabase helpers ──────────────────────────────────────────────────────────
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def sb_get_broker_list() -> list[dict]:
    """Fetch broker list from DealLedger Supabase brokers table."""
    rows, offset = [], 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/brokers",
            headers=sb_headers(),
            params={
                "select": "id,name,url,account",
                
                "limit": 1000,
                "offset": offset,
            },
            timeout=30,
        )
        if r.status_code != 200:
            log.warning(f"Broker list fetch error: {r.status_code}")
            break
        batch = r.json()
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
        time.sleep(0.1)
    return rows

def sb_upsert_listings(rows: list[dict]) -> bool:
    if not rows:
        return True
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/listings_broker",
            headers=sb_headers(),
            json=batch,
            timeout=60,
        )
        if r.status_code not in (200, 201):
            log.error(f"Upsert error {r.status_code}: {r.text[:200]}")
            return False
        time.sleep(0.1)
    return True

# ── Generic HTML listing scraper ──────────────────────────────────────────────
def make_session():
    session = cffi_requests.Session(impersonate="chrome120")
    session.proxies = {"http": PROXY_URL, "https": PROXY_URL}
    return session

def extract_listings_from_html(html: str, broker_url: str, broker_name: str) -> list[dict]:
    """
    Generic pattern detection for broker listing pages.
    Looks for price patterns, title patterns, common listing card structures.
    """
    soup = BeautifulSoup(html, "lxml")
    listings = []

    # Remove nav/header/footer noise
    for tag in soup.find_all(["nav", "header", "footer", "script", "style"]):
        tag.decompose()

    # Common listing card patterns
    card_selectors = [
        "article.listing", "div.listing-card", "div.listing-item",
        "li.listing", ".business-listing", ".listing-result",
        "[class*='listing']", "[class*='business-card']",
        "article", "li.result",
    ]

    cards = []
    for sel in card_selectors:
        found = soup.select(sel)
        if len(found) >= 3:  # need at least 3 to be meaningful
            cards = found[:200]
            break

    if not cards:
        # Fallback: look for price patterns in page
        price_pattern = re.compile(r'\$[\d,]+(?:K|M)?(?:\s*(?:million|thousand))?', re.I)
        prices = price_pattern.findall(html)
        if prices:
            log.debug(f"Found {len(prices)} prices in {broker_url} but no card structure")
        return []

    for card in cards:
        text = card.get_text(" ", strip=True)

        # Extract URL
        link = card.find("a", href=True)
        href = link["href"] if link else None
        if href and not href.startswith("http"):
            base = f"https://{urlparse(broker_url).netloc}"
            href = base + href if href.startswith("/") else f"{base}/{href}"

        # Skip non-listing links
        if href and any(x in href for x in ["#", "javascript:", "mailto:", "/about", "/contact", "/login"]):
            continue

        # Extract price
        price = None
        price_match = re.search(r'\$\s*([\d,]+(?:\.\d+)?)\s*([KkMm])?', text)
        if price_match:
            num = float(price_match.group(1).replace(",", ""))
            mult = {"k": 1000, "K": 1000, "m": 1_000_000, "M": 1_000_000}.get(price_match.group(2) or "", 1)
            price = int(num * mult)
            # Sanity check: business prices $10k–$50M
            if not (10_000 <= price <= 50_000_000):
                price = None

        # Extract title (first heading or strong text)
        title = None
        for tag in ["h2", "h3", "h4", "strong", "b"]:
            el = card.find(tag)
            if el and len(el.get_text(strip=True)) > 10:
                title = el.get_text(strip=True)[:200]
                break
        if not title:
            title = text[:100] if text else None

        # Extract location
        location = None
        loc_match = re.search(
            r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)?,\s*(?:' +
            '|'.join(["AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
                      "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
                      "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
                      "TX","UT","VT","VA","WA","WV","WI","WY"]) +
            r'))\b', text
        )
        if loc_match:
            location = loc_match.group(1)

        if title and href and price:
            uid = hashlib.md5(href.encode()).hexdigest()[:16]
            listings.append({
                "id":           f"{broker_name[:20]}:{uid}",
                "broker_name":  broker_name,
                "broker_url":   broker_url,
                "listing_url":  href,
                "title":        title,
                "price":        price,
                "location_raw": location,
                "source":       "broker_direct",
                "first_seen":   datetime.now(timezone.utc).isoformat(),
                "last_seen":    datetime.now(timezone.utc).isoformat(),
                "is_active":    True,
            })

    return listings

def scrape_broker(broker: dict, session) -> list[dict]:
    """Scrape a single broker site. Returns list of listings."""
    name = broker.get("name", "")
    url  = broker.get("url", "")

    if not url:
        return []

    # Ensure proper URL format
    if not url.startswith("http"):
        url = f"https://{url}"

    # Try common listing page paths
    paths_to_try = [
        url,
        url.rstrip("/") + "/businesses-for-sale",
        url.rstrip("/") + "/listings",
        url.rstrip("/") + "/buy-a-business",
        url.rstrip("/") + "/for-sale",
    ]

    for path in paths_to_try:
        try:
            r = session.get(path, timeout=20, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 5000:
                listings = extract_listings_from_html(r.text, url, name)
                if listings:
                    log.info(f"  ✓ {name}: {len(listings)} listings from {path}")
                    return listings
        except Exception as e:
            log.debug(f"  ✗ {name} @ {path}: {e}")
            continue

    return []

# ── Main ──────────────────────────────────────────────────────────────────────
def run(args):
    if not SUPABASE_KEY:
        log.error("Set SUPABASE_SERVICE_KEY env var")
        sys.exit(1)

    log.info("Fetching broker list from DealLedger...")
    brokers = sb_get_broker_list()
    log.info(f"Total brokers in DB: {len(brokers)}")

    # Filter RE brokers
    if args.skip_re:
        before = len(brokers)
        brokers = [b for b in brokers if not is_re_broker(b.get("name",""), b.get("url",""))]
        log.info(f"After RE filter: {len(brokers)} (removed {before - len(brokers)} RE brokers)")

    # Filter to specific broker
    if args.broker:
        brokers = [b for b in brokers if args.broker.lower() in (b.get("name","") or "").lower()]
        log.info(f"Filtered to broker '{args.broker}': {len(brokers)}")

    # Test mode limit
    if args.test:
        brokers = brokers[:args.limit]
        log.info(f"Test mode: {len(brokers)} brokers")

    log.info(f"Scraping {len(brokers)} brokers with {args.workers} workers...")

    session = make_session()
    all_listings = []
    success = 0
    failed  = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(scrape_broker, b, session): b for b in brokers}
        for i, future in enumerate(as_completed(futures)):
            broker = futures[future]
            try:
                listings = future.result()
                if listings:
                    all_listings.extend(listings)
                    success += 1
                else:
                    failed += 1
            except Exception as e:
                log.debug(f"Error scraping {broker.get('name')}: {e}")
                failed += 1

            if (i + 1) % 50 == 0:
                log.info(f"Progress: {i+1}/{len(brokers)} | Listings so far: {len(all_listings)}")

    log.info(f"\n{'='*60}")
    log.info(f"RESULTS: {len(all_listings)} listings | {success} brokers succeeded | {failed} failed")

    # Dedup by URL
    seen_urls = set()
    deduped = []
    for l in all_listings:
        url = l.get("listing_url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(l)
    log.info(f"After dedup: {len(deduped)} unique listings")

    if args.test:
        log.info("Test mode — not writing to Supabase")
        for l in deduped[:5]:
            log.info(f"  Sample: {l['title'][:60]} | ${l.get('price','?')} | {l['broker_name']}")
        return

    log.info("Upserting to Supabase listings_broker...")
    if sb_upsert_listings(deduped):
        log.info("Done.")
    else:
        log.error("Upsert failed.")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-re",  action="store_true", default=True,  help="Skip real estate brokers")
    parser.add_argument("--workers",  type=int, default=8,  help="Concurrent workers")
    parser.add_argument("--broker",   type=str,             help="Filter to specific broker name")
    parser.add_argument("--test",     action="store_true",  help="Test mode, don't write to DB")
    parser.add_argument("--limit",    type=int, default=20, help="Limit in test mode")
    args = parser.parse_args()
    run(args)
