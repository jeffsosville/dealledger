#!/usr/bin/env python3
"""
DealLedger Scraper v4 — Zero API Cost
======================================
Replaces the Claude API agentic loop with rule-based pattern detection.
Same outputs (scraper configs + listings), $0 in API costs.

Run:
  python scraper_agent_v4.py --batch data/brokers_clean.csv --workers 5
  python scraper_agent_v4.py --url https://example.com/listings
"""

import argparse, csv, json, os, re, sys, time
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup, Comment

try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except:
    HAS_CFFI = False

# Import specialized scrapers (unchanged from v3)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scrapers'))
try:
    from specialized_scrapers import scrape_specialized_broker
    HAS_SPECIALIZED = True
except ImportError:
    HAS_SPECIALIZED = False

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

# ── Known specialized brokers (same as v3) ────────────────────────────────────

KNOWN_DOMAINS = {
    "murphybusiness.com":        "Murphy Business",
    "hedgestone.com":            "Hedgestone Business Advisors",
    "tworld.com":                "Transworld Business Advisors",
    "transworld.net":            "Transworld Business Advisors",
    "twbusinessadvisors.com":    "Transworld Business Advisors",
    "sunbeltnetwork.com":        "Sunbelt Business Brokers",
    "vrbusinessbrokers.com":     "VR Business Brokers",
    "vrbbusa.com":               "VR Business Brokers",
    "fcbb.com":                  "First Choice Business Brokers",
    "linkbusiness.com":          "Link Business",
    "execbb.com":                "Executive Business Brokers",
}

# ── Pattern library — CSS selectors to try, in priority order ─────────────────
# These cover 80%+ of broker sites without any AI

CONTAINER_PATTERNS = [
    # Generic listing cards
    ".listing-item", ".listing-card", ".listing",
    ".business-listing", ".business-card", ".business-item",
    ".property-item", ".property-card", ".property-listing",
    # BizBuySell / BizQuest style
    ".result-item", ".search-result", ".result-card",
    # Grid/flex layouts
    ".card", ".item", ".post",
    # Table rows
    "table tr",
    # Article tags
    "article",
    # Data attributes
    "[data-listing]", "[data-id]", "[data-property]",
    # Generic containers with listing-like classes
    ".col-listing", ".listing-wrapper", ".listing-box",
    ".for-sale-item", ".available-listing",
]

TITLE_PATTERNS = [
    ".listing-title", ".business-name", ".property-name",
    ".listing-name", "h2", "h3", ".title", ".name",
    "[class*='title']", "[class*='name']", "a[href*='listing']",
]

PRICE_PATTERNS = [
    ".price", ".asking-price", ".listing-price", ".business-price",
    "[class*='price']", "[class*='cost']", "[class*='asking']",
    "span:contains('$')", ".amount",
]

LOCATION_PATTERNS = [
    ".location", ".city", ".state", ".address",
    "[class*='location']", "[class*='city']", "[class*='state']",
    ".listing-location", ".business-location",
]

# Common listings URL path patterns
LISTINGS_PATHS = [
    "/listings", "/businesses-for-sale", "/business-listings",
    "/buy-a-business", "/available-businesses", "/for-sale",
    "/search", "/results", "/buy", "/properties",
]

# ── HTTP fetch ─────────────────────────────────────────────────────────────────

def fetch(url, timeout=15):
    try:
        if HAS_CFFI:
            r = cffi_requests.get(url, impersonate="chrome120", timeout=timeout)
        else:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        return r.text, None
    except Exception as e:
        return None, str(e)


def parse(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c.extract()
    return soup

# ── Pattern detection — replaces Claude ───────────────────────────────────────

def detect_container(soup):
    """Try each container pattern, return the one with the most matches."""
    best_sel, best_count = None, 0
    for sel in CONTAINER_PATTERNS:
        try:
            matches = soup.select(sel)
            # Filter out tiny elements (nav items, etc.) — real listings have content
            real = [m for m in matches if len(m.get_text(strip=True)) > 30]
            if len(real) > best_count:
                best_count = len(real)
                best_sel = sel
        except Exception:
            continue
    return best_sel, best_count


def detect_field(container, patterns):
    """Try each field pattern within a container element."""
    for sel in patterns:
        try:
            found = container.select_one(sel)
            if found:
                text = found.get_text(strip=True)
                if text and len(text) > 1:
                    return sel, text
        except Exception:
            continue
    return None, None


def extract_listings(soup, container_sel):
    """Extract listings using detected selectors."""
    containers = soup.select(container_sel)
    real = [c for c in containers if len(c.get_text(strip=True)) > 30][:20]

    if not real:
        return [], {}

    # Detect field selectors from first container
    title_sel, _ = detect_field(real[0], TITLE_PATTERNS)
    price_sel, _ = detect_field(real[0], PRICE_PATTERNS)
    loc_sel, _   = detect_field(real[0], LOCATION_PATTERNS)

    listings = []
    for el in real:
        rec = {}
        if title_sel:
            t = el.select_one(title_sel)
            rec["title"] = t.get_text(strip=True)[:200] if t else None
        if price_sel:
            p = el.select_one(price_sel)
            rec["price"] = p.get_text(strip=True)[:100] if p else None
        if loc_sel:
            l = el.select_one(loc_sel)
            rec["location"] = l.get_text(strip=True)[:100] if l else None
        # Always grab first link as detail URL
        a = el.select_one("a[href]")
        rec["url"] = a["href"] if a else None
        if any(rec.values()):
            listings.append(rec)

    field_sels = {}
    if title_sel: field_sels["title"] = title_sel
    if price_sel: field_sels["price"] = price_sel
    if loc_sel:   field_sels["location"] = loc_sel

    return listings, field_sels


def find_listings_url(base_url):
    """Try common listing URL patterns if the given URL has no listings."""
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for path in LISTINGS_PATHS:
        url = base + path
        try:
            r = requests.head(url, headers={"User-Agent": UA}, timeout=8, allow_redirects=True)
            if r.status_code == 200:
                return url
        except:
            continue
    return None


def is_js_rendered(soup, container_sel):
    """Guess if page needs JS rendering."""
    containers = soup.select(container_sel) if container_sel else []
    body_text = soup.get_text(strip=True)
    # Signs of JS-only rendering
    if len(body_text) < 500:
        return True
    if "enable javascript" in body_text.lower():
        return True
    if "loading..." in body_text.lower() and not containers:
        return True
    return False

# ── Save config ────────────────────────────────────────────────────────────────

def save_config(url, broker_name, container_sel, field_sels, count, confidence):
    os.makedirs("data/scraper_configs", exist_ok=True)
    domain = urlparse(url).netloc.replace("www.", "")
    safe = re.sub(r"[^a-zA-Z0-9.-]", "_", domain)
    path = f"data/scraper_configs/{safe}.json"
    config = {
        "broker_name":        broker_name,
        "source_url":         url,
        "container_selector": container_sel,
        "fields":             field_sels,
        "listing_count":      count,
        "confidence":         confidence,
        "_meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "generator":    "dealledger-scraper-v4-no-api"
        }
    }
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
    return path

# ── Main scraper logic ─────────────────────────────────────────────────────────

def scrape_url(url, verbose=True):
    domain = urlparse(url).netloc.lower().replace("www.", "")
    broker_name = KNOWN_DOMAINS.get(domain, domain.split(".")[0])
    result = {"url": url, "success": False, "count": 0}

    def log(msg):
        if verbose:
            print(msg)

    log(f"\n{'='*55}")
    log(f"🔍 {url}")
    log(f"{'='*55}")

    # 1. Specialized scraper for known brokers
    if domain in KNOWN_DOMAINS and HAS_SPECIALIZED:
        log(f"   → Known broker: {broker_name}, using specialized scraper")
        try:
            broker = {"account": "v4-run", "name": broker_name, "url": url}
            listings = scrape_specialized_broker(broker, verbose=verbose)
            if listings:
                result["success"] = True
                result["count"] = len(listings)
                log(f"   ✅ {len(listings)} listings via specialized scraper")
                return result
        except Exception as e:
            log(f"   ⚠️  Specialized scraper error: {e}")

    # 2. Fetch the page
    html, err = fetch(url)
    if not html:
        log(f"   ❌ Fetch failed: {err}")
        result["error"] = err
        return result

    soup = parse(html)

    # 3. Detect container
    container_sel, count = detect_container(soup)

    # 4. If no containers found, try finding the listings URL
    if not container_sel or count < 2:
        log(f"   ℹ️  No containers at {url}, trying common listing paths...")
        alt_url = find_listings_url(url)
        if alt_url and alt_url != url:
            log(f"   → Trying: {alt_url}")
            html2, err2 = fetch(alt_url)
            if html2:
                soup = parse(html2)
                container_sel, count = detect_container(soup)
                url = alt_url  # use the better URL

    if not container_sel or count < 2:
        # Check if JS-rendered
        if is_js_rendered(soup, container_sel):
            log(f"   ⚠️  Likely JS-rendered — needs Playwright")
            result["error"] = "js_rendered"
        else:
            log(f"   ❌ No listing containers found")
            result["error"] = "no_containers"
        return result

    # 5. Extract listings
    listings, field_sels = extract_listings(soup, container_sel)

    has_title = sum(1 for l in listings if l.get("title"))
    has_price = sum(1 for l in listings if l.get("price"))
    quality = (has_title / len(listings) * 100) if listings else 0

    log(f"   → Container: {container_sel} ({count} found)")
    log(f"   → Fields: {list(field_sels.keys())}")
    log(f"   → Quality: {has_title}/{len(listings)} titles, {has_price}/{len(listings)} prices")

    if listings and quality >= 40:
        confidence = "high" if quality >= 70 else "medium"
        path = save_config(url, broker_name, container_sel, field_sels, count, confidence)
        result["success"] = True
        result["count"] = count
        result["config"] = path
        result["listings"] = listings
        log(f"   ✅ Saved config: {path} [{confidence}]")
        if listings:
            log(f"   → Sample: {json.dumps(listings[0])[:120]}")
    else:
        log(f"   ❌ Quality too low ({quality:.0f}%) — skipping")
        result["error"] = "low_quality"

    return result

# ── Batch runner ───────────────────────────────────────────────────────────────

def run_batch(csv_path, workers=5):
    urls = []
    with open(csv_path) as f:
        first = f.read(500)

    if ',' in first.split('\n')[0]:
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get('listings_url') or row.get('url') or
                       row.get('URL') or row.get('website') or
                       list(row.values())[0])
                if url and str(url).strip().startswith('http'):
                    urls.append(url.strip())
    else:
        with open(csv_path) as f:
            urls = [l.strip() for l in f if l.strip().startswith('http')]

    total = len(urls)
    print(f"\n🚀 Batch: {total} URLs, {workers} workers")
    print(f"   Est. time: ~{total * 3 // workers} min  (no API calls = ~3s/URL)\n")

    results = []
    completed = 0
    success = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(scrape_url, url, False): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            try:
                result = future.result()
                if result["success"]:
                    success += 1
                    print(f"✅ [{completed}/{total}] {url[:60]} → {result.get('count',0)} listings")
                else:
                    err = result.get('error','unknown')
                    print(f"❌ [{completed}/{total}] {url[:60]} ({err})")
            except Exception as e:
                print(f"💥 [{completed}/{total}] {url[:60]} ERROR: {e}")
                result = {"url": url, "success": False, "error": str(e)}
            results.append(result)

            if completed % 50 == 0:
                print(f"\n📊 {completed}/{total} | {success} successful ({round(success/completed*100)}%)\n")

    print(f"\n{'='*55}")
    print(f"COMPLETE: {success}/{total} ({round(success/total*100)}%)")
    print(f"Configs: data/scraper_configs/")

    # Error breakdown
    errors = {}
    for r in results:
        if not r.get("success"):
            e = r.get("error", "unknown")
            errors[e] = errors.get(e, 0) + 1
    if errors:
        print(f"Error breakdown: {json.dumps(errors)}")

    # Save all listings to data/listings/
    os.makedirs("data/listings", exist_ok=True)
    all_listings = []
    for r in results:
        if r.get("success") and r.get("listings"):
            for l in r["listings"]:
                l["broker_name"] = l.get("broker_name") or r.get("url","")
                l["source_url"] = l.get("source_url") or l.get("url") or r.get("url","")
                all_listings.append(l)
    if all_listings:
        today = datetime.now().strftime('%Y%m%d')
        listings_path = f"data/listings/v4_batch_{today}.json"
        with open(listings_path, "w") as f:
            json.dump(all_listings, f, default=str)
        print(f"💾 Saved {len(all_listings)} listings → {listings_path}")

    # Save all listings to data/listings/
    os.makedirs("data/listings", exist_ok=True)
    all_listings = []
    for r in results:
        if r.get("success") and r.get("listings"):
            for l in r["listings"]:
                l["broker_name"] = l.get("broker_name") or r.get("url","")
                l["source_url"] = l.get("source_url") or l.get("url") or r.get("url","")
                all_listings.append(l)
    if all_listings:
        today = datetime.now().strftime('%Y%m%d')
        listings_path = f"data/listings/v4_batch_{today}.json"
        with open(listings_path, "w") as f:
            json.dump(all_listings, f, default=str)
        print(f"💾 Saved {len(all_listings)} listings → {listings_path}")

    os.makedirs("data", exist_ok=True)
    summary_path = f"data/batch_summary_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(summary_path, "w") as f:
        json.dump({"total": total, "success": success,
                   "rate": round(success/total*100),
                   "errors": errors, "results": results}, f, indent=2)
    print(f"Summary: {summary_path}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DealLedger Scraper v4 — No API")
    parser.add_argument("url", nargs="?", help="Single URL to scrape")
    parser.add_argument("--batch", help="CSV or URL list file")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers (default 5)")
    args = parser.parse_args()

    if args.batch:
        run_batch(args.batch, workers=args.workers)
    elif args.url:
        scrape_url(args.url, verbose=True)
    else:
        parser.print_help()
