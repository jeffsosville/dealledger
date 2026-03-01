#!/usr/bin/env python3
"""
DealLedger Scraper v5 — Discovery-Driven Daily Scraper
========================================================
Uses broker_discovery table (built by discovery.py) to route each broker
to the right extraction method. Near-zero AI cost on daily runs.

IMPORTANT: Run discovery.py first to populate broker_discovery table.

Run:
  python scraper_agent_v5.py --batch data/brokers_clean.csv --workers 8
  python scraper_agent_v5.py --url https://example.com
  python scraper_agent_v5.py --batch data/brokers_clean.csv --failed-only
  python scraper_agent_v5.py --print-sql
"""

import argparse, csv, json, os, re, sys, time
from datetime import datetime, timezone
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup, Comment

try:
    from curl_cffi import requests as cffi_requests
    HAS_CFFI = True
except ImportError:
    HAS_CFFI = False

try:
    from supabase import create_client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
HEADERS = {"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,application/json", "Accept-Language": "en-US,en;q=0.9"}
REQUEST_DELAY = 1.0
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 15]
MEMORY_DIR = "data/memory"

STATE_ABBREV = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
    "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
    "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA",
    "kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD",
    "massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
    "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV",
    "new hampshire":"NH","new jersey":"NJ","new mexico":"NM","new york":"NY",
    "north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK",
    "oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC",
    "south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT",
    "virginia":"VA","washington":"WA","west virginia":"WV","wisconsin":"WI",
    "wyoming":"WY","district of columbia":"DC",
}
VALID_ABBREVS = set(STATE_ABBREV.values())

def normalize_state(raw):
    if not raw: return None
    parts = [p.strip() for p in str(raw).split(",")]
    for candidate in reversed(parts):
        c = candidate.strip().lower()
        if c in STATE_ABBREV: return STATE_ABBREV[c]
        cu = candidate.strip().upper()
        if cu in VALID_ABBREVS: return cu
    memory_append("state_fix", f"- Unknown: `{raw}`")
    return None

MEMORY_FILES = {
    "patterns": "broker_patterns.md", "failures": "failure_log.md",
    "state_fix": "state_fixes.md", "haiku": "haiku_feedback.md",
}

def ensure_memory():
    os.makedirs(MEMORY_DIR, exist_ok=True)
    for key, fname in MEMORY_FILES.items():
        path = os.path.join(MEMORY_DIR, fname)
        if not os.path.exists(path):
            with open(path, "w") as f:
                f.write(f"# {fname.replace('_',' ').replace('.md','').title()}\n_Auto-updated_\n\n")

def memory_append(key, entry):
    ensure_memory()
    path = os.path.join(MEMORY_DIR, MEMORY_FILES[key])
    ts = datetime.now().strftime("%Y-%m-%d")
    with open(path, "a") as f:
        f.write(f"\n## {ts}\n{entry}\n")

def fetch(url, timeout=15, extra_headers=None, method="get", json_body=None):
    h = {**HEADERS, **(extra_headers or {})}
    for attempt in range(MAX_RETRIES):
        try:
            if HAS_CFFI:
                r = cffi_requests.post(url, impersonate="chrome131", headers=h, json=json_body, timeout=timeout) if method == "post" else cffi_requests.get(url, impersonate="chrome131", headers=h, timeout=timeout)
            else:
                r = requests.post(url, headers=h, json=json_body, timeout=timeout, allow_redirects=True) if method == "post" else requests.get(url, headers=h, timeout=timeout, allow_redirects=True)
            if r.status_code == 429:
                time.sleep(int(r.headers.get("Retry-After", RETRY_BACKOFF[attempt]))); continue
            if r.status_code == 403: return None, "blocked_403"
            if r.status_code >= 500 and attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt]); continue
            r.raise_for_status()
            return r, None
        except Exception as e:
            if attempt < MAX_RETRIES - 1: time.sleep(RETRY_BACKOFF[attempt]); continue
            return None, str(e)
    return None, "max_retries"

def fetch_text(url, **kw):
    r, err = fetch(url, **kw)
    return (r.text, None) if r else (None, err)

def fetch_json_url(url, **kw):
    kw["extra_headers"] = {**kw.get("extra_headers", {}), "Accept": "application/json"}
    r, err = fetch(url, **kw)
    if not r: return None, err
    try: return r.json(), None
    except: return None, "json_parse_error"

def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script","style","noscript","iframe","svg"]): tag.decompose()
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)): c.extract()
    return soup

_sb = None
def get_supabase():
    global _sb
    if _sb is None and HAS_SUPABASE:
        url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
        if url and key: _sb = create_client(url, key)
    return _sb

def load_discovery(domain):
    sb = get_supabase()
    if not sb: return None
    try:
        res = sb.table("broker_discovery").select("*").eq("domain", domain).execute()
        return res.data[0] if res.data else None
    except: return None

def save_listings(domain, url, listings, method):
    sb = get_supabase()
    if not sb or not listings: return
    try:
        rows = [{"domain": domain, "source_url": url, "title": l.get("title"),
                 "price": l.get("price"), "location": l.get("location"),
                 "state": l.get("state"), "listing_url": l.get("url"),
                 "raw_data": json.dumps(l), "method": method,
                 "scraped_at": datetime.now(timezone.utc).isoformat()} for l in listings]
        sb.table("broker_listings").upsert(rows, on_conflict="domain,listing_url").execute()
    except: pass

def _extract_items(data):
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for key in ["items","results","data","listings","businesses","records","posts","products"]:
            if isinstance(data.get(key), list): return data[key]
    return []

def _normalize_api_item(item):
    if not isinstance(item, dict): return {}
    title = item.get("title") or item.get("name") or item.get("business_name") or item.get("header")
    price = item.get("price") or item.get("asking_price") or item.get("askingPrice") or item.get("listPrice")
    location = item.get("location") or item.get("city") or item.get("address") or item.get("state") or item.get("region")
    url = item.get("url") or item.get("link") or item.get("permalink") or item.get("slug") or item.get("urlStub")
    return {
        "title": str(title).strip()[:200] if title else None,
        "price": str(price).strip()[:100] if price else None,
        "location": str(location).strip()[:100] if location else None,
        "url": str(url).strip() if url else None,
    }

def _find_listing_list(data):
    best = []
    if isinstance(data, dict):
        for val in data.values():
            c = _find_listing_list(val)
            if len(c) > len(best): best = c
    elif isinstance(data, list):
        if data and isinstance(data[0], dict): return data
        for item in data:
            c = _find_listing_list(item)
            if len(c) > len(best): best = c
    return best

def _walk_rsc(node, out, data_keys):
    if isinstance(node, dict):
        if data_keys & set(node.keys()): out.append(node)
        for v in node.values():
            if isinstance(v, (dict, list)): _walk_rsc(v, out, data_keys)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, (dict, list)): _walk_rsc(item, out, data_keys)

def extract_api_paginated(base_url, endpoint, page_size=100, max_pages=50):
    listings = []
    page = 1
    url_base = base_url.rstrip("/") + endpoint
    while page <= max_pages:
        sep = "&" if "?" in url_base else "?"
        url = f"{url_base}{sep}per_page={page_size}&page={page}&limit={page_size}&offset={(page-1)*page_size}"
        data, err = fetch_json_url(url)
        if err or data is None: break
        items = _extract_items(data)
        if not items: break
        listings.extend([_normalize_api_item(i) for i in items])
        if len(items) < page_size: break
        page += 1
        time.sleep(REQUEST_DELAY)
    return listings

def extract_shopify(base_url):
    listings = []
    page = 1
    while True:
        data, err = fetch_json_url(f"{base_url}/products.json?limit=250&page={page}")
        if err or not data: break
        products = data.get("products", [])
        if not products: break
        for p in products:
            price = p["variants"][0].get("price") if p.get("variants") else None
            listings.append({"title": p.get("title"), "price": str(price) if price else None,
                             "location": None, "url": f"{base_url}/products/{p.get('handle')}"})
        if len(products) < 250: break
        page += 1
        time.sleep(REQUEST_DELAY)
    return listings

def extract_squarespace(url):
    data, err = fetch_json_url(url.rstrip("/") + "?format=json")
    if err or not data: return []
    items = data.get("items") or data.get("collection", {}).get("items") or []
    return [{"title": i.get("title"), "price": None, "location": None,
             "url": i.get("fullUrl") or i.get("urlId")} for i in items]

def extract_nextdata(html, base_url):
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not match: return []
    try:
        data = json.loads(match.group(1))
        page_props = data.get("props", {}).get("pageProps", {})
        best = []
        for val in page_props.values():
            if isinstance(val, list) and len(val) > len(best) and val and isinstance(val[0], dict):
                best = val
        return [_normalize_api_item(i) for i in best]
    except: return []

def extract_rsc(url):
    DATA_KEYS = {"data","items","results","listings","businesses","records","pageProps","initialData"}
    rsc_text, err = fetch_text(url, extra_headers={"RSC":"1","Next-Router-State-Tree":"","Accept":"text/x-component, */*"})
    if not rsc_text: return []
    objects = []
    for line in rsc_text.splitlines():
        m = re.match(r'^[0-9a-f]+:', line)
        if not m: continue
        try:
            parsed = json.loads(line[m.end():])
            _walk_rsc(parsed, objects, DATA_KEYS)
        except: continue
    best = []
    for obj in objects:
        for val in (obj.values() if isinstance(obj, dict) else []):
            if isinstance(val, list) and len(val) > len(best) and val and isinstance(val[0], dict):
                best = val
    return [_normalize_api_item(i) for i in best]

def extract_nuxt(html):
    soup = BeautifulSoup(html, "html.parser")
    tag = soup.find("script", id="__NUXT_DATA__")
    if tag and tag.string:
        try:
            items = _find_listing_list(json.loads(tag.string))
            if items: return [_normalize_api_item(i) for i in items]
        except: pass
    match = re.search(r'window\.__NUXT__\s*=\s*(.+?);\s*(?:</script>|$)', html, re.DOTALL)
    if match:
        try:
            items = _find_listing_list(json.loads(match.group(1)))
            return [_normalize_api_item(i) for i in items]
        except: pass
    return []

def extract_jsonld(html):
    soup = BeautifulSoup(html, "html.parser")
    listings = []
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string)
            items = data if isinstance(data, list) else [data]
            for item in items:
                t = str(item.get("@type","")).lower()
                if any(x in t for x in ["business","listing","product","offer","service","realestate"]):
                    addr = item.get("address", {})
                    loc = None
                    if isinstance(addr, dict):
                        loc = f"{addr.get('addressLocality','')}, {addr.get('addressRegion','')}".strip(", ") or None
                    listings.append({"title": item.get("name"),
                                     "price": item.get("offers",{}).get("price") if isinstance(item.get("offers"),dict) else None,
                                     "location": loc, "url": item.get("url")})
        except: continue
    return listings

def extract_data_attributes(html, container_attr, base_url):
    soup = parse_html(html)
    listings = []
    for el in soup.find_all(attrs={container_attr: True}):
        rec = {"title": el.get("data-title") or el.get("data-name") or el.get_text(strip=True)[:100],
               "price": el.get("data-price") or el.get("data-asking-price"),
               "location": el.get("data-location") or el.get("data-city") or el.get("data-state"),
               "url": el.get("data-url") or el.get("data-href")}
        if any(rec.values()): listings.append(rec)
    return listings

def extract_css(html, discovery, base_url):
    soup = parse_html(html)
    container_sel = discovery.get("container")
    if not container_sel: return []
    containers = [c for c in soup.select(container_sel) if len(c.get_text(strip=True)) > 30][:100]
    if not containers: return []
    title_sel, price_sel, loc_sel = discovery.get("title_sel"), discovery.get("price_sel"), discovery.get("location_sel")
    listings = []
    for el in containers:
        rec = {}
        if title_sel:
            t = el.select_one(title_sel)
            if t: rec["title"] = t.get_text(strip=True)[:200]
        if price_sel:
            p = el.select_one(price_sel)
            if p: rec["price"] = p.get_text(strip=True)[:100]
        if loc_sel:
            l = el.select_one(loc_sel)
            if l: rec["location"] = l.get_text(strip=True)[:100]
        a = el.select_one("a[href]")
        if a: rec["url"] = a.get("href")
        if any(rec.values()): listings.append(rec)
    return listings

def normalize_listings(listings, base_url):
    for l in listings:
        l["state"] = normalize_state(l.get("location"))
        if l.get("url") and str(l["url"]).startswith("/"):
            l["url"] = base_url.rstrip("/") + l["url"]
    return [l for l in listings if any(l.values())]

def scrape_url(url, verbose=True):
    domain = urlparse(url).netloc.lower().replace("www.", "")
    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    result = {"url": url, "domain": domain, "success": False, "count": 0}
    def log(msg):
        if verbose: print(msg)

    log(f"\n{'='*60}\n🔍 {url}")
    discovery = load_discovery(domain)
    if not discovery:
        log(f"   ⚠️  No discovery — run discovery.py first")
        result["error"] = "no_discovery"; return result

    method = discovery.get("method", "unknown")
    status = discovery.get("status", "unknown")
    log(f"   📋 {method} | {discovery.get('platform','?')}")

    if status in ("js_only", "auth_required", "dead", "failed"):
        log(f"   ⏭️  Skip — {status}")
        result["error"] = status; return result

    listings = []
    endpoint = discovery.get("endpoint", "")

    try:
        if method == "wordpress_api":
            listings = extract_api_paginated(base_url, endpoint, page_size=discovery.get("page_size") or 100)
        elif method == "shopify_api":
            listings = extract_shopify(base_url)
        elif method == "squarespace_json":
            listings = extract_squarespace(url)
        elif method in ("ghost_api", "drupal_api", "rest_api"):
            listings = extract_api_paginated(base_url, endpoint, page_size=discovery.get("page_size") or 20)
        elif method == "nextjs_nextdata":
            html, err = fetch_text(url)
            if html: listings = extract_nextdata(html, base_url)
        elif method == "nextjs_rsc":
            listings = extract_rsc(url)
        elif method in ("nuxt_blob", "vue_ssr"):
            html, err = fetch_text(url)
            if html: listings = extract_nuxt(html)
        elif method == "jsonld":
            html, err = fetch_text(url)
            if html: listings = extract_jsonld(html)
        elif method == "data_attributes":
            html, err = fetch_text(url)
            if html: listings = extract_data_attributes(html, discovery.get("container_attr","data-id"), base_url)
        elif method in ("css_rules", "haiku_css"):
            html, err = fetch_text(url)
            if html:
                listings = extract_css(html, discovery, base_url)
                if not listings:
                    memory_append("failures", f"- **{domain}**: {method} selectors broke")
                    result["error"] = "selectors_broken"; return result
                if method == "css_rules":
                    memory_append("patterns", f"- `{discovery.get('container')}` working on **{domain}** ({len(listings)})")
        elif method == "graphql":
            log(f"   ⚠️  GraphQL requires manual query — skipping")
            result["error"] = "graphql_manual"; return result
        else:
            log(f"   ❌ Unknown method: {method}")
            result["error"] = f"unknown_{method}"; return result
    except Exception as e:
        log(f"   ❌ Error: {e}")
        memory_append("failures", f"- **{domain}** ({method}): {str(e)[:100]}")
        result["error"] = str(e); return result

    listings = normalize_listings(listings, base_url)
    if not listings:
        log(f"   ❌ No listings extracted")
        result["error"] = "empty"; return result

    save_listings(domain, url, listings, method)
    os.makedirs("data/listings", exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9.-]", "_", domain)
    with open(f"data/listings/{safe}.jsonl", "w") as f:
        for l in listings: f.write(json.dumps(l) + "\n")

    log(f"   ✅ {len(listings)} listings [{method}]")
    if listings and verbose: log(f"   → {json.dumps(listings[0])[:120]}")
    result.update({"success": True, "count": len(listings), "method": method})
    return result

def load_urls(csv_path):
    urls = []
    with open(csv_path) as f: first = f.read(500)
    if "," in first.split("\n")[0]:
        with open(csv_path) as f:
            for row in csv.DictReader(f):
                url = row.get("listings_url") or row.get("url") or row.get("URL") or row.get("website") or list(row.values())[0]
                if url and str(url).strip().startswith("http"): urls.append(url.strip())
    else:
        with open(csv_path) as f: urls = [l.strip() for l in f if l.strip().startswith("http")]
    return urls

def run_batch(csv_path, workers=8, failed_only=False):
    urls = load_urls(csv_path)
    if failed_only:
        sb = get_supabase()
        if sb:
            try:
                scraped = sb.table("broker_listings").select("domain").execute()
                done = {r["domain"] for r in scraped.data}
                before = len(urls)
                urls = [u for u in urls if urlparse(u).netloc.lower().replace("www.","") not in done]
                print(f"   Skipping {before-len(urls)} already scraped, running {len(urls)}")
            except Exception as e: print(f"   ⚠️  Filter error: {e}")

    total = len(urls)
    print(f"\n🚀 Scraping {total} brokers | {workers} workers")
    print(f"   Supabase: {'OK' if HAS_SUPABASE else 'NO'} | curl_cffi: {'OK' if HAS_CFFI else 'NO'}\n")

    results, completed, success, method_counts, total_listings = [], 0, 0, {}, 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(scrape_url, url, False): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            try:
                result = future.result()
                if result["success"]:
                    success += 1
                    m = result.get("method","?")
                    method_counts[m] = method_counts.get(m,0) + 1
                    total_listings += result.get("count",0)
                    icon = "🌐" if "api" in m else "📦" if m in ("nextjs_nextdata","nextjs_rsc","nuxt_blob") else "🔧"
                    print(f"✅ [{completed}/{total}] {icon} {url[:52]} → {result['count']}")
                else:
                    print(f"❌ [{completed}/{total}] {url[:52]} ({result.get('error','?')})")
            except Exception as e:
                print(f"💥 [{completed}/{total}] {url[:52]} ERROR: {e}")
                result = {"url": url, "success": False, "error": str(e)}
            results.append(result)
            if completed % 100 == 0:
                print(f"\n📊 {completed}/{total} | {success} ok ({round(success/completed*100)}%) | {total_listings:,} listings\n")

    errors = {}
    for r in results:
        if not r.get("success"):
            e = r.get("error","unknown"); errors[e] = errors.get(e,0)+1

    print(f"\n{'='*60}")
    print(f"COMPLETE: {success}/{total} ({round(success/total*100) if total else 0}%)")
    print(f"Total listings: {total_listings:,}\nBy method:")
    for m, c in sorted(method_counts.items(), key=lambda x: -x[1]): print(f"  {m:<25} {c}")
    if errors: print(f"\nErrors: {json.dumps(errors)}")

    os.makedirs("data", exist_ok=True)
    out = f"data/scrape_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(out, "w") as f:
        json.dump({"total":total,"success":success,"total_listings":total_listings,"methods":method_counts,"errors":errors}, f, indent=2)
    print(f"\nSaved: {out}")
    return results

SUPABASE_SQL = """
-- Run AFTER discovery.py --print-sql
CREATE TABLE IF NOT EXISTS broker_listings (
    id          BIGSERIAL PRIMARY KEY,
    domain      TEXT NOT NULL,
    source_url  TEXT,
    title       TEXT,
    price       TEXT,
    location    TEXT,
    state       TEXT,
    listing_url TEXT,
    raw_data    JSONB,
    method      TEXT,
    scraped_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (domain, listing_url)
);
CREATE INDEX IF NOT EXISTS broker_listings_domain_idx  ON broker_listings(domain);
CREATE INDEX IF NOT EXISTS broker_listings_state_idx   ON broker_listings(state);
CREATE INDEX IF NOT EXISTS broker_listings_scraped_idx ON broker_listings(scraped_at);
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DealLedger Scraper v5")
    parser.add_argument("url", nargs="?")
    parser.add_argument("--batch")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--failed-only", action="store_true")
    parser.add_argument("--print-sql", action="store_true")
    args = parser.parse_args()
    if args.print_sql: print(SUPABASE_SQL); sys.exit(0)
    ensure_memory()
    if args.batch: run_batch(args.batch, workers=args.workers, failed_only=args.failed_only)
    elif args.url:
        result = scrape_url(args.url, verbose=True)
        print(f"\n{json.dumps({k:v for k,v in result.items() if k!='listings'},indent=2)}")
    else: parser.print_help()
