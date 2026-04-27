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

import argparse, csv, hashlib, json, os, re, sys, time
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
        rows = [{
            "canonical_id": l.get("canonical_id"),
            "domain": domain,
            "source_url": url,
            "title": l.get("title"),
            "price": l.get("price"),
            "location": l.get("location"),
            "state": l.get("state"),
            "listing_url": l.get("url"),
            "raw_data": json.dumps(l),
            "method": method,
            "scraped_at": datetime.now(timezone.utc).isoformat()
        } for l in listings if l.get("canonical_id")]
        sb.table("broker_listings").upsert(rows, on_conflict="canonical_id").execute()
    except Exception as e:
        memory_append("failures", f"- **{domain}** save_listings error: {str(e)[:100]}")

def _extract_items(data):
    if isinstance(data, list): return data
    if isinstance(data, dict):
        for key in ["items","results","data","listings","businesses","records","posts","products"]:
            if isinstance(data.get(key), list): return data[key]
    return []

def _normalize_api_item(item):
    if not isinstance(item, dict): return {}
    def _val(v):
        if isinstance(v, dict): return v.get("rendered") or v.get("name") or v.get("value") or str(v)
        return v
    title = _val(item.get("title") or item.get("name") or item.get("business_name") or item.get("header"))
    price = _val(item.get("price") or item.get("asking_price") or item.get("askingPrice") or item.get("listPrice"))
    location = _val(item.get("location") or item.get("city") or item.get("address") or item.get("state") or item.get("region"))
    url = _val(item.get("url") or item.get("link") or item.get("permalink") or item.get("slug") or item.get("urlStub"))
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

def extract_api_paginated(base_url, endpoint, page_size=100, max_pages=50, discovery=None):
    """
    Pagination-strategy-aware extractor.
    Uses discovery.pagination if available, otherwise auto-detects on first call.
    Strategies: page_param, offset, next_url
    """
    listings = []
    url_base = base_url.rstrip("/") + endpoint

    # Determine pagination strategy from discovery config
    pagination_raw = (discovery or {}).get("pagination") or {}
    # pagination may be a string (from Supabase) or a dict
    if isinstance(pagination_raw, str):
        pagination = {"type": pagination_raw}
    else:
        pagination = pagination_raw
    strategy   = pagination.get("type")       # "page_param" | "offset" | "next_url"
    p_param    = pagination.get("param", "page")
    s_param    = pagination.get("size_param", "per_page")

    # Auto-detect strategy if not recorded
    if not strategy:
        sep = "&" if "?" in url_base else "?"
        probe_url = f"{url_base}{sep}{s_param}={page_size}&{p_param}=1"
        data, err = fetch_json_url(probe_url)
        if err or data is None: return []
        items = _extract_items(data)
        if not items: return []
        # Check if offset works better
        offset_url = f"{url_base}{sep}limit={page_size}&offset=0"
        data2, err2 = fetch_json_url(offset_url)
        if not err2 and data2 and len(_extract_items(data2)) == len(items):
            strategy = "offset"
        else:
            strategy = "page_param"
        listings.extend([_normalize_api_item(i) for i in items])
        if len(items) < page_size: return listings
        start_page = 2
    else:
        start_page = 1

    if strategy == "next_url":
        # Follow next link from response
        sep = "&" if "?" in url_base else "?"
        next_url = f"{url_base}{sep}{s_param}={page_size}"
        pages = 0
        while next_url and pages < max_pages:
            data, err = fetch_json_url(next_url)
            if err or data is None: break
            items = _extract_items(data)
            if not items: break
            listings.extend([_normalize_api_item(i) for i in items])
            # Look for next link in response
            next_url = None
            if isinstance(data, dict):
                next_url = (data.get("next") or data.get("next_page") or
                            data.get("nextPage") or data.get("_links", {}).get("next", {}).get("href"))
            pages += 1
            time.sleep(REQUEST_DELAY)

    elif strategy == "offset":
        offset = len(listings)
        for _ in range(max_pages):
            sep = "&" if "?" in url_base else "?"
            url = f"{url_base}{sep}limit={page_size}&offset={offset}"
            data, err = fetch_json_url(url)
            if err or data is None: break
            items = _extract_items(data)
            if not items: break
            listings.extend([_normalize_api_item(i) for i in items])
            if len(items) < page_size: break
            offset += len(items)
            time.sleep(REQUEST_DELAY)

    else:  # page_param (default)
        for page in range(start_page, max_pages + 1):
            sep = "&" if "?" in url_base else "?"
            url = f"{url_base}{sep}{s_param}={page_size}&{p_param}={page}"
            data, err = fetch_json_url(url)
            if err or data is None: break
            items = _extract_items(data)
            if not items: break
            listings.extend([_normalize_api_item(i) for i in items])
            if len(items) < page_size: break
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

def make_canonical_id(domain, title, price, location, url):
    """Stable dedup key: sha1 of domain + normalized fields"""
    # Normalize URL path only (strip tracking params)
    url_path = ""
    if url:
        try:
            p = urlparse(str(url))
            # Keep path + query but strip utm/tracking params
            clean_params = "&".join(
                f"{k}={v}" for part in (p.query or "").split("&")
                for k, v in [part.split("=", 1)] if part and not k.startswith(("utm_", "gclid", "fbclid", "_ga"))
            ) if p.query else ""
            url_path = p.path + (f"?{clean_params}" if clean_params else "")
        except: url_path = str(url)[:200]

    raw = "|".join([
        domain.lower().strip(),
        (title or "").lower().strip()[:100],
        re.sub(r"[^0-9]", "", str(price or ""))[:20],
        (location or "").lower().strip()[:50],
        url_path.lower()[:200],
    ])
    return hashlib.sha1(raw.encode()).hexdigest()


def _nj_find_best_list(obj):
    """find_best_list for network_json — mirrors pw_network_discovery scoring."""
    HINT = {
        "title","name","asking","asking_price","askingPrice","price","listPrice",
        "city","state","location","address","url","link","slug","permalink",
        "id","listingId","listing_id","business","business_name"
    }
    CONTAINERS = ["items","results","data","listings","businesses","records","posts","products"]
    best, best_score = [], 0
    def score_list(lst):
        if not lst or not isinstance(lst, list) or not isinstance(lst[0], dict): return 0
        keys = set()
        for it in lst[:5]: keys |= set(it.keys())
        return len(keys & HINT) * 10 + min(len(lst), 200)
    def walk(node):
        nonlocal best, best_score
        if isinstance(node, dict):
            for k in CONTAINERS:
                s = score_list(node.get(k))
                if s > best_score: best_score, best = s, node[k]
            for v in node.values(): walk(v)
        elif isinstance(node, list):
            s = score_list(node)
            if s > best_score: best_score, best = s, node
            for it in node[:20]: walk(it)
    walk(obj)
    return best, best_score

def _nj_get_next_url(payload):
    if isinstance(payload, dict):
        for k in ("next","next_url","nextUrl","nextPage","next_page"):
            v = payload.get(k)
            if isinstance(v, str) and v.startswith("http"): return v
        links = payload.get("links")
        if isinstance(links, dict):
            v = links.get("next")
            if isinstance(v, str) and v.startswith("http"): return v
    return None

def _nj_find_cursor(payload):
    if isinstance(payload, dict):
        for k in ("next_cursor","nextCursor","cursor","after","endCursor","end_cursor"):
            v = payload.get(k)
            if isinstance(v, (str, int)) and str(v): return str(v)
        def walk(d, depth=0):
            if depth > 4: return None
            if isinstance(d, dict):
                if "pageInfo" in d and isinstance(d["pageInfo"], dict):
                    pi = d["pageInfo"]
                    ec = pi.get("endCursor") or pi.get("end_cursor")
                    if ec: return str(ec)
                for v in d.values():
                    got = walk(v, depth+1)
                    if got: return got
            elif isinstance(d, list):
                for it in d[:10]:
                    got = walk(it, depth+1)
                    if got: return got
            return None
        return walk(payload.get("data") or payload)
    return None

def _nj_set_param(url, key, value):
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    parts = urlsplit(url)
    q = dict(parse_qsl(parts.query, keep_blank_values=True))
    q[key] = str(value)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(q, doseq=True), parts.fragment))

def _nj_get_param(url, key):
    from urllib.parse import urlsplit, parse_qsl
    return dict(parse_qsl(urlsplit(url).query, keep_blank_values=True)).get(key)

def extract_network_json(discovery, base_url, max_pages=50, verbose=False):
    """
    Daily executor for network_json method.
    Uses endpoint_url + pagination stored by Playwright discovery.
    Returns (listings, error_or_None).
    """
    from urllib.parse import urlsplit, parse_qsl, urlencode, urljoin

    # Get plan — stored directly in discovery row or under "config" JSONB
    plan = discovery
    if discovery.get("config"):
        try: plan = json.loads(discovery["config"]) if isinstance(discovery["config"], str) else discovery["config"]
        except: pass

    endpoint_url = plan.get("endpoint_url") or plan.get("endpoint")
    if not endpoint_url:
        return [], "missing_endpoint_url"

    # Make absolute
    if not endpoint_url.startswith("http"):
        endpoint_url = urljoin(base_url.rstrip("/") + "/", endpoint_url)

    req_headers = (plan.get("request", {}) or {}).get("headers", {}) or plan.get("request_headers") or {}
    headers = {k: v for k, v in req_headers.items()
               if k.lower() in {"accept","authorization","x-api-key","cookie","referer"}}
    pagination = plan.get("pagination_guess") or plan.get("pagination") or "unknown"
    page_size = int(discovery.get("page_size") or 50)

    def log(msg):
        if verbose: print(msg)

    def fetch_page(url):
        data, err = fetch_json_url(url, extra_headers=headers)
        return data, err

    listings_out = []
    seen_hashes = set()
    url = endpoint_url
    page = 0

    PAGE_KEYS   = ["page","p","pg"]
    OFFSET_KEYS = ["offset","skip","start"]
    CURSOR_KEYS = ["cursor","after","pageCursor","page_cursor","starting_after"]

    while page < max_pages:
        page += 1
        data, err = fetch_page(url)
        if err or data is None:
            if page == 1: return listings_out, err or "fetch_failed"
            break

        best_list, score = _nj_find_best_list(data)
        if score < 30 or not best_list:
            log(f"   ⚠️  Low score ({score}) on page {page}")
            if page == 1: return listings_out, "not_listings_json"
            break

        # Normalize + dedupe
        for item in best_list:
            rec = _normalize_api_item(item)
            key = "|".join([rec.get("url") or "", rec.get("title") or "",
                            rec.get("price") or "", rec.get("location") or ""])
            h = hash(key)
            if h in seen_hashes: continue
            seen_hashes.add(h)
            listings_out.append(rec)

        if pagination in ("page_param","offset") and len(best_list) < page_size:
            break

        # Compute next URL
        next_url = None
        q = dict(parse_qsl(urlsplit(url).query, keep_blank_values=True))

        if pagination == "next_url":
            next_url = _nj_get_next_url(data)

        elif pagination == "cursor":
            cursor = _nj_find_cursor(data)
            if cursor:
                present = next((ck for ck in CURSOR_KEYS if ck in q), "cursor")
                next_url = _nj_set_param(url, present, cursor)
            else:
                next_url = _nj_get_next_url(data)

        elif pagination == "page_param":
            present = next((pk for pk in PAGE_KEYS if pk in q), None)
            if present:
                cur = int(q.get(present, 1))
                next_url = _nj_set_param(url, present, cur + 1)
            else:
                next_url = _nj_set_param(url, "page", page + 1)
            # Preserve size param
            for sk in ("per_page","limit","page_size","pagesize"):
                if sk in q: next_url = _nj_set_param(next_url, sk, q[sk]); break

        elif pagination == "offset":
            present = next((ok for ok in OFFSET_KEYS if ok in q), None)
            cur_offset = int(q.get(present, 0)) if present else 0
            next_url = _nj_set_param(url, present or "offset", cur_offset + page_size)
            if "limit" not in q and "per_page" not in q:
                next_url = _nj_set_param(next_url, "limit", page_size)

        else:
            break  # unknown: single shot

        if not next_url or next_url == url: break
        url = next_url
        time.sleep(REQUEST_DELAY)

    return listings_out, None


def normalize_listings(listings, base_url):
    domain = urlparse(base_url).netloc.lower().replace("www.", "")
    seen = set()
    out = []
    for l in listings:
        raw_url = l.get("url")
        if raw_url:
            raw_url = str(raw_url).strip()
            if raw_url.startswith("//"):
                raw_url = urlparse(base_url).scheme + ":" + raw_url
            elif raw_url.startswith("/") or not raw_url.startswith("http"):
                raw_url = base_url.rstrip("/") + "/" + raw_url.lstrip("/")
            l["url"] = raw_url
        l["state"] = normalize_state(l.get("location"))
        cid = make_canonical_id(domain, l.get("title"), l.get("price"), l.get("location"), l.get("url"))
        l["canonical_id"] = cid
        if cid in seen: continue
        seen.add(cid)
        if any(v for v in l.values() if v is not None):
            out.append(l)
    return out


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
            listings = extract_api_paginated(base_url, endpoint, page_size=discovery.get("page_size") or 100, discovery=discovery)
        elif method == "network_json":
            listings, nj_err = extract_network_json(discovery, base_url, verbose=verbose)
            if nj_err and not listings:
                result["error"] = nj_err; return result
        elif method == "shopify_api":
            listings = extract_shopify(base_url)
        elif method == "squarespace_json":
            listings = extract_squarespace(url)
        elif method in ("ghost_api", "drupal_api", "rest_api"):
            listings = extract_api_paginated(base_url, endpoint, page_size=discovery.get("page_size") or 20, discovery=discovery)
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
    id           BIGSERIAL PRIMARY KEY,
    canonical_id TEXT NOT NULL,          -- sha1 dedup key
    domain       TEXT NOT NULL,
    source_url   TEXT,
    title        TEXT,
    price        TEXT,
    location     TEXT,
    state        TEXT,
    listing_url  TEXT,
    raw_data     JSONB,
    method       TEXT,
    scraped_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (canonical_id)
);
CREATE INDEX IF NOT EXISTS broker_listings_domain_idx    ON broker_listings(domain);
CREATE INDEX IF NOT EXISTS broker_listings_state_idx     ON broker_listings(state);
CREATE INDEX IF NOT EXISTS broker_listings_scraped_idx   ON broker_listings(scraped_at);
CREATE INDEX IF NOT EXISTS broker_listings_canonical_idx ON broker_listings(canonical_id);

-- If migrating existing table, add canonical_id column:
-- ALTER TABLE broker_listings ADD COLUMN IF NOT EXISTS canonical_id TEXT;
-- CREATE UNIQUE INDEX IF NOT EXISTS broker_listings_canonical_idx ON broker_listings(canonical_id);
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
