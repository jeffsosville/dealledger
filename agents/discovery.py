#!/usr/bin/env python3
"""
DealLedger Discovery Module
============================
Run ONCE per broker domain. Fingerprints the platform and finds the best
data source. Stores result in Supabase so daily scrapes never repeat discovery.

Priority order (highest to lowest):
  1. Platform API   — WordPress, Shopify, Squarespace, Wix, Ghost, Drupal
  2. JSON blobs     — __NEXT_DATA__, __NUXT__, __remixContext, __INITIAL_STATE__
  3. RSC payload    — Next.js App Router (no __NEXT_DATA__)
  4. GraphQL        — /graphql, /gql, /api/graphql
  5. JSON-LD        — <script type="application/ld+json">
  6. Data attrs     — data-id, data-listing-id, data-price, data-testid
  7. CSS selectors  — semantic HTML (v4 rule-based)
  8. Haiku fallback — last resort AI analysis
  9. Flag           — js_only, auth_required, dead, blocked

Run:
  python discovery.py --url https://example.com
  python discovery.py --batch data/brokers_clean.csv --workers 5
  python discovery.py --batch data/brokers_clean.csv --rediscover  # re-run all
  python discovery.py --print-sql  # print Supabase setup SQL
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
except ImportError:
    HAS_CFFI = False

try:
    import anthropic
    HAS_ANTHROPIC = True
except ImportError:
    HAS_ANTHROPIC = False

try:
    from supabase import create_client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False

# ── Config ─────────────────────────────────────────────────────────────────────

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36"
HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
HAIKU_MODEL = "claude-haiku-4-5-20251001"
REQUEST_DELAY = 1.0
MAX_RETRIES = 3
RETRY_BACKOFF = [2, 5, 15]

# Data-like keys for RSC/JSON blob detection
DATA_KEYS = {"data", "items", "results", "products", "listings", "records",
             "pageProps", "initialData", "props", "businesses", "brokerages"}

# Common API endpoint probes for unknown platforms
API_PROBES = [
    "/api/listings", "/api/businesses", "/api/properties", "/api/v1/listings",
    "/api/v1/businesses", "/api/v2/listings", "/listings.json", "/businesses.json",
    "/api/search", "/api/v1/search", "/api/posts", "/api/v1/posts",
]

GRAPHQL_PROBES = ["/graphql", "/gql", "/api/graphql", "/v1/graphql", "/__graphql"]

LISTINGS_PATHS = [
    "/listings", "/businesses-for-sale", "/business-listings",
    "/buy-a-business", "/available-businesses", "/for-sale",
    "/search", "/results", "/buy", "/properties", "/businesses",
]

# v4 CSS container patterns
CONTAINER_PATTERNS = [
    ".listing-item", ".listing-card", ".listing",
    ".business-listing", ".business-card", ".business-item",
    ".property-item", ".property-card", ".property-listing",
    ".result-item", ".search-result", ".result-card",
    ".card", ".item", ".post", "table tr", "article",
    "[data-listing]", "[data-id]", "[data-property]",
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
    "[class*='price']", "[class*='cost']", "[class*='asking']", ".amount",
]

LOCATION_PATTERNS = [
    ".location", ".city", ".state", ".address",
    "[class*='location']", "[class*='city']", "[class*='state']",
    ".listing-location", ".business-location",
]

HAIKU_PROMPT = """Analyze this broker website HTML and return CSS selectors for extracting business-for-sale listings.

Return ONLY a JSON object, no markdown:
{{
  "has_listings": true/false,
  "container": "CSS selector for repeating listing element",
  "title": "CSS selector for title (relative to container) or null",
  "price": "CSS selector for price (relative to container) or null",
  "location": "CSS selector for location (relative to container) or null",
  "link": "CSS selector for detail link (relative to container) or null",
  "confidence": "high/medium/low",
  "notes": "brief observation"
}}

URL: {url}
HTML:
{html}"""

# ── HTTP helpers ───────────────────────────────────────────────────────────────

def fetch(url, timeout=15, headers=None, method="get", json_body=None):
    """Fetch with retry, curl_cffi fallback, and rate limit handling."""
    h = {**HEADERS, **(headers or {})}
    for attempt in range(MAX_RETRIES):
        try:
            if HAS_CFFI:
                if method == "post":
                    r = cffi_requests.post(url, impersonate="chrome131",
                                           headers=h, json=json_body, timeout=timeout)
                else:
                    r = cffi_requests.get(url, impersonate="chrome131",
                                          headers=h, timeout=timeout)
            else:
                if method == "post":
                    r = requests.post(url, headers=h, json=json_body,
                                      timeout=timeout, allow_redirects=True)
                else:
                    r = requests.get(url, headers=h,
                                     timeout=timeout, allow_redirects=True)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", RETRY_BACKOFF[attempt]))
                time.sleep(wait)
                continue
            if r.status_code >= 500 and attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
                continue
            return r, None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF[attempt])
                continue
            return None, str(e)
    return None, "max_retries_exceeded"


def fetch_text(url, **kwargs):
    r, err = fetch(url, **kwargs)
    if r is None:
        return None, err
    try:
        r.raise_for_status()
        return r.text, None
    except Exception as e:
        return None, f"HTTP {r.status_code}"


def fetch_json(url, **kwargs):
    r, err = fetch(url, headers={"Accept": "application/json"}, **kwargs)
    if r is None:
        return None, err
    try:
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        return None, str(e)


def head_ok(url):
    """Quick check if URL returns 200."""
    try:
        r = requests.head(url, headers=HEADERS, timeout=8, allow_redirects=True)
        return r.status_code == 200
    except:
        return False


def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c.extract()
    return soup


def clean_for_ai(html, max_chars=12000):
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "style", "noscript", "iframe", "svg", "head"]):
        tag.decompose()
    body = soup.find("body")
    h = str(body) if body else str(soup)
    if len(h) > max_chars:
        h = h[:4000] + "\n...[TRUNCATED]...\n" + h[-8000:]
    return h

# ── Platform detection ─────────────────────────────────────────────────────────

def detect_platform(html, url):
    """Return platform string from HTML fingerprints."""
    text = html[:50000]  # only scan first 50k chars

    # WordPress
    if any(x in text for x in ["/wp-content/", "/wp-includes/", 'rel="https://api.w.org/"']):
        return "wordpress"

    # Shopify
    if any(x in text for x in ["cdn.shopify.com", "myshopify.com", "Shopify.theme"]):
        return "shopify"

    # Squarespace
    if any(x in text for x in ["static.squarespace.com", "squarespace.com/static"]):
        return "squarespace"

    # Wix
    if any(x in text for x in ["parastorage.com", "static.wixstatic.com", "wix.com/pages"]):
        return "wix"

    # Webflow
    if any(x in text for x in ["webflow.js", 'data-wf-site', 'data-wf-page']):
        return "webflow"

    # Ghost
    if 'content="Ghost"' in text or "/ghost/api/" in text:
        return "ghost"

    # Drupal
    if any(x in text for x in ['content="Drupal"', "/sites/default/files/"]):
        return "drupal"

    # Next.js (Pages Router)
    if '__NEXT_DATA__' in text:
        return "nextjs_pages"

    # Next.js (App Router / RSC) — no __NEXT_DATA__ but has _next/static
    if "_next/static" in text or "_next/chunk" in text:
        return "nextjs_app"

    # Nuxt
    if any(x in text for x in ["window.__NUXT__", "__NUXT_DATA__", "nuxt"]):
        return "nuxt"

    # Remix
    if "window.__remixContext" in text:
        return "remix"

    # Gatsby
    if "gatsby-page-data" in text or "___gatsby" in text:
        return "gatsby"

    # Vue SSR
    if "window.__INITIAL_STATE__" in text:
        return "vue_ssr"

    # SvelteKit
    if "data-sveltekit" in text:
        return "sveltekit"

    return "unknown"

# ── Platform-specific extractors ───────────────────────────────────────────────

def try_wordpress(base_url):
    """Try WordPress REST API. Returns (listings, endpoint, pagination) or None."""
    # Check wp-json root
    data, err = fetch_json(f"{base_url}/wp-json/")
    if err or not data:
        return None

    # Try to find listing-related custom post types
    namespaces = data.get("namespaces", [])
    routes = data.get("routes", {})

    # Common custom post types for broker sites
    post_type_candidates = [
        "listings", "business-listings", "businesses", "properties",
        "available-businesses", "for-sale", "franchise"
    ]

    for pt in post_type_candidates:
        url = f"{base_url}/wp-json/wp/v2/{pt}?per_page=10"
        items, err = fetch_json(url)
        if items and isinstance(items, list) and len(items) > 0:
            # Probe max page size
            max_size = probe_page_size(base_url + f"/wp-json/wp/v2/{pt}")
            return {
                "method": "wordpress_api",
                "endpoint": f"/wp-json/wp/v2/{pt}",
                "pagination": "page_param",
                "page_size": max_size,
                "sample_count": len(items),
            }

    # Fallback: standard posts
    items, err = fetch_json(f"{base_url}/wp-json/wp/v2/posts?per_page=10")
    if items and isinstance(items, list) and len(items) > 0:
        return {
            "method": "wordpress_api",
            "endpoint": "/wp-json/wp/v2/posts",
            "pagination": "page_param",
            "page_size": 100,
            "sample_count": len(items),
        }
    return None


def try_shopify(base_url):
    """Try Shopify product endpoints."""
    for path in ["/products.json?limit=250", "/collections/all/products.json?limit=250"]:
        data, err = fetch_json(base_url + path)
        if data and isinstance(data, dict):
            products = data.get("products", [])
            if products:
                return {
                    "method": "shopify_api",
                    "endpoint": path.split("?")[0],
                    "pagination": "page_param",
                    "page_size": 250,
                    "sample_count": len(products),
                }
    return None


def try_squarespace(url):
    """Append ?format=json to get Squarespace JSON."""
    json_url = url.rstrip("/") + "?format=json"
    data, err = fetch_json(json_url)
    if data and isinstance(data, dict):
        # Look for items/collection data
        items = (data.get("items") or data.get("collection", {}).get("items") or
                 data.get("pagination", {}).get("items") or [])
        if items:
            return {
                "method": "squarespace_json",
                "endpoint": "?format=json",
                "pagination": "page_param",
                "page_size": 20,
                "sample_count": len(items),
            }
    return None


def try_ghost(base_url):
    """Try Ghost content API — needs key but sometimes open."""
    # Ghost without key sometimes returns public content
    data, err = fetch_json(f"{base_url}/ghost/api/content/posts/?limit=10")
    if data and isinstance(data, dict) and data.get("posts"):
        return {
            "method": "ghost_api",
            "endpoint": "/ghost/api/content/posts/",
            "pagination": "page_param",
            "page_size": 100,
            "sample_count": len(data["posts"]),
        }
    return None


def try_drupal(base_url):
    """Try Drupal JSON:API."""
    data, err = fetch_json(f"{base_url}/jsonapi/node/article?page[limit]=10")
    if data and isinstance(data, dict) and data.get("data"):
        return {
            "method": "drupal_api",
            "endpoint": "/jsonapi/node/article",
            "pagination": "page_param",
            "page_size": 50,
            "sample_count": len(data["data"]),
        }
    return None


def try_nextjs_pages(html):
    """Extract __NEXT_DATA__ blob."""
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html, re.DOTALL
    )
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
        page_props = data.get("props", {}).get("pageProps", {})
        # Look for listing-like arrays in pageProps
        for key, val in page_props.items():
            if isinstance(val, list) and len(val) > 2:
                if isinstance(val[0], dict):
                    return {
                        "method": "nextjs_nextdata",
                        "endpoint": None,
                        "pagination": "none",
                        "page_size": len(val),
                        "sample_count": len(val),
                        "data_key": key,
                    }
        # Return even if no obvious list — pageProps has something
        if page_props:
            return {
                "method": "nextjs_nextdata",
                "endpoint": None,
                "pagination": "none",
                "page_size": 0,
                "sample_count": 0,
            }
    except Exception:
        pass
    return None


def try_rsc(url, base_url):
    """Try Next.js App Router RSC payload."""
    rsc_text, err = fetch_text(url, headers={
        "RSC": "1",
        "Next-Router-State-Tree": "",
        "Accept": "text/x-component, */*",
    })
    if not rsc_text:
        return None

    # Check if it's actually RSC format
    if not re.search(r'^[0-9a-f]+:', rsc_text, re.MULTILINE):
        return None

    objects = parse_rsc(rsc_text)
    listing_data = find_listings_in_objects(objects)

    if listing_data:
        return {
            "method": "nextjs_rsc",
            "endpoint": None,
            "pagination": "rsc",
            "page_size": len(listing_data),
            "sample_count": len(listing_data),
        }
    return None


def parse_rsc(rsc_text):
    """Parse RSC line protocol and extract data objects."""
    results = []
    for line in rsc_text.splitlines():
        m = re.match(r'^[0-9a-f]+:', line)
        if not m:
            continue
        try:
            parsed = json.loads(line[m.end():])
        except (json.JSONDecodeError, TypeError):
            continue
        if isinstance(parsed, dict):
            _walk_rsc(parsed, results)
        elif isinstance(parsed, list):
            _walk_rsc(parsed, results)
    return results


def _walk_rsc(node, out):
    if isinstance(node, dict):
        if DATA_KEYS & set(node.keys()):
            out.append(node)
        for v in node.values():
            if isinstance(v, (dict, list)):
                _walk_rsc(v, out)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, (dict, list)):
                _walk_rsc(item, out)


def try_nuxt(html):
    """Extract Nuxt __NUXT__ blob."""
    # Try __NUXT_DATA__ script tag first
    tag = BeautifulSoup(html, "html.parser").find("script", id="__NUXT_DATA__")
    if tag and tag.string:
        try:
            data = json.loads(tag.string)
            if isinstance(data, (list, dict)):
                return {
                    "method": "nuxt_blob",
                    "endpoint": None,
                    "pagination": "none",
                    "sample_count": 0,
                }
        except:
            pass

    # Try window.__NUXT__ assignment
    match = re.search(r'window\.__NUXT__\s*=\s*(.+?);\s*(?:</script>|$)',
                      html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            return {
                "method": "nuxt_blob",
                "endpoint": None,
                "pagination": "none",
                "sample_count": 0,
            }
        except:
            pass
    return None


def try_json_ld(html):
    """Check for JSON-LD structured data with listing-like content."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string)
            items = data if isinstance(data, list) else [data]
            # Look for business/listing types
            for item in items:
                t = item.get("@type", "")
                if any(x in str(t).lower() for x in
                       ["business", "listing", "product", "offer", "realestate",
                        "localbusiness", "service"]):
                    return {
                        "method": "jsonld",
                        "endpoint": None,
                        "pagination": "none",
                        "sample_count": len(items),
                    }
        except:
            continue
    return None


def try_graphql(base_url):
    """Probe common GraphQL endpoints."""
    introspection = {"query": "{ __schema { queryType { name } } }"}
    for path in GRAPHQL_PROBES:
        url = base_url + path
        data, err = fetch_json(url, method="post", json_body=introspection)
        if data and isinstance(data, dict) and "data" in data:
            return {
                "method": "graphql",
                "endpoint": path,
                "pagination": "cursor",
                "sample_count": 0,
            }
    return None


def try_generic_api(base_url):
    """Probe common REST API patterns."""
    for path in API_PROBES:
        url = base_url + path
        data, err = fetch_json(url)
        if data is None:
            continue
        # Check if response looks like a listing collection
        items = None
        if isinstance(data, list) and len(data) > 0:
            items = data
        elif isinstance(data, dict):
            for key in ["items", "results", "data", "listings", "businesses",
                        "records", "posts"]:
                if isinstance(data.get(key), list) and len(data[key]) > 0:
                    items = data[key]
                    break
        if items and isinstance(items[0], dict):
            max_size = probe_page_size(url)
            return {
                "method": "rest_api",
                "endpoint": path,
                "pagination": detect_pagination_type(data),
                "page_size": max_size,
                "sample_count": len(items),
            }
    return None


def try_data_attributes(soup):
    """Check for data-* attributes that contain listing data."""
    data_attr_patterns = [
        "data-listing-id", "data-business-id", "data-id",
        "data-price", "data-listing", "data-product-id",
        "data-testid", "data-cy",
    ]
    for attr in data_attr_patterns:
        elements = soup.find_all(attrs={attr: True})
        if len(elements) >= 3:
            return {
                "method": "data_attributes",
                "endpoint": None,
                "pagination": "none",
                "container_attr": attr,
                "sample_count": len(elements),
            }
    return None


def try_css_rules(soup, url):
    """v4 rule-based CSS detection."""
    best_sel, best_count = None, 0
    for sel in CONTAINER_PATTERNS:
        try:
            matches = soup.select(sel)
            real = [m for m in matches if len(m.get_text(strip=True)) > 30]
            if len(real) > best_count:
                best_count = len(real)
                best_sel = sel
        except:
            continue

    if not best_sel or best_count < 2:
        return None

    # Detect field selectors
    containers = [c for c in soup.select(best_sel)
                  if len(c.get_text(strip=True)) > 30]
    if not containers:
        return None

    title_sel = _detect_field(containers[0], TITLE_PATTERNS)
    price_sel = _detect_field(containers[0], PRICE_PATTERNS)
    loc_sel   = _detect_field(containers[0], LOCATION_PATTERNS)

    # Quality check
    has_title = sum(1 for c in containers if c.select_one(title_sel)) if title_sel else 0
    quality = has_title / len(containers) if containers else 0

    if quality < 0.4:
        return None

    return {
        "method": "css_rules",
        "endpoint": None,
        "pagination": "page_param",
        "container": best_sel,
        "title_sel": title_sel,
        "price_sel": price_sel,
        "location_sel": loc_sel,
        "sample_count": best_count,
        "confidence": "high" if quality >= 0.7 else "medium",
    }


def try_haiku(url, html):
    """Last resort: Haiku analyzes HTML."""
    if not HAS_ANTHROPIC:
        return None
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None

    client = anthropic.Anthropic(api_key=key)
    cleaned = clean_for_ai(html)

    try:
        resp = client.messages.create(
            model=HAIKU_MODEL,
            max_tokens=500,
            messages=[{"role": "user", "content":
                       HAIKU_PROMPT.format(url=url, html=cleaned)}]
        )
        text = re.sub(r'^```(?:json)?\s*|\s*```$', '', resp.content[0].text.strip())
        data = json.loads(text)

        if not data.get("has_listings"):
            return None

        return {
            "method": "haiku_css",
            "endpoint": None,
            "pagination": "page_param",
            "container": data.get("container"),
            "title_sel": data.get("title"),
            "price_sel": data.get("price"),
            "location_sel": data.get("location"),
            "sample_count": 0,
            "confidence": data.get("confidence", "medium"),
            "haiku_tokens": resp.usage.input_tokens + resp.usage.output_tokens,
        }
    except Exception as e:
        return None

# ── Helper utilities ───────────────────────────────────────────────────────────

def probe_page_size(base_url):
    """Test how large a page size the API accepts."""
    for size in [250, 100, 50, 20]:
        separator = "&" if "?" in base_url else "?"
        data, err = fetch_json(f"{base_url}{separator}per_page={size}&limit={size}&page=1")
        if data is not None:
            items = (data if isinstance(data, list) else
                     data.get("items", data.get("results", data.get("data", []))))
            if items:
                return size
    return 20


def detect_pagination_type(data):
    """Guess pagination type from API response shape."""
    if isinstance(data, dict):
        if "next_cursor" in data or "cursor" in data:
            return "cursor"
        if "next" in data and isinstance(data["next"], str):
            return "next_url"
        if "offset" in data or "skip" in data:
            return "offset"
    return "page_param"


def find_listings_in_objects(objects):
    """Find the list that looks most like business listings."""
    best = []
    for obj in objects:
        for key, val in (obj.items() if isinstance(obj, dict) else []):
            if isinstance(val, list) and len(val) > len(best):
                if val and isinstance(val[0], dict):
                    best = val
    return best


def _detect_field(container, patterns):
    for sel in patterns:
        try:
            found = container.select_one(sel)
            if found and len(found.get_text(strip=True)) > 1:
                return sel
        except:
            continue
    return None


def is_js_rendered(html):
    text = BeautifulSoup(html, "html.parser").get_text(strip=True)
    if len(text) < 500:
        return True
    if "enable javascript" in text.lower():
        return True
    return False


def find_listings_page(base_url):
    """Try common listing URL patterns, return first 200 OK."""
    for path in LISTINGS_PATHS:
        url = base_url + path
        if head_ok(url):
            return url
    return None

# ── Supabase ───────────────────────────────────────────────────────────────────

_sb = None

def get_supabase():
    global _sb
    if _sb is None and HAS_SUPABASE:
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if url and key:
            _sb = create_client(url, key)
    return _sb


def cache_get_discovery(domain):
    sb = get_supabase()
    if not sb:
        return None
    try:
        res = sb.table("broker_discovery").select("*").eq("domain", domain).execute()
        return res.data[0] if res.data else None
    except:
        return None


def cache_set_discovery(domain, url, result):
    sb = get_supabase()
    if not sb:
        return
    try:
        row = {
            "domain": domain,
            "url": url,
            "method": result.get("method", "unknown"),
            "endpoint": result.get("endpoint"),
            "pagination": result.get("pagination"),
            "page_size": result.get("page_size"),
            "container": result.get("container"),
            "title_sel": result.get("title_sel"),
            "price_sel": result.get("price_sel"),
            "location_sel": result.get("location_sel"),
            "container_attr": result.get("container_attr"),
            "data_key": result.get("data_key"),
            "confidence": result.get("confidence", "medium"),
            "sample_count": result.get("sample_count", 0),
            "status": result.get("status", "ok"),
            "platform": result.get("platform", "unknown"),
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        }
        sb.table("broker_discovery").upsert(row, on_conflict="domain").execute()
    except Exception as e:
        print(f"   ⚠️  Supabase write error: {e}")

# ── Main discovery logic ───────────────────────────────────────────────────────

_haiku_tokens = 0

def discover(url, verbose=True):
    """Run full discovery on a URL. Returns discovery result dict."""
    domain = urlparse(url).netloc.lower().replace("www.", "")
    base_url = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
    result = {"url": url, "domain": domain, "method": None, "status": "unknown"}

    def log(msg):
        if verbose:
            print(msg)

    log(f"\n{'='*60}")
    log(f"🔍 Discovering: {url}")
    log(f"{'='*60}")

    # ── Step 1: Check cache ───────────────────────────────────────
    cached = cache_get_discovery(domain)
    if cached and not cached.get("status") == "pending":
        log(f"   💾 Already discovered: {cached.get('method')} [{cached.get('status')}]")
        return cached

    # ── Step 2: Fetch homepage ────────────────────────────────────
    html, err = fetch_text(url)
    if not html:
        # Try to find listings page
        alt = find_listings_page(base_url)
        if alt:
            html, err = fetch_text(alt)
            if html:
                url = alt
    if not html:
        log(f"   ❌ Fetch failed: {err}")
        result.update({"method": "dead", "status": "dead"})
        cache_set_discovery(domain, url, result)
        return result

    # ── Step 3: Check for auth wall ───────────────────────────────
    soup_raw = BeautifulSoup(html, "html.parser")
    body_text = soup_raw.get_text(strip=True).lower()
    if any(x in body_text for x in ["login required", "sign in to view",
                                      "members only", "please log in"]):
        log(f"   🔒 Auth required")
        result.update({"method": "auth_required", "status": "auth_required"})
        cache_set_discovery(domain, url, result)
        return result

    # ── Step 4: Detect platform ───────────────────────────────────
    platform = detect_platform(html, url)
    result["platform"] = platform
    log(f"   🏷️  Platform: {platform}")

    # ── Step 5: Platform-specific API attempts ────────────────────
    api_result = None

    if platform == "wordpress":
        log(f"   → Trying WordPress REST API...")
        api_result = try_wordpress(base_url)

    elif platform == "shopify":
        log(f"   → Trying Shopify products.json...")
        api_result = try_shopify(base_url)

    elif platform == "squarespace":
        log(f"   → Trying Squarespace ?format=json...")
        api_result = try_squarespace(url)

    elif platform == "ghost":
        log(f"   → Trying Ghost content API...")
        api_result = try_ghost(base_url)

    elif platform == "drupal":
        log(f"   → Trying Drupal JSON:API...")
        api_result = try_drupal(base_url)

    elif platform == "nextjs_pages":
        log(f"   → Trying __NEXT_DATA__ extraction...")
        api_result = try_nextjs_pages(html)

    elif platform == "nextjs_app":
        log(f"   → Trying Next.js RSC payload...")
        api_result = try_rsc(url, base_url)

    elif platform in ("nuxt", "vue_ssr"):
        log(f"   → Trying Nuxt/__NUXT__ blob...")
        api_result = try_nuxt(html)

    if api_result:
        log(f"   ✅ Platform API found: {api_result['method']} ({api_result.get('sample_count', 0)} items)")
        api_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
        cache_set_discovery(domain, url, api_result)
        return api_result

    # ── Step 6: Generic API probes ────────────────────────────────
    log(f"   → Probing generic API endpoints...")
    api_result = try_generic_api(base_url)
    if api_result:
        log(f"   ✅ REST API found: {api_result['endpoint']} ({api_result.get('sample_count', 0)} items)")
        api_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
        cache_set_discovery(domain, url, api_result)
        return api_result

    # ── Step 7: GraphQL probe ─────────────────────────────────────
    log(f"   → Probing GraphQL endpoints...")
    gql_result = try_graphql(base_url)
    if gql_result:
        log(f"   ✅ GraphQL found: {gql_result['endpoint']}")
        gql_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
        cache_set_discovery(domain, url, gql_result)
        return gql_result

    # ── Step 8: JSON-LD structured data ──────────────────────────
    log(f"   → Checking JSON-LD structured data...")
    jld_result = try_json_ld(html)
    if jld_result:
        log(f"   ✅ JSON-LD found ({jld_result.get('sample_count', 0)} items)")
        jld_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
        cache_set_discovery(domain, url, jld_result)
        return jld_result

    # ── Step 9: JS-rendered check ────────────────────────────────
    if is_js_rendered(html):
        log(f"   ⚠️  JS-rendered — needs Playwright")
        result.update({"method": "js_only", "status": "js_only", "platform": platform})
        cache_set_discovery(domain, url, result)
        return result

    # Parse soup for remaining checks
    soup = parse_html(html)

    # ── Step 10: Data attributes ──────────────────────────────────
    log(f"   → Checking data attributes...")
    attr_result = try_data_attributes(soup)
    if attr_result:
        log(f"   ✅ Data attributes found: {attr_result['container_attr']}")
        attr_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
        cache_set_discovery(domain, url, attr_result)
        return attr_result

    # ── Step 11: CSS rules (v4) ───────────────────────────────────
    log(f"   → Trying CSS rule-based detection...")
    css_result = try_css_rules(soup, url)
    if css_result:
        log(f"   ✅ CSS rules: {css_result['container']} ({css_result['sample_count']} items) [{css_result['confidence']}]")
        css_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
        cache_set_discovery(domain, url, css_result)
        return css_result

    # ── Step 12: Haiku fallback ───────────────────────────────────
    if HAS_ANTHROPIC and os.environ.get("ANTHROPIC_API_KEY"):
        log(f"   🤖 Calling Haiku (last resort)...")
        haiku_result = try_haiku(url, html)
        if haiku_result:
            global _haiku_tokens
            _haiku_tokens += haiku_result.pop("haiku_tokens", 0)
            log(f"   ✅ Haiku found: {haiku_result['container']} [{haiku_result['confidence']}]")
            haiku_result.update({"platform": platform, "status": "ok", "domain": domain, "url": url})
            cache_set_discovery(domain, url, haiku_result)
            return haiku_result
    else:
        log(f"   ⚠️  Haiku not available (no ANTHROPIC_API_KEY)")

    # ── Step 13: No method found ──────────────────────────────────
    log(f"   ❌ No extraction method found")
    result.update({"method": "failed", "status": "failed", "platform": platform})
    cache_set_discovery(domain, url, result)
    return result

# ── Batch runner ───────────────────────────────────────────────────────────────

def run_batch(csv_path, workers=5, rediscover=False):
    # Load URLs
    urls = []
    with open(csv_path) as f:
        first = f.read(500)
    if "," in first.split("\n")[0]:
        with open(csv_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("listings_url") or row.get("url") or
                       row.get("URL") or row.get("website") or
                       list(row.values())[0])
                if url and str(url).strip().startswith("http"):
                    urls.append(url.strip())
    else:
        with open(csv_path) as f:
            urls = [l.strip() for l in f if l.strip().startswith("http")]

    # Filter already-discovered unless --rediscover
    if not rediscover:
        sb = get_supabase()
        if sb:
            try:
                cached = sb.table("broker_discovery").select("domain, status").execute()
                done = {r["domain"] for r in cached.data if r.get("status") not in ("pending", None)}
                before = len(urls)
                urls = [u for u in urls
                        if urlparse(u).netloc.lower().replace("www.", "") not in done]
                print(f"   Skipping {before - len(urls)} already-discovered, running {len(urls)} new")
            except Exception as e:
                print(f"   ⚠️  Could not filter cache: {e}")

    total = len(urls)
    print(f"\n🚀 Discovery batch: {total} URLs, {workers} workers\n")

    results = []
    completed = 0
    method_counts = {}

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_url = {executor.submit(discover, url, False): url for url in urls}
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            completed += 1
            try:
                result = future.result()
                method = result.get("method", "unknown")
                method_counts[method] = method_counts.get(method, 0) + 1
                status = result.get("status", "?")
                icon = ("✅" if status == "ok" else
                        "⚠️ " if status in ("js_only", "auth_required") else "❌")
                print(f"{icon} [{completed}/{total}] {url[:55]} → {method}")
            except Exception as e:
                print(f"💥 [{completed}/{total}] {url[:55]} ERROR: {e}")
                result = {"url": url, "method": "error", "status": "error"}
            results.append(result)

            if completed % 100 == 0:
                print(f"\n📊 {completed}/{total} | methods: {json.dumps(method_counts)}\n")

    # Summary
    print(f"\n{'='*60}")
    print(f"DISCOVERY COMPLETE: {total} brokers")
    print(f"\nMethod breakdown:")
    for method, count in sorted(method_counts.items(), key=lambda x: -x[1]):
        pct = round(count / total * 100) if total else 0
        print(f"  {method:<25} {count:>4} ({pct}%)")

    if _haiku_tokens > 0:
        est = _haiku_tokens / 1_000_000 * 0.80
        print(f"\nHaiku: {_haiku_tokens:,} tokens | est. ${est:.4f}")

    os.makedirs("data", exist_ok=True)
    out = f"data/discovery_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(out, "w") as f:
        json.dump({"total": total, "methods": method_counts,
                   "haiku_tokens": _haiku_tokens, "results": results}, f, indent=2)
    print(f"\nSaved: {out}")
    return results

# ── Supabase SQL ───────────────────────────────────────────────────────────────

SUPABASE_SQL = """
-- Run once in Supabase SQL editor
CREATE TABLE IF NOT EXISTS broker_discovery (
    id             SERIAL PRIMARY KEY,
    domain         TEXT UNIQUE NOT NULL,
    url            TEXT,
    platform       TEXT,
    method         TEXT,    -- wordpress_api, shopify_api, squarespace_json, wix_api,
                            -- nextjs_nextdata, nextjs_rsc, nuxt_blob, rest_api,
                            -- graphql, jsonld, data_attributes, css_rules,
                            -- haiku_css, js_only, auth_required, dead, failed
    endpoint       TEXT,    -- API path if applicable
    pagination     TEXT,    -- page_param, cursor, offset, next_url, none, rsc
    page_size      INT,
    container      TEXT,    -- CSS selector for container (css methods)
    title_sel      TEXT,
    price_sel      TEXT,
    location_sel   TEXT,
    container_attr TEXT,    -- data-* attribute (data_attributes method)
    data_key       TEXT,    -- key in JSON blob (nextdata method)
    confidence     TEXT,
    sample_count   INT,
    status         TEXT DEFAULT 'ok',  -- ok, js_only, auth_required, dead, failed
    discovered_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS broker_discovery_domain_idx ON broker_discovery(domain);
CREATE INDEX IF NOT EXISTS broker_discovery_method_idx ON broker_discovery(method);
CREATE INDEX IF NOT EXISTS broker_discovery_status_idx ON broker_discovery(status);
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DealLedger Discovery Module")
    parser.add_argument("url", nargs="?", help="Single URL to discover")
    parser.add_argument("--batch", help="CSV or URL list file")
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--rediscover", action="store_true",
                        help="Re-run discovery even for cached domains")
    parser.add_argument("--print-sql", action="store_true",
                        help="Print Supabase SQL setup and exit")
    args = parser.parse_args()

    if args.print_sql:
        print(SUPABASE_SQL)
        sys.exit(0)

    if args.batch:
        run_batch(args.batch, workers=args.workers, rediscover=args.rediscover)
    elif args.url:
        result = discover(args.url, verbose=True)
        print(f"\n{json.dumps(result, indent=2)}")
    else:
        parser.print_help()
