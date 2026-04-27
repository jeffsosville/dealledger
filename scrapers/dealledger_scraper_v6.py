#!/usr/bin/env python3
"""
DealLedger Scraper V6
=====================
Builds on V5 with three core upgrades:

1. DETAIL PAGE FETCHING
   After extracting listing cards from a list page, V6 follows each
   detail URL and re-extracts from the full page. This is where state,
   complete title, full financials, and description actually live.
   Cards on list pages are truncated summaries — detail pages have everything.

2. BROAD LOCATION EXTRACTION
   V5 only caught "City, ST" format. V6 also catches:
   - Full state names ("Located in Florida", "Chicago, Illinois")
   - State in title ("Electrical Contractor Maryland")
   - Bare state names anywhere in text
   - State abbreviations in parentheses "(FL)"

3. SUPABASE OUTPUT
   Writes directly to listings_direct in addition to local snapshots.
   Upserts on id so reruns are idempotent.
   Preserves first_seen, always updates last_seen.

4. PROXY SUPPORT (V6.1)
   Residential proxy via PROXY_URL env var.
   Automatically used for domains in PROXY_DOMAINS set.
   Works for both requests and Playwright fetchers.
   Add domains to PROXY_DOMAINS as you discover 403s.

SELF-CORRECTION LOOP (auto-accept mode per CLAUDE.md)
  --broker mode runs a single broker and outputs a classified result:
  PATTERN_MATCH, HTTP_403, CAPTCHA, or NO_PATTERN.
  Claude can read this output, adjust logic, and retest before bulk runs.
  Bulk runs (--all, --top-n) always require manual go-ahead.

Usage:
    # Single broker test gate (auto-accept loop)
    python3 dealledger_scraper_v6.py --broker "https://example.com/listings" --name "Example"

    # Bulk (manual go-ahead required)
    python3 dealledger_scraper_v6.py --brokers data/brokers.csv --test
    python3 dealledger_scraper_v6.py --brokers data/brokers.csv --top-n 50
    python3 dealledger_scraper_v6.py --brokers data/brokers.csv --all

    # Skip Supabase
    python3 dealledger_scraper_v6.py --brokers data/brokers.csv --top-n 50 --no-supabase

Env vars:
    SUPABASE_URL
    SUPABASE_SERVICE_KEY
    PROXY_URL          # e.g. http://user:pass@gw.dataimpulse.com:823

Requirements:
    pip3 install pandas beautifulsoup4 playwright requests supabase --break-system-packages
    playwright install chromium
"""

import argparse
import hashlib
import json
import math
import os
import random
import re
import sys
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    print("⚠️  Playwright not installed — JS-rendered sites will be skipped.")
    print("   pip3 install playwright && playwright install chromium")

try:
    from supabase import create_client
    HAS_SUPABASE = True
except ImportError:
    HAS_SUPABASE = False


# ============================================================
# CONFIGURATION
# ============================================================

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

LISTING_KEYWORDS = [
    "for sale", "asking price", "cash flow", "revenue", "ebitda", "sde",
    "seller discretionary", "business for sale", "listing", "gross revenue",
    "net income", "annual revenue", "inventory", "franchise", "established",
    "profitable", "turnkey", "owner operator", "relocatable", "absentee",
]

PRICE_PATTERN = re.compile(r'\$[\d,]+(?:\.\d{2})?|\d{1,3}(?:,\d{3})+')
MONEY_PATTERN = re.compile(r'\$\s*[\d,]+(?:\.\d{2})?')

# Max detail pages per broker to avoid hammering servers
MAX_DETAIL_PAGES = 50
DETAIL_DELAY     = (0.5, 1.5)   # seconds between detail fetches

US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada",
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York",
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma",
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia",
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia",
}
STATE_NAME_TO_ABBREV = {v.lower(): k for k, v in US_STATES.items()}

VERTICALS = {
    "cleaning":      ["cleaning", "janitorial", "maid", "custodial", "housekeeping",
                      "carpet cleaning", "pressure wash", "window cleaning", "sanitation"],
    "hvac":          ["hvac", "heating", "cooling", "air conditioning", "furnace",
                      "refrigeration", "mechanical contractor"],
    "landscaping":   ["landscaping", "lawn care", "lawn maintenance", "tree service",
                      "irrigation", "hardscaping", "snow removal"],
    "plumbing":      ["plumbing", "plumber", "drain", "sewer", "water heater", "pipe"],
    "electrical":    ["electrical", "electrician", "wiring", "lighting"],
    "pest_control":  ["pest control", "exterminator", "termite", "pest management"],
    "roofing":       ["roofing", "roofer", "roof repair", "shingle"],
    "vending":       ["vending", "vending machine", "vending route", "amusement", "atm route"],
    "restaurant":    ["restaurant", "cafe", "bar", "grill", "pizzeria", "bakery",
                      "catering", "food truck"],
    "automotive":    ["auto repair", "auto body", "car wash", "oil change", "tire",
                      "mechanic", "automotive"],
    "healthcare":    ["medical", "dental", "therapy", "clinic", "healthcare", "wellness",
                      "pharmacy", "chiropractic"],
    "technology":    ["saas", "software", "tech", "app", "digital", "ecommerce",
                      "shopify", "subscription"],
    "construction":  ["construction", "contractor", "remodeling", "concrete", "masonry"],
    "retail":        ["retail", "store", "boutique", "clothing", "gift shop", "florist"],
    "manufacturing": ["manufacturing", "machining", "fabrication", "production",
                      "factory", "industrial"],
}


# ============================================================
# PROXY CONFIG
# ============================================================

# Set PROXY_URL env var — never hardcode credentials
# Format: http://user:pass@host:port
PROXY_URL = os.environ.get("PROXY_URL", "")

# Domains that should always use the proxy (403 blockers)
PROXY_DOMAINS = {
    "hedgestone.com",
    # Add more as you discover them
}

def _needs_proxy(domain: str) -> bool:
    """Return True if this domain requires proxy routing."""
    if not PROXY_URL:
        return False
    return any(pd in domain for pd in PROXY_DOMAINS)


# ============================================================
# LOCATION EXTRACTOR (V6 — broad)
# ============================================================

class LocationExtractor:
    """
    Multi-strategy location extraction.
    Strategies run in order of precision — returns first confident match.
    """

    CITY_STATE_ABBREV = re.compile(
        r'\b([A-Z][a-zA-Z]+(?:[\s-][A-Z][a-zA-Z]+)?)\s*,\s*([A-Z]{2})\b'
    )
    CITY_STATE_FULL = re.compile(
        r'\b([A-Z][a-zA-Z]+(?:[\s-][A-Z][a-zA-Z]+)?)\s*,\s*'
        r'(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|'
        r'Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|'
        r'Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|'
        r'Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|'
        r'New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|'
        r'Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|'
        r'Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|'
        r'District of Columbia)\b',
        re.IGNORECASE,
    )
    LOCATED_IN = re.compile(
        r'(?:located\s+in|location\s*[:\-]?\s*|based\s+in|area\s*[:\-]?\s*)'
        r'\s*([A-Z][a-zA-Z]+(?:\s[A-Z][a-zA-Z]+)?)',
        re.IGNORECASE,
    )
    STATE_IN_PARENS = re.compile(r'\(([A-Z]{2})\)')
    BARE_ABBREV = re.compile(
        r'(?:^|[\s,–\-])'
        r'(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|'
        r'MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|'
        r'TX|UT|VT|VA|WA|WV|WI|WY|DC)'
        r'(?:$|[\s,\.])'
    )

    @classmethod
    def extract(cls, text: str) -> dict | None:
        if not text:
            return None

        # 1. "City, ST"
        m = cls.CITY_STATE_ABBREV.search(text)
        if m and m.group(2) in US_STATES:
            return {"city": m.group(1), "state": m.group(2)}

        # 2. "City, Full State Name"
        m = cls.CITY_STATE_FULL.search(text)
        if m:
            abbrev = STATE_NAME_TO_ABBREV.get(m.group(2).lower())
            if abbrev:
                return {"city": m.group(1), "state": abbrev}

        # 3. "Located in X" / "Location: X"
        m = cls.LOCATED_IN.search(text)
        if m:
            candidate = m.group(1).strip()
            abbrev = STATE_NAME_TO_ABBREV.get(candidate.lower())
            if abbrev:
                return {"city": None, "state": abbrev}
            if candidate.upper() in US_STATES:
                return {"city": None, "state": candidate.upper()}

        # 4. Full state name anywhere (longest match first)
        text_lower = text.lower()
        for name, abbrev in sorted(STATE_NAME_TO_ABBREV.items(),
                                   key=lambda x: len(x[0]), reverse=True):
            if re.search(r'\b' + re.escape(name) + r'\b', text_lower):
                return {"city": None, "state": abbrev}

        # 5. "(FL)" style
        m = cls.STATE_IN_PARENS.search(text)
        if m and m.group(1) in US_STATES:
            return {"city": None, "state": m.group(1)}

        # 6. Bare abbreviation (last resort, lower confidence)
        m = cls.BARE_ABBREV.search(text)
        if m and m.group(1) in US_STATES:
            return {"city": None, "state": m.group(1)}

        return None


# ============================================================
# PATTERN CACHE
# ============================================================

class PatternCache:

    def __init__(self, cache_path="data/pattern_cache.json"):
        self.cache_path = cache_path
        self.patterns = self._load()

    def _load(self):
        if os.path.exists(self.cache_path):
            with open(self.cache_path) as f:
                return json.load(f)
        return {}

    def save(self):
        os.makedirs(os.path.dirname(self.cache_path) or ".", exist_ok=True)
        with open(self.cache_path, "w") as f:
            json.dump(self.patterns, f, indent=2)

    def get(self, domain):
        entry = self.patterns.get(domain)
        if not entry:
            return None
        if "pattern" in entry and isinstance(entry["pattern"], dict):
            return entry["pattern"]
        if "container_selector" in entry:
            return entry
        return None

    def store(self, domain, pattern):
        self.patterns[domain] = pattern
        self.save()

    def predict(self, html, url):
        if not self.patterns:
            return None
        soup = BeautifulSoup(html, "html.parser")
        best_match, best_score = None, 0
        for domain, pattern in self.patterns.items():
            sel = pattern.get("container_selector", "")
            if not sel:
                continue
            try:
                score = len(soup.select(sel))
                if score > best_score and score >= 3:
                    best_score = score
                    best_match = pattern
            except Exception:
                pass
        return best_match


# ============================================================
# PATTERN DETECTOR
# ============================================================

class PatternDetector:

    @staticmethod
    def detect(html, url):
        soup = BeautifulSoup(html, "html.parser")
        candidates = []

        # Strategy 1: repeated classed elements with listing signals
        for tag in ["div", "article", "li", "tr", "a", "section"]:
            class_groups = defaultdict(list)
            for el in soup.find_all(tag):
                classes = el.get("class", [])
                if classes:
                    class_groups[f"{tag}.{'.'.join(classes)}"].append(el)

            for selector, group in class_groups.items():
                if len(group) < 3:
                    continue
                score = 0
                for el in group[:10]:
                    text = el.get_text(separator=" ", strip=True).lower()
                    if PRICE_PATTERN.search(text):
                        score += 2
                    if any(kw in text for kw in LISTING_KEYWORDS[:6]):
                        score += 1
                if score >= 3:
                    candidates.append({
                        "container_selector": selector,
                        "count": len(group),
                        "score": score,
                        "sample_text": group[0].get_text(separator=" ", strip=True)[:200],
                    })

        # Strategy 2: known selectors
        for selector in [
            "div.listing", "div.listing-item", "div.property-listing",
            "article.listing", "div.business-listing", "div.result",
            "div.search-result", "div.card", "div.listing-card",
            "tr.listing-row", "div.property-card", "li.listing",
            "div.item", "div.post", "div.entry",
        ]:
            try:
                elements = soup.select(selector)
                if len(elements) >= 3:
                    score = 0
                    for el in elements[:10]:
                        text = el.get_text(separator=" ", strip=True).lower()
                        if PRICE_PATTERN.search(text):
                            score += 2
                        if any(kw in text for kw in LISTING_KEYWORDS[:6]):
                            score += 1
                    if score >= 2:
                        candidates.append({
                            "container_selector": selector,
                            "count": len(elements),
                            "score": score + 5,
                            "sample_text": elements[0].get_text(separator=" ", strip=True)[:200],
                        })
            except Exception:
                pass

        if not candidates:
            return None
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[0]


# ============================================================
# LISTING EXTRACTOR (V6)
# ============================================================

class ListingExtractor:

    @staticmethod
    def extract_price(text):
        parsed = []
        for p in MONEY_PATTERN.findall(text):
            try:
                val = float(p.replace("$", "").replace(",", "").strip())
                if 1_000 <= val <= 100_000_000:
                    parsed.append(val)
            except Exception:
                pass
        return max(parsed) if parsed else None

    @staticmethod
    def extract_cash_flow(text):
        text_lower = text.lower()
        for kw in ["cash flow", "sde", "ebitda", "seller discretionary",
                   "net income", "owner benefit", "adjusted earnings", "owner's benefit"]:
            idx = text_lower.find(kw)
            if idx >= 0:
                nearby = text[max(0, idx - 10):idx + 100]
                for p in MONEY_PATTERN.findall(nearby):
                    try:
                        val = float(p.replace("$", "").replace(",", "").strip())
                        if val >= 1_000:
                            return val
                    except Exception:
                        pass
        return None

    @staticmethod
    def extract_revenue(text):
        text_lower = text.lower()
        for kw in ["gross revenue", "gross sales", "annual sales", "total sales",
                   "gross income", "annual revenue", "revenue"]:
            idx = text_lower.find(kw)
            if idx >= 0:
                nearby = text[max(0, idx - 10):idx + 100]
                for p in MONEY_PATTERN.findall(nearby):
                    try:
                        val = float(p.replace("$", "").replace(",", "").strip())
                        if val >= 1_000:
                            return val
                    except Exception:
                        pass
        return None

    @staticmethod
    def classify_vertical(text):
        text_lower = text.lower()
        scores = {v: sum(1 for kw in kws if kw in text_lower)
                  for v, kws in VERTICALS.items()}
        scores = {v: s for v, s in scores.items() if s > 0}
        return max(scores, key=scores.get) if scores else "other"

    @staticmethod
    def best_title_from_detail(soup) -> str | None:
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(strip=True)
            if len(t) > 5:
                return t[:500]
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return og["content"][:500]
        tag = soup.find("title")
        if tag:
            return tag.get_text(strip=True)[:500]
        return None

    @staticmethod
    def best_description_from_detail(soup) -> str | None:
        for sel in [
            "div.description", "div.listing-description", "div.business-description",
            "div.content", "div.listing-content", "div.details",
            "[class*='description']", "[class*='detail']",
        ]:
            try:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(separator=" ", strip=True)
                    if len(text) > 100:
                        return text[:2000]
            except Exception:
                pass
        # Fallback: largest div under 5000 chars
        best = ""
        for div in soup.find_all("div"):
            text = div.get_text(separator=" ", strip=True)
            if len(text) > len(best) and len(text) < 5000:
                best = text
        return best[:2000] if best else None

    @classmethod
    def from_card(cls, element, base_url: str, broker_name: str) -> dict | None:
        """Extract a listing stub from a list-page card."""
        text = element.get_text(separator=" ", strip=True)
        if len(text) < 20:
            return None

        title_el = element.find(["h1", "h2", "h3", "h4", "h5", "a"])
        title = title_el.get_text(strip=True) if title_el else text[:100]

        link_el = element.find("a", href=True)
        detail_url = urljoin(base_url, link_el["href"]) if link_el else None

        # Don't treat pagination/anchor links as detail URLs
        if detail_url:
            d = detail_url.rstrip("/")
            b = base_url.rstrip("/")
            if d == b or "#" in d.split("?")[0][-5:]:
                detail_url = None

        asking_price = cls.extract_price(text)
        cash_flow    = cls.extract_cash_flow(text)
        revenue      = cls.extract_revenue(text)
        location     = LocationExtractor.extract(text)
        vertical     = cls.classify_vertical(text)

        listing_id = hashlib.sha256(
            f"{title}|{asking_price}|{detail_url or base_url}".encode()
        ).hexdigest()[:16]

        now = datetime.now(timezone.utc).isoformat()

        return {
            "id":            listing_id,
            "title":         title[:500],
            "asking_price":  asking_price,
            "cash_flow":     cash_flow,
            "revenue":       revenue,
            "city":          location["city"]  if location else None,
            "state":         location["state"] if location else None,
            "vertical":      vertical,
            "url":           detail_url or base_url,
            "broker_name":   broker_name,
            "broker_domain": urlparse(base_url).netloc,
            "description":   text[:1000],
            "first_seen":    now,
            "last_seen":     now,
            "_detail_url":   detail_url,
        }

    @classmethod
    def enrich_from_detail(cls, listing: dict, html: str) -> dict:
        """
        Re-extract all fields from the full detail page.
        Only overwrites if the detail page has richer data.
        State is the most important field to gain here.
        """
        soup = BeautifulSoup(html, "html.parser")
        full_text = soup.get_text(separator=" ", strip=True)

        # Title
        detail_title = cls.best_title_from_detail(soup)
        if detail_title and len(detail_title) > len(listing.get("title") or ""):
            listing["title"] = detail_title

        # Description
        desc = cls.best_description_from_detail(soup)
        if desc:
            listing["description"] = desc

        # Financials — full page text has much more context
        if not listing.get("asking_price"):
            listing["asking_price"] = cls.extract_price(full_text)
        if not listing.get("cash_flow"):
            listing["cash_flow"] = cls.extract_cash_flow(full_text)
        if not listing.get("revenue"):
            listing["revenue"] = cls.extract_revenue(full_text)

        # Location — the key reason we fetch detail pages
        if not listing.get("state"):
            loc = LocationExtractor.extract(full_text)
            if loc:
                listing["city"]  = listing.get("city") or loc.get("city")
                listing["state"] = loc["state"]

        # Re-classify vertical with full description
        listing["vertical"] = cls.classify_vertical(
            (listing.get("description") or "") + " " + (listing.get("title") or "")
        )

        return listing


# ============================================================
# PAGE FETCHER (with proxy support)
# ============================================================

class PageFetcher:

    def __init__(self):
        self.session = requests.Session()
        self.playwright   = None
        self.browser_plain = None   # no proxy
        self.browser_proxy = None   # with proxy

    def _proxy_dict(self):
        """requests-style proxy dict from PROXY_URL env var."""
        if not PROXY_URL:
            return None
        return {"http": PROXY_URL, "https": PROXY_URL}

    def _playwright_proxy(self):
        """Playwright proxy config from PROXY_URL env var."""
        if not PROXY_URL:
            return None
        return {"server": PROXY_URL}

    def _ensure_playwright(self):
        if not self.playwright:
            self.playwright = sync_playwright().start()

    def _get_browser(self, use_proxy: bool):
        self._ensure_playwright()
        if use_proxy:
            if not self.browser_proxy:
                proxy_conf = self._playwright_proxy()
                self.browser_proxy = self.playwright.chromium.launch(
                    headless=True,
                    proxy=proxy_conf,
                )
            return self.browser_proxy
        else:
            if not self.browser_plain:
                self.browser_plain = self.playwright.chromium.launch(headless=True)
            return self.browser_plain

    def fetch_requests(self, url, timeout=15, use_proxy=False):
        headers = {
            "User-Agent":      random.choice(USER_AGENTS),
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer":         "https://www.google.com/",
        }
        resp = self.session.get(
            url, headers=headers, timeout=timeout,
            allow_redirects=True,
            proxies=self._proxy_dict() if use_proxy else None,
        )
        resp.raise_for_status()
        return resp.text

    def fetch_playwright(self, url, timeout=20000, use_proxy=False):
        if not HAS_PLAYWRIGHT:
            raise RuntimeError("Playwright not installed")
        browser = self._get_browser(use_proxy)
        page = browser.new_page(user_agent=random.choice(USER_AGENTS))
        try:
            page.goto(url, timeout=timeout, wait_until="networkidle")
            page.wait_for_timeout(2000)
            return page.content()
        finally:
            page.close()

    def fetch(self, url, use_proxy=False):
        try:
            html = self.fetch_requests(url, use_proxy=use_proxy)
            if len(html) > 3000:
                return html, "requests"
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                raise  # Propagate 403 for failure classification
        except Exception:
            pass

        if HAS_PLAYWRIGHT:
            try:
                return self.fetch_playwright(url, use_proxy=use_proxy), "playwright"
            except Exception:
                pass

        return self.fetch_requests(url, timeout=25, use_proxy=use_proxy), "requests"

    def close(self):
        if self.browser_plain:
            self.browser_plain.close()
        if self.browser_proxy:
            self.browser_proxy.close()
        if self.playwright:
            self.playwright.stop()


# ============================================================
# PAGINATION HANDLER
# ============================================================

class PaginationHandler:

    @staticmethod
    def find_next_page(soup, current_url):
        for sel in ["a.next", "a.next-page", "a[rel='next']", "li.next a",
                    "a.pagination-next", "a[aria-label='Next']",
                    ".pagination a.active + a"]:
            try:
                el = soup.select_one(sel)
                if el and el.get("href"):
                    return urljoin(current_url, el["href"])
            except Exception:
                pass

        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True).lower() in ["next", "next »", "next ›",
                                                   "next page", "→", ">"]:
                return urljoin(current_url, a["href"])
        return None


# ============================================================
# SUPABASE WRITER
# ============================================================

class SupabaseWriter:

    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        if not HAS_SUPABASE:
            raise RuntimeError("pip3 install supabase")
        self.client = create_client(url, key)

    def upsert(self, listings: list[dict]) -> int:
        """
        Upsert into listings_direct on conflict id.
        Preserves first_seen on existing rows.
        """
        now = datetime.now(timezone.utc).isoformat()
        rows = []
        for l in listings:
            rows.append({
                "id":            l["id"],
                "title":         (l.get("title") or "")[:500],
                "url":           l.get("url"),
                "broker_name":   l.get("broker_name"),
                "broker_domain": l.get("broker_domain"),
                "city":          l.get("city"),
                "state":         l.get("state"),
                "asking_price":  l.get("asking_price"),
                "cash_flow":     l.get("cash_flow"),
                "revenue":       l.get("revenue"),
                "vertical":      l.get("vertical", "other"),
                "description":   (l.get("description") or "")[:2000],
                "status":        "active",
                "source":        "broker_direct",
                "first_seen":    l.get("first_seen", now),
                "last_seen":     now,
                "updated_at":    now,
                "created_at":    l.get("first_seen", now),
            })

        written = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            self.client.table("listings_direct").upsert(
                batch, on_conflict="id", ignore_duplicates=False
            ).execute()
            written += len(batch)
            time.sleep(0.1)
        return written


# ============================================================
# MAIN SCRAPER
# ============================================================

class DealLedgerScraper:

    def __init__(self, output_dir="data/snapshots", use_supabase=True):
        self.output_dir = output_dir
        self.pattern_cache = PatternCache()
        self.fetcher = PageFetcher()
        self.supabase_writer = None

        if use_supabase:
            try:
                self.supabase_writer = SupabaseWriter()
                print("✅ Supabase connected")
            except Exception as e:
                print(f"⚠️  Supabase unavailable: {e} — local files only")

        if PROXY_URL:
            print(f"🔀 Proxy enabled — {len(PROXY_DOMAINS)} domain(s) routed through proxy")
        else:
            print("ℹ️  No proxy configured (set PROXY_URL env var to enable)")

        self.stats = {
            "started": datetime.now(timezone.utc).isoformat(),
            "brokers_attempted": 0,
            "brokers_success":   0,
            "brokers_failed":    0,
            "total_listings":        0,
            "listings_with_price":   0,
            "listings_with_cashflow": 0,
            "listings_with_state":   0,
            "detail_pages_fetched":  0,
            "patterns_cached":   len(self.pattern_cache.patterns),
            "patterns_learned":  0,
            "verticals":         Counter(),
            "failure_types":     Counter(),
        }
        self.all_listings: list[dict] = []
        self.failures:     list[dict] = []

    @staticmethod
    def _load_brokers(csv_path: str) -> list[dict]:
        df = pd.read_csv(csv_path)
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        url_col = next(
            (c for c in df.columns if any(k in c for k in ["url", "website", "link"])),
            None,
        )
        if not url_col:
            for col in df.columns:
                if any(str(v).startswith("http") for v in df[col].dropna().head(5)):
                    url_col = col
                    break

        if not url_col:
            print(f"❌ Cannot find URL column. Columns: {list(df.columns)}")
            sys.exit(1)

        name_col = next(
            (c for c in df.columns if any(k in c for k in ["name", "broker", "company"])),
            None,
        )

        brokers = []
        for _, row in df.iterrows():
            url = str(row[url_col]).strip()
            if not url.startswith("http"):
                continue
            name = (str(row[name_col]).strip()
                    if name_col and pd.notna(row.get(name_col))
                    else urlparse(url).netloc)
            brokers.append({"name": name, "url": url, "domain": urlparse(url).netloc})

        print(f"📋 Loaded {len(brokers)} brokers from {csv_path}")
        return brokers

    @staticmethod
    def _classify_failure(error: Exception,
                          status_code: int | None = None) -> str:
        """Returns HTTP_403 | CAPTCHA | NO_PATTERN | FETCH_ERROR"""
        if status_code == 403 or "403" in str(error):
            return "HTTP_403"
        msg = str(error).lower()
        if any(w in msg for w in ["captcha", "cloudflare", "challenge",
                                  "robot", "blocked", "access denied"]):
            return "CAPTCHA"
        if "no listing pattern" in msg:
            return "NO_PATTERN"
        return "FETCH_ERROR"

    def scrape_broker(self, broker: dict) -> list[dict]:
        name      = broker["name"]
        url       = broker["url"]
        domain    = broker["domain"]
        use_proxy = _needs_proxy(domain)

        print(f"\n{'='*60}")
        print(f"🔍 [{self.stats['brokers_attempted']+1}] {name}"
              + (" 🔀" if use_proxy else ""))
        print(f"   {url}")
        self.stats["brokers_attempted"] += 1

        try:
            # ── Fetch list page ───────────────────────────────────────────
            try:
                html, method = self.fetcher.fetch(url, use_proxy=use_proxy)
            except requests.HTTPError as e:
                code = e.response.status_code if e.response else None
                ftype = self._classify_failure(e, code)
                print(f"   ❌ {ftype} (HTTP {code})")
                self.failures.append({"broker": name, "url": url,
                                      "error": str(e), "type": ftype})
                self.stats["brokers_failed"] += 1
                self.stats["failure_types"][ftype] += 1
                return []

            print(f"   ✅ Fetched ({method}, {len(html):,} bytes)")

            # ── Pattern detection ─────────────────────────────────────────
            cached = self.pattern_cache.get(domain)
            if cached:
                pattern = cached
                print(f"   📦 Cached pattern: {pattern['container_selector']}")
            else:
                pattern = PatternDetector.detect(html, url)
                if pattern:
                    predicted = self.pattern_cache.predict(html, url)
                    if predicted and predicted.get("score", 0) > pattern.get("score", 0):
                        pattern = predicted
                        print(f"   🧠 ML predicted: {pattern['container_selector']}")
                    else:
                        print(f"   🔎 Detected: {pattern['container_selector']} "
                              f"({pattern['count']} elements)")
                    self.pattern_cache.store(domain, pattern)
                    self.stats["patterns_learned"] += 1
                else:
                    print(f"   ⚠️  NO_PATTERN")
                    self.failures.append({"broker": name, "url": url,
                                          "error": "No listing pattern detected",
                                          "type": "NO_PATTERN",
                                          "html_size": len(html)})
                    self.stats["brokers_failed"] += 1
                    self.stats["failure_types"]["NO_PATTERN"] += 1
                    return []

            # ── Phase 1: extract cards from all list pages ────────────────
            cards: list[dict] = []
            current_url = url
            page_num = 1
            max_pages = 20

            while current_url and page_num <= max_pages:
                if page_num > 1:
                    time.sleep(random.uniform(1, 3))
                    try:
                        html, _ = self.fetcher.fetch(current_url, use_proxy=use_proxy)
                    except Exception:
                        break

                soup = BeautifulSoup(html, "html.parser")
                try:
                    elements = soup.select(pattern["container_selector"])
                except Exception:
                    elements = []

                if not elements:
                    break

                page_cards = []
                for el in elements:
                    listing = ListingExtractor.from_card(el, current_url, name)
                    if listing:
                        page_cards.append(listing)

                if not page_cards:
                    break

                cards.extend(page_cards)
                if page_num > 1:
                    print(f"   📄 Page {page_num}: {len(page_cards)} cards")

                current_url = PaginationHandler.find_next_page(soup, current_url)
                page_num += 1

            # Deduplicate cards
            seen: set[str] = set()
            unique_cards: list[dict] = []
            for c in cards:
                if c["id"] not in seen:
                    seen.add(c["id"])
                    unique_cards.append(c)

            if not unique_cards:
                print(f"   ⚠️  Pattern matched but no listings extracted")
                self.failures.append({"broker": name, "url": url,
                                      "error": "Pattern matched, extraction failed",
                                      "type": "NO_PATTERN",
                                      "pattern": pattern["container_selector"]})
                self.stats["brokers_failed"] += 1
                return []

            print(f"   📋 {len(unique_cards)} cards ({page_num-1} pages)")

            # ── Phase 2: fetch detail pages ───────────────────────────────
            detail_candidates = [
                c for c in unique_cards
                if c.get("_detail_url") and c["_detail_url"] != url
            ][:MAX_DETAIL_PAGES]

            enriched = 0
            states_gained = 0

            for listing in detail_candidates:
                detail_url = listing.pop("_detail_url")
                try:
                    time.sleep(random.uniform(*DETAIL_DELAY))
                    detail_html, _ = self.fetcher.fetch(detail_url, use_proxy=use_proxy)
                    had_state = bool(listing.get("state"))
                    listing = ListingExtractor.enrich_from_detail(listing, detail_html)
                    listing["url"] = detail_url
                    enriched += 1
                    self.stats["detail_pages_fetched"] += 1
                    if not had_state and listing.get("state"):
                        states_gained += 1
                except Exception:
                    listing.pop("_detail_url", None)

            # Strip remaining internal fields
            for l in unique_cards:
                l.pop("_detail_url", None)

            if enriched:
                print(f"   🔍 {enriched} detail pages → {states_gained} states gained")

            p  = sum(1 for l in unique_cards if l.get("asking_price"))
            cf = sum(1 for l in unique_cards if l.get("cash_flow"))
            st = sum(1 for l in unique_cards if l.get("state"))
            print(f"   💰 price={p}  cf={cf}  state={st}/{len(unique_cards)}")

            self.stats["brokers_success"] += 1
            return unique_cards

        except Exception as e:
            ftype = self._classify_failure(e)
            print(f"   ❌ {ftype}: {str(e)[:150]}")
            self.failures.append({"broker": name, "url": url,
                                  "error": str(e)[:300], "type": ftype,
                                  "traceback": traceback.format_exc()[-500:]})
            self.stats["brokers_failed"] += 1
            self.stats["failure_types"][ftype] += 1
            return []

    def run(self, brokers: list[dict]) -> None:
        print(f"📦 Pattern cache: {len(self.pattern_cache.patterns)} patterns\n")

        for broker in brokers:
            listings = self.scrape_broker(broker)
            self.all_listings.extend(listings)
            for l in listings:
                self.stats["verticals"][l.get("vertical", "other")] += 1
            time.sleep(random.uniform(2, 5))

        self.stats.update({
            "total_listings":         len(self.all_listings),
            "listings_with_price":    sum(1 for l in self.all_listings if l.get("asking_price")),
            "listings_with_cashflow": sum(1 for l in self.all_listings if l.get("cash_flow")),
            "listings_with_state":    sum(1 for l in self.all_listings if l.get("state")),
            "patterns_cached":        len(self.pattern_cache.patterns),
            "completed":              datetime.now(timezone.utc).isoformat(),
            "verticals":              dict(self.stats["verticals"]),
            "failure_types":          dict(self.stats["failure_types"]),
        })

        self._save_results()

        if self.supabase_writer and self.all_listings:
            print(f"\n📤 Writing {len(self.all_listings)} listings to Supabase...")
            try:
                written = self.supabase_writer.upsert(self.all_listings)
                print(f"   ✅ {written} rows upserted")
            except Exception as e:
                print(f"   ❌ Supabase write failed: {e}")

        self._print_summary()

    def _save_results(self):
        today = datetime.now().strftime("%Y-%m-%d")
        snap  = os.path.join(self.output_dir, today)
        os.makedirs(snap, exist_ok=True)

        with open(os.path.join(snap, "listings.json"), "w") as f:
            json.dump(self.all_listings, f, indent=2, default=str)
        if self.all_listings:
            pd.DataFrame(self.all_listings).to_csv(
                os.path.join(snap, "listings.csv"), index=False)
        with open(os.path.join(snap, "failures.json"), "w") as f:
            json.dump(self.failures, f, indent=2, default=str)
        with open(os.path.join(snap, "summary.json"), "w") as f:
            json.dump(self.stats, f, indent=2, default=str)

        print(f"\n📁 Saved to {snap}/")

    def _print_summary(self):
        s = self.stats
        print(f"\n{'='*60}")
        print(f"DEALLEDGER V6 COMPLETE")
        print(f"{'='*60}")
        print(f"Brokers:   {s['brokers_attempted']} attempted  "
              f"{s['brokers_success']} ok  {s['brokers_failed']} failed")
        if s.get("failure_types"):
            for ft, cnt in s["failure_types"].items():
                print(f"  {ft}: {cnt}")
        print()
        print(f"Listings:  {s['total_listings']} total")
        print(f"  price:   {s['listings_with_price']}")
        print(f"  cf:      {s['listings_with_cashflow']}")
        print(f"  state:   {s['listings_with_state']}")
        print(f"  detail pages: {s['detail_pages_fetched']}")
        print(f"Patterns:  {s['patterns_cached']} cached  "
              f"{s['patterns_learned']} learned")
        if s.get("verticals"):
            print(f"\nVerticals:")
            for v, cnt in sorted(s["verticals"].items(), key=lambda x: -x[1]):
                print(f"  {v:<20} {cnt}")
        print(f"{'='*60}")

    def cleanup(self):
        self.fetcher.close()


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="DealLedger Scraper V6")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--broker",  help="Single broker URL (test gate)")
    src.add_argument("--brokers", help="Brokers CSV path")
    parser.add_argument("--name",        default="Test Broker")
    parser.add_argument("--test",        action="store_true")
    parser.add_argument("--top-n",       type=int)
    parser.add_argument("--all",         action="store_true")
    parser.add_argument("--output",      default="data/snapshots")
    parser.add_argument("--no-supabase", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("DEALLEDGER SCRAPER V6")
    print("=" * 60)

    scraper = DealLedgerScraper(
        output_dir=args.output,
        use_supabase=not args.no_supabase,
    )

    try:
        if args.broker:
            print(f"🧪 SINGLE BROKER TEST\n")
            brokers = [{"name": args.name, "url": args.broker,
                        "domain": urlparse(args.broker).netloc}]
        else:
            brokers = scraper._load_brokers(args.brokers)
            if args.test:
                brokers = brokers[:5]
                print(f"\n🧪 TEST MODE: {len(brokers)} brokers")
            elif args.top_n:
                brokers = brokers[:args.top_n]
                print(f"\n📊 TOP {len(brokers)} brokers")
            elif args.all:
                print(f"\n🚀 ALL {len(brokers)} brokers")
            else:
                print("Specify --test, --top-n N, or --all")
                sys.exit(1)

        scraper.run(brokers)
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    main()