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
   first_seen is protected by a DB trigger, not by this code — see
   SupabaseWriter.upsert(). last_seen is always updated.

4. PROXY SUPPORT (V6.1)
   Residential proxy via PROXY_URL env var.
   Automatically used for domains in PROXY_DOMAINS set.
   Works for both requests and Playwright fetchers.
   Add domains to PROXY_DOMAINS as you discover 403s.

5. STALENESS ROTATION (V6.2)
   --stale-first orders brokers by least-recently-scraped BEFORE --top-n
   slices the batch. Reads max(last_seen) per broker_domain from
   listings_direct; never-scraped brokers sort to the front. This makes
   the daily batch rotate through the whole registry instead of re-scraping
   the same first-N rows of the CSV every day. Falls back to CSV order if
   the ordering query fails — never blocks a run.

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

    # Rotating batch (recommended for daily cron)
    python3 dealledger_scraper_v6.py --brokers data/brokers.csv --stale-first --top-n 250

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
import csv as _csv
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
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Load .env (SUPABASE_*, PROXY_*) if python-dotenv is available. Harmless if
# not installed or already exported — real env vars still win.
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Per-broker crawl_run logging. Imported after load_dotenv() above; the module
# reads SUPABASE_* at call time rather than import time, so ordering here is
# not load-bearing either way.
from crawl_run_log import start_run, finish_run

# curl_cffi impersonates a real Chrome TLS/JA3 fingerprint — the same fix that
# unblocked the collector (plain requests gets 403'd). Falls back to
# stdlib requests if unavailable.
try:
    from curl_cffi import requests as cffi_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("⚠️  curl_cffi not installed — falling back to plain requests (403-prone).")
    print("   pip3 install curl_cffi")

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

# ============================================================
# V6.3 MONEY PARSER  —  drop-in replacement (tested 14/14 formats)
# Handles: $2,200,000  $2.200.000  $150K  $1.68M  $500 000  $2.2M
# Rejects: real cents ($4.50) so it never invents sub-$1k "listings"
# ============================================================
MONEY_TOKEN = re.compile(
    r'\$\s*'
    r'(\d{1,3}(?:[.,\s]\d{3})*(?:\.\d{1,2})?|\d+(?:\.\d+)?)'
    r'(?:\s?([KkMmBb])(?![A-Za-z0-9]))?'
)


def _money_to_float(num_str, suffix):
    s = num_str.strip()
    if ',' in s and '.' in s:
        if s.rfind(',') > s.rfind('.'):
            s = s.replace('.', '').replace(',', '.')      # euro: 1.234.567,89
        else:
            s = s.replace(',', '')                        # us: 1,234,567.89
    elif s.count('.') > 1:
        s = s.replace('.', '')                            # 2.200.000
    elif s.count(',') > 1:
        s = s.replace(',', '')                            # 2,200,000
    elif ',' in s:
        after = s.split(',')[1]
        if len(after) == 3 and suffix is None:
            s = s.replace(',', '')                        # ,000 thousands
        elif len(after) <= 2:
            s = s.replace(',', '.')                       # ,99 decimal
        else:
            s = s.replace(',', '')
    elif '.' in s and suffix is None:
        after = s.split('.')[-1]
        if len(after) == 3:
            s = s.replace('.', '')                        # .000 thousands (not cents)
        # len 1-2 -> real cents, leave as-is
    elif ' ' in s:
        s = s.replace(' ', '')                            # space thousands
    try:
        val = float(s)
    except ValueError:
        return None
    if suffix and (',' in num_str or val >= 1000):
        suffix = None
   
    mult = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}.get((suffix or '').lower(), 1)
    return val * mult


def parse_all_prices(text):
    """All dollar amounts in text, normalized to floats."""
    out = []
    for m in MONEY_TOKEN.finditer(text):
        v = _money_to_float(m.group(1), m.group(2))
        if v is not None:
            out.append(v)
    return out


# Link/heading text that is a call-to-action, not a listing title.
GENERIC_LINK_TEXT = {
    "view listing", "view listings", "view details", "view detail", "read more",
    "learn more", "contact us", "contact", "details", "more info", "more information",
    "view", "see details", "see more", "view more", "inquire", "get details",
    "view property", "full details", "request info", "request information",
    "add to favorites", "save", "share", "next", "previous",
    # CTA button labels seen grabbed as titles (companysellers: "MORE DETAILS")
    "more details", "view more details", "see full details", "details here",
    "view business", "view business details", "learn more about this business",
    "click for details", "get more info", "request details", "view now",
}

# href path tokens that strongly indicate a listing DETAIL page.
_DETAIL_HREF_TOKENS = (
    "/listing", "/property", "/properties", "/business", "businesses-for-sale/",
    "/opportunit", "/details", "/detail/", "/biz/", "?listing=", "/for-sale/",
    "/deal", "/company/",
)

# Commercial-real-estate / lease brokers to exclude wholesale — out of scope
# for DealLedger (Main Street business M&A, not CRE). Matched as a substring
# of the netloc.
CRE_LEASE_DOMAINS = {
    "malonecb.com",
    # Residential/commercial real-estate brokerage (Twin Cities), confirmed by
    # fetching the live site 2026-09-08 — no business-for-sale content exists;
    # "Our Listings" is IDX-syndicated property listings. Its detail pages all
    # share one generic template title ("Listings Details - JH Callahan"),
    # which is a symptom of scraping the wrong vertical, not a title-extraction
    # bug to fix — each row has a distinct ListingId (real property listings),
    # just none of them are businesses. 450 rows in listings_direct as of
    # 2026-09-08, all from the 2026-09-02 discovery sweep.
    "jhcallahan.com",
}

# Signature of a commercial-RE / lease listing (rate-per-SF pricing, cap rate,
# NNN, "for lease"). Used to drop lease rows that slip through on other sites
# without excluding real businesses that merely mention square footage.
_LEASE_CRE_RE = re.compile(
    r'for\s+lease|for\s+sublease|annual\s*/\s*sf|\$\s*[\d.,]+\s*/\s*sf|'
    r'\bpsf\b|per\s+sf\b|price\s*/\s*sf|cap\s*rate|\bnnn\b|triple\s*net|'
    r'lease\s*rate|rentable\s+(?:sf|area)',
    re.IGNORECASE,
)


def looks_like_cre_or_lease(text):
    """True for commercial-RE / lease listings (rate-per-SF, cap rate, NNN,
    for-lease). Does NOT fire on a business that merely states square footage."""
    return bool(_LEASE_CRE_RE.search(text))


def _find_listing_iframes(html, base_url):
    """Return candidate iframe src URLs that likely hold listings, best-first.

    Many broker sites (Wix, Squarespace, custom) embed their listings in an
    <iframe> — a third-party feed (listing_feeds, AllBizForSale, etc.) or the
    BizBuySell widget. The outer page has no listing HTML, so detection returns
    NO_PATTERN; the listings live at the iframe's src, a different URL we must
    fetch separately. This finds those srcs and skips obvious non-listing
    iframes (ads, tracking, video, maps, social)."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    SKIP = ("google", "youtube", "vimeo", "facebook", "twitter", "instagram",
            "doubleclick", "googletagmanager", "recaptcha", "gstatic",
            "maps.google", "player.", "ads", "analytics", "hotjar", "intercom",
            "calendly", "hubspot", "linkedin")
    WANT = ("listing", "feed", "business", "search", "properties", "inventory",
            "results", "forsale", "for-sale", "bizbuysell", "widget", "embed")
    for ifr in soup.find_all("iframe"):
        src = ifr.get("src") or ifr.get("data-src") or ""
        if not src:
            continue
        full = urljoin(base_url, src)
        low = full.lower()
        if any(s in low for s in SKIP):
            continue
        # score: prefer srcs whose URL or the iframe id/class hints "listings"
        idc = " ".join([ifr.get("id", "")] + ifr.get("class", [])).lower()
        score = sum(1 for w in WANT if w in low) + sum(2 for w in WANT if w in idc)
        # even unscored same-doc iframes can hold listings; keep with low prio
        out.append((score, full))
    out.sort(key=lambda x: x[0], reverse=True)
    # de-dup preserving order
    seen, urls = set(), []
    for _, u in out:
        if u not in seen:
            seen.add(u); urls.append(u)
    return urls


def _iframe_is_bizbuysell(iframe_url):
    return "bizbuysell.com" in (iframe_url or "").lower()


def best_card_title(element, base_url="", firm_name=None):
    """
    Recover a real listing title from a card element.

    Handles the failure modes seen in the wild:
      - number1businessbroker: first <a> wraps only the image (empty text);
        the title lives in <div class="... name">.
      - appbusinessbrokers: heading is a truncated fragment ("App"); the real
        title comes from the longest text node or the detail-href slug.
    Returns None if no title-like text ≥ 8 chars can be found.
    """
    # ONE RULE: a candidate must not be junk — a CTA ("MORE DETAILS"), a status
    # ("Pending"/"Sold"), a category slug, a register-modal string, or the
    # broker's own firm name. Rejecting it here is what lets us fall through to
    # the detail-href slug, which is where the real business name survives.
    def ok(t):
        return (t and len(t) >= 8 and t.lower() not in GENERIC_LINK_TEXT
                and not is_junk_title(t, firm_name))

    for tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
        for el in element.find_all(tag):
            t = el.get_text(" ", strip=True)
            if ok(t):
                return t[:500]
    for el in element.select("[class*='title'], [class*='name'], [class*='heading']"):
        t = el.get_text(" ", strip=True)
        if ok(t):
            return t[:500]
    for a in element.find_all("a"):
        t = a.get_text(" ", strip=True)
        if ok(t):
            return t[:500]
    # Fallback B: derive a title from the listing detail href / data-url slug
    # (e.g. .../listing/nj-highly-profitable-... -> title). Tried before the
    # text-node fallback because for anonymized-CTA cards the slug is the only
    # place the real name survives.
    slug = _title_from_slug(element, base_url)
    if slug:
        return slug
    # Fallback A: the longest single text node (a real descriptive string,
    # not a CTA or a fragment split across spans).
    longest = ""
    for s in element.stripped_strings:
        if len(s) > len(longest):
            longest = s
    if len(longest) >= 15 and longest.lower() not in GENERIC_LINK_TEXT \
            and not is_junk_listing(longest):
        return longest[:500]
    # Fallback C: card text up to the first financial marker (isolates the name).
    txt = element.get_text(" ", strip=True)
    cut = re.split(r'\s*(?:revenue|price|profit|cash\s*flow|asking|ebitda|sde|\$)',
                   txt, maxsplit=1, flags=re.IGNORECASE)[0].strip()
    cand = cut if len(cut) >= 8 else txt
    return cand[:200] if len(cand) >= 8 else None


def _title_from_slug(element, base_url=""):
    """Turn a listing detail URL slug into a human title, e.g.
    '/listing/ios-privacy-security-utility-app/' -> 'Ios Privacy Security
    Utility App'. Uses a container data-url attr or a detail-looking <a href>."""
    hrefs = []
    if element.get("data-url"):
        hrefs.append(element["data-url"])
    for a in element.find_all("a", href=True):
        hrefs.append(a["href"])
    for href in hrefs:
        href = (href or "").strip()
        if not href or href.startswith("#"):
            continue
        low = href.lower()
        # Prefer hrefs that look like a listing detail page.
        if hrefs.index(href) > 0 and not any(tok in low for tok in _DETAIL_HREF_TOKENS):
            continue
        seg = urlparse(href).path.rstrip("/").split("/")[-1]
        seg = re.sub(r'\.\w+$', '', seg)                 # strip extension
        slug = re.sub(r'[-_+]+', ' ', seg).strip()
        slug = re.sub(r'\s+', ' ', slug)
        if len(slug) >= 8 and not slug.replace(" ", "").isdigit():
            return slug.title()[:200]
    return None


# Anchors that are never a listing's own detail page, even though they're a
# real, live link inside the card. WordPress "fusion-portfolio-post" articles
# (naabconsulting.com and other Avada/Fusion sites) put a "Posted by <author>"
# byline link BEFORE the actual listing link in DOM order — picking the first
# <a> unconditionally grabbed /author/brian/, which is_junk_listing correctly
# flags as junk, silently dropping all 296 real listings on the page instead
# of just fixing the URL.
_NON_DETAIL_HREF_TOKENS = ("/author/", "/category/", "/tag/", "portfolio_category",
                           "/wp-content/", "javascript:", "mailto:", "tel:")


def _best_detail_link(element, base_url=""):
    """The <a href> most likely to be THIS card's own detail page.

    Prefers a link matching _DETAIL_HREF_TOKENS; among the rest, skips known
    non-detail anchors (byline, category/tag archive); falls back to the
    first live link so behavior is unchanged when nothing better is found.
    """
    candidates = []
    for a in element.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        low = href.lower()
        if low.startswith(("javascript:", "mailto:", "tel:")):
            continue
        candidates.append(href)
    if not candidates:
        return None
    for href in candidates:
        if any(tok in href.lower() for tok in _DETAIL_HREF_TOKENS):
            return href
    for href in candidates:
        if not any(tok in href.lower() for tok in _NON_DETAIL_HREF_TOKENS):
            return href
    return candidates[0]


def has_detail_link(element, base_url=""):
    """True if the element links to a plausible listing detail page (not a
    nav item, on-page anchor, mailto/tel, or the site homepage)."""
    root = ""
    if base_url:
        pu = urlparse(base_url)
        root = f"{pu.scheme}://{pu.netloc}".rstrip("/")
    for a in element.find_all("a", href=True):
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        if href.lower().startswith(("javascript:", "mailto:", "tel:")):
            continue
        absu = urljoin(base_url, href) if base_url else href
        if root and absu.rstrip("/") == root:
            continue  # homepage link
        return True
    return False


# Junk-title gate — shared single source of truth with the regression suite
# (scrapers/junk_filter.py). Applied at EXTRACTION time (the durable fix) so
# nav fragments, price fragments, status badges, CTA copy, legal pages, and
# CRE/land never become "listings" in the first place.
try:
    from junk_filter import (is_listing_junk as is_junk_listing, is_sold_or_pending,
                             is_junk_title, title_from_slug)
except ImportError:  # allow import as a package (scrapers.dealledger_scraper_v6)
    from scrapers.junk_filter import (is_listing_junk as is_junk_listing,
                                      is_sold_or_pending, is_junk_title, title_from_slug)


def css_selector(tag, classes):
    """
    Build a CSS-VALID selector from a tag + class list. Utility-CSS class
    names contain characters that are illegal in a bare CSS selector and make
    BeautifulSoup.select() raise ("Malformed selector"): Tailwind responsive/
    state prefixes use ':' (md:flex, hover:bg), fractional widths use '/'
    (w-1/2), some use '.' (p-1.5). Escape them so the selector parses.
    """
    out = []
    for c in classes:
        out.append(re.sub(r'([ :./\[\]()#!,%&>+~*=\'"])', r'\\\1', c))
    return tag + "".join("." + c for c in out)


# Sold / off-market status shown on a card as a class, badge, ribbon, or image
# overlay (NOT just the word appearing in a description — that would false-fire).
_SOLD_TEXT_RE = re.compile(
    r'\b(sold|under[\s-]*contract|sale[\s-]*pending|off[\s-]*market)\b', re.I)
_SOLD_CLASS_RE = re.compile(
    r'\b(sold|under-?contract|sale-?pending|off-?market)\b', re.I)


def looks_sold(element):
    """True if a listing card is marked SOLD / UNDER CONTRACT via a CSS class,
    badge/ribbon/status element, or image overlay. High-precision: it does not
    match the word merely appearing in the description text."""
    for el in [element, *element.find_all(True)]:
        if _SOLD_CLASS_RE.search(" ".join(el.get("class", []))):
            return True
    for el in element.select("[class*='badge'], [class*='ribbon'], [class*='status'], "
                             "[class*='label'], [class*='flag'], [class*='overlay']"):
        if _SOLD_TEXT_RE.search(el.get_text(" ", strip=True)):
            return True
    for img in element.find_all("img"):
        if _SOLD_TEXT_RE.search(f"{img.get('alt','')} {img.get('title','')}"):
            return True
    return False


# Category/browse-grid words that masquerade as listing titles. A grid whose
# "titles" are mostly these is a navigation grid, not businesses for sale.
_CATEGORY_TITLE_WORDS = {
    "restaurants", "retail", "manufacturing", "automotive", "franchise",
    "franchises", "services", "service", "wholesale", "distribution",
    "healthcare", "construction", "technology", "hospitality", "industry",
    "industries", "categories", "category", "browse", "all listings",
    "view all", "read more", "learn more", "contact us", "about us",
    "commercial", "residential", "for lease", "for rent", "home", "next",
    "previous", "search", "filter", "sort by",
}

# US state names/abbrevs used as location-browse tiles (not listings).
_LOCATION_ONLY = {
    "florida", "texas", "california", "georgia", "arizona", "colorado",
    "new york", "north carolina", "south carolina", "tennessee", "virginia",
    "ohio", "illinois", "michigan", "washington", "oregon", "nevada",
}


def _is_nav_selector(selector):
    """True if a CSS selector is a navigation menu / non-listing container.
    WordPress emits li.menu-item-* for nav bars; those repeat and link out, so
    the detector otherwise matches the site's MENU as 'listings' (myersba: 61
    nav links; svnmarinas: 53 'Advisory Services'). A listing site uses custom
    post-type classes (post-type-listing, property, business-listing), never
    menu-item. Reject these outright — a NO_PATTERN broker is better than one
    that writes its nav bar as inventory."""
    if not selector:
        return False
    s = selector.lower()
    NAV_TOKENS = ("menu-item", "menu_item", "nav-item", "navbar", "nav-link",
                  "sub-menu", "submenu", "menu-link", "dropdown", "breadcrumb",
                  "footer", "widget", "sidebar")
    return any(tok in s for tok in NAV_TOKENS)


def validate_listing_set(cards, url=""):
    """
    Gate an extracted set of listings before it is written. Returns
    (verdict, reason) where verdict is 'ok', 'review', or 'reject'.

    Guards against the loosened detector matching a category/browse grid or
    other repeating non-listing structure. The checks look for the signatures
    that separate real business-for-sale listings from navigation tiles:

      - reject: almost no card yields ANY concrete data (no price, no cash
        flow, no state) AND titles look like category/location words — the
        classic "browse by industry / by state" grid false positive.
      - reject: titles are near-duplicates (same string repeated) — a
        template/nav artifact, not distinct listings.
      - review: extracted but weak signal (very few priced, or short titles) —
        write, but flag needs_review for a human sample-check.
      - ok: enough cards carry real listing data.
    """
    n = len(cards)
    if n == 0:
        return "reject", "empty"

    titles = [(c.get("title") or "").strip() for c in cards]
    lowered = [t.lower() for t in titles]

    # Signal density: how many cards carry concrete listing data?
    priced = sum(1 for c in cards if c.get("asking_price"))
    cashflowed = sum(1 for c in cards if c.get("cash_flow"))
    stated = sum(1 for c in cards if c.get("state"))
    with_data = sum(1 for c in cards
                    if c.get("asking_price") or c.get("cash_flow") or c.get("state"))

    # Title quality: fraction that look like category/location browse tiles.
    def _is_category(t):
        if not t or len(t) < 3:
            return True
        if t in _CATEGORY_TITLE_WORDS or t in _LOCATION_ONLY:
            return True
        # single generic word, or "N Businesses" style bucket labels
        if len(t.split()) <= 2 and t in _CATEGORY_TITLE_WORDS:
            return True
        return False

    category_like = sum(1 for t in lowered if _is_category(t))
    category_frac = category_like / n

    # Near-duplicate titles: a template/nav artifact, not real listings.
    uniq_titles = len({t for t in lowered if t})
    dup_frac = 1 - (uniq_titles / n) if n else 0

    # --- reject conditions ---
    # A browse grid: almost no concrete data AND titles are mostly categories.
    if with_data == 0 and category_frac >= 0.5:
        return "reject", f"no data + {category_frac:.0%} category-like titles"
    # Heavily duplicated titles with no data = template repeat, not listings.
    if dup_frac >= 0.7 and with_data == 0:
        return "reject", f"{dup_frac:.0%} duplicate titles, no data"
    # Every card is a category/location word.
    if category_frac >= 0.85:
        return "reject", f"{category_frac:.0%} category/location titles"

    # --- review conditions (write, but flag) ---
    if with_data < max(2, int(0.2 * n)):
        return "review", f"weak signal: only {with_data}/{n} cards carry data"
    if priced == 0 and cashflowed == 0:
        return "review", "no price or cash flow on any card (detail-only site)"

    return "ok", f"{with_data}/{n} cards carry data"


# CLAUDE.md principle 7: "a broker that suddenly yields ten times its usual
# count has broken, not grown." aria.net (343 rows for 3 listings, blocklisted
# but never repaired), pavilionservices.com (1,201 rows — a WP blog's
# placeholder-category posts, discovered 2026-09-08), and
# businessesforsale.nebba.com (the same handful of cards re-crawled as new on
# every page) all share one signature no matter the root cause: a very small
# set of distinct titles carrying a very large row count. validate_listing_set
# catches near-duplicates WITHIN a single check but doesn't gate on this
# ratio directly, and it runs on the whole set at once rather than being
# reusable against a threshold calibrated with known-good input.
#
# Threshold calibrated 2026-09-08 against live listings_direct (principle 8 —
# tested against input that MUST pass, not just the bad case):
#   MUST PASS (real brokers, moderate template repetition):
#     execbb.com                 2,125 rows / 395 titles = 5.4
#     restaurantrealty.com          843 rows / 136 titles = 6.2
#     www.calhouncompanies.com      870 rows / 154 titles = 5.6
#     corbettrestaurantgroup.com  1,111 rows / 160 titles = 6.9
#   MUST REJECT (confirmed junk — index pages / blog posts / pagination loop):
#     www.aria.net                  357 rows /  10 titles = 35.7
#     jhcallahan.com                 (excluded via CRE_LEASE_DOMAINS instead —
#                                     real listings, wrong vertical, not a
#                                     title-repetition problem)
#     www.sagbrokerage.com           96 rows /   2 titles = 48.0
#     businessesforsale.nebba.com  254 rows /   5 titles = 50.8
# 15 sits with room on both sides of that split. See scrapers/test_yield_gate.py.
TITLE_REPETITION_THRESHOLD = 15


def title_repetition_gate(cards, threshold=TITLE_REPETITION_THRESHOLD):
    """
    Reject a batch whose rows/distinct-title ratio is a "broken, not grown"
    signature rather than real inventory. Returns (verdict, reason) in the
    same shape as validate_listing_set: 'ok' or 'reject' (no 'review' tier —
    this pattern has no legitimate soft case, unlike weak-signal cards).
    """
    n = len(cards)
    if n == 0:
        return "ok", "empty"
    titles = {(c.get("title") or "")[:80].strip().lower() for c in cards}
    titles.discard("")
    if not titles:
        return "ok", "no titles to compare"
    ratio = n / len(titles)
    if ratio > threshold:
        return "reject", f"{n} rows / {len(titles)} titles = {ratio:.1f} rows/title (> {threshold})"
    return "ok", f"{ratio:.1f} rows/title"


# Words that only appear in site chrome. A "listing" whose title reads
# "About Toggle child menu Expand" is a navigation dropdown.
#
# missionpeakbrokers.com taught us this: an 18-item nav menu beat a working
# pattern that was extracting 12 real listings, because every nav item has a
# link and 20+ characters of text, so is_listing_element said yes 18 times.
_NAV_WORDS = re.compile(
    r"\b(toggle|child menu|submenu|expand|collapse|skip to|main menu|"
    r"navigation|breadcrumb|search this|filter by|sort by|sign in|log in|"
    r"my account|newsletter|cookie|privacy policy|terms of)\b",
    re.I,
)

# Structural containers that genuinely never hold listing cards.
_CHROME_TAGS = {"nav", "header", "footer", "aside"}

# Class names that mean navigation, matched as WHOLE words only.
#
# The first version of this used a loose prefix match on words including
# "widget", "header" and "filter". That hit `elementor-widget-container`,
# `card-header` and `entry-header` - the standard wrappers around real listing
# cards on WordPress - and rejected genuine listings across whole sites.
# NO_PATTERN went from ~49% to 78% in one run.
#
# So: whole words, a short list, and only two levels up. The precise signal is
# the nav VOCABULARY check below, not class-name guessing.
_CHROME_CLASSES = {
    "nav", "navbar", "navigation", "main-nav", "primary-nav", "menu",
    "main-menu", "site-header", "site-footer", "breadcrumb", "breadcrumbs",
    "sidebar", "offcanvas", "off-canvas", "topbar", "cookie-banner",
}


def _in_site_chrome(element) -> bool:
    """
    Is this element inside real navigation?

    Two levels only. A listing card sits a long way inside a page and its
    distant ancestors are layout containers with arbitrary names - walking
    five levels up and pattern-matching class strings produces far more false
    positives than it prevents.
    """
    node = element
    for _ in range(2):
        node = getattr(node, "parent", None)
        if node is None or not getattr(node, "name", None):
            return False
        if node.name in _CHROME_TAGS:
            return True
        if node.get("role") in ("navigation", "banner", "contentinfo"):
            return True
        classes = {c.lower() for c in (node.get("class") or [])}
        if classes & _CHROME_CLASSES:
            return True
    return False


def is_listing_element(element, base_url=""):
    """
    A container element counts as a listing only if it has a real title AND
    (a real price OR a detail link), and is not site chrome.

    The chrome checks are not optional decoration. Without them a repeated nav
    menu scores higher than a real listing grid, because menus repeat cleanly
    and listings do not.
    """
    text = element.get_text(" ", strip=True)
    if len(text) < 20:
        return False

    # Only the opening of the text - a card whose FIRST words are nav
    # vocabulary is a menu. One of these words appearing later in a long
    # description is not evidence of anything.
    if _NAV_WORDS.search(text[:60]):
        return False
    if element.name in _CHROME_TAGS:
        return False
    if _in_site_chrome(element):
        return False

    title = best_card_title(element, base_url)
    if not title or is_junk_listing(title):
        return False
    if _NAV_WORDS.search(title):
        return False

    has_price = any(v >= 1_000 for v in parse_all_prices(text))
    return has_price or has_detail_link(element, base_url)



def stable_listing_id(title: str, url: str, base_url: str, broker_domain: str) -> str:
    """
    Identity for a listing across runs.

    DO NOT put asking_price in here. It used to be
    sha256(title|asking_price|url), and a price that parsed even slightly
    differently between runs produced a different id, so the upsert on `id`
    never matched and wrote a new row instead of updating. On 29 Aug 2026 that
    turned 316 real quietlight.com listings into 11,243 rows. A price change is
    a price change - it is not a different listing.

    Prefer the detail URL. Fall back to broker + title, never to the index URL,
    or every listing on a page collapses into one identity.
    """
    if url and url != base_url:
        key = url
    else:
        key = f"{broker_domain}|{(title or '').strip().lower()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def positive_or_none(v):
    """
    Normalize a money field to a positive float, else None.

    Guarantees we NEVER store 0 (or negative / non-numeric) for a financial
    field: "no real price" is null, not 0. Applied both when building a
    listing and again at the Supabase write boundary, so a 0 from any source
    — an older scraper variant, or a legacy row being re-upserted — is
    scrubbed before it can pollute listings_direct.
    """
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None

# Max detail pages per broker to avoid hammering servers
MAX_DETAIL_PAGES = 50
DETAIL_DELAY     = (0.5, 1.5)   # seconds between detail fetches

# SPEED (2026-09-16). Runs since 12 Sep stopped hitting 100 brokers in the
# 285-minute window (run #162: 34 brokers), so every run ended on the timeout,
# lost everything after its last flush, and never wrote crawl_failures.
#
# BROKER_TIME_BUDGET: seconds one broker may take before pagination and
#   detail fetching stop and what was collected is written. One slow site can
#   no longer eat the run.
# SKIP_KNOWN_DETAILS: don't re-fetch detail pages for listings already in
#   listings_direct with enriched data; carry the stored fields forward
#   instead (the upsert writes every column, so they must be carried or they
#   would be overwritten with card-only values).
BROKER_TIME_BUDGET = int(os.environ.get("BROKER_TIME_BUDGET", "480"))
SKIP_KNOWN_DETAILS = os.environ.get("SKIP_KNOWN_DETAILS", "1") != "0"

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

# DataImpulse residential proxy — credentials come from the environment,
# NEVER hardcoded. Provide either:
#   PROXY_USER + PROXY_PASS  (+ optional PROXY_HOST, default gw.dataimpulse.com:823)
# or a full PROXY_URL:  http://user:pass@host:port
#
# DataImpulse routing params are baked into the username at session-build time:
#   __cr.us       -> force US exit IPs (foreign residential IPs draw harsher
#                    Akamai/Cloudflare treatment on US sites)
#   ;sessid.<id>  -> sticky session: hold ONE exit IP instead of rotating per
#                    request (rotation caused curl(28) timeouts + broke cookie
#                    warming). A fresh sessid gives a fresh IP on re-warm.
def _load_proxy_creds():
    """(user, password, host) from env, or (None, None, host)."""
    user = os.environ.get("PROXY_USER", "").strip()
    pw   = os.environ.get("PROXY_PASS", "").strip()
    host = os.environ.get("PROXY_HOST", "gw.dataimpulse.com:823").strip()
    if not (user and pw):
        raw = os.environ.get("PROXY_URL", "").strip()
        if raw:
            p = urlparse(raw)
            user = p.username or ""
            pw   = p.password or ""
            if p.hostname:
                host = f"{p.hostname}:{p.port}" if p.port else p.hostname
    return (user or None, pw or None, host)

PROXY_USER, PROXY_PASS, PROXY_HOST = _load_proxy_creds()
PROXY_AVAILABLE = bool(PROXY_USER and PROXY_PASS)

# Retry/backoff tuning.
MAX_403_RETRIES = 3      # re-warm + retry this many times on a 403/429/503
BACKOFF_BASE    = 3.0    # backoff = 3s, 6s, 12s (+ jitter)
REQ_TIMEOUT     = 25     # per-request timeout; a dead proxy IP fails fast


def build_proxy_url(sessid):
    """DataImpulse proxy URL with US sticky session, or None if unconfigured."""
    if not PROXY_AVAILABLE:
        return None
    return f"http://{PROXY_USER}__cr.us;sessid.{sessid}:{PROXY_PASS}@{PROXY_HOST}"


BLOCKLIST_DOMAINS = {
    "aria.net",
    # Confirmed no real listings page (CLAUDE.md principle 15). Its /opportunities
    # page is "seeking to acquire" buyer-side placeholders, not for-sale listings —
    # a past crawl produced 1,201 active rows of duplicate category-placeholder
    # junk ("Manufacturing Company", "Distribution Company", ... each repeated
    # with an identical suspiciously-round price). See claude/junk-rules-proposed.sql.
    "pavilionservices.com",
    # Confirmed junk, same "broken, not grown" shape (CLAUDE.md principle 7),
    # measured 2026-09-08: 96 rows / 2 distinct titles ("SAG Hospitality
    # Brokerage a UD Consulting Company" — site header text, not a listing —
    # and "Buy or Sell a Liquor License" — a service/category page). Present
    # in data/brokers_clean.csv (row for https://www.sagbrokerage.com/for-sale/)
    # — the blocklist check strips "www." before comparing, so this catches it.
    "sagbrokerage.com",
    # Confirmed junk: 254 rows / 5 distinct titles, worst locally-sampled case
    # of any domain here — 6,318 rows collapse to a handful of literal titles
    # repeated ~950x each. Same card re-crawled as new on every page, aria's
    # exact pagination-loop signature. Present in data/brokers_clean.csv under
    # two different subpaths (Ohio and Texas) — this is the full netloc, not
    # just "nebba.com", since businessesforsale.nebba.com never carries a
    # "www." prefix to strip.
    "businessesforsale.nebba.com",
}


# Marketplace aggregators. CLAUDE.md standing constraint: "Direct broker
# sourcing is the moat ... Marketplace aggregators are not a source." Matched
# on the domain OR any subdomain (us.businessesforsale.com,
# broker.bizbuysell.com), so a marketplace profile page pasted into
# data/brokers_clean.csv is skipped instead of scraped. Added 2026-09-15 after
# a bizbuysell.com broker-profile URL and four businessesforsale.com agent
# pages were found in the CSV.
MARKETPLACE_DOMAINS = {
    "bizbuysell.com",
    "bizquest.com",
    "loopnet.com",
    "businessesforsale.com",
}


# Owned by the specialized pipeline (scrapers/specialized_scrapers.py).
# V6 must never scrape these: the specialized scrapers already cover them
# properly, so a generic attempt is wasted budget AND risks writing worse
# rows over good ones. Matched on the domain OR any subdomain, because FCBB
# alone has ~10 city sites (pittsburgh.fcbb.com, atlantametro.fcbb.com, ...).
# Kept in sync with scrape_specialized_broker() dispatch.
SPECIALIZED_DOMAINS = {
    "execbb.com",                    # LarryBodnerScraper
    "linkbusiness.com",              # LinkBusinessScraper
    "murphybusiness.com",            # MurphyScraper
    "hedgestone.com",                # HedgestoneScraper
    "tworld.com",                    # TransworldScraper
    "sunbeltnetwork.com",            # SunbeltScraper
    "vrbbusa.com",                   # VRScraper
    "vrbusinessbrokers.com",         # VRScraper
    "fcbb.com",                      # FCBBScraper (+ all *.fcbb.com)
    "wesellrestaurants.com",         # WeSellRestaurantsScraper
    "vestedbb.com",                  # VestedScraper
    "routesforsale.net",             # RoutesForSaleScraper (exact site only —
                                     # commercialroutesforsale.com and
                                     # deliveryroutesforsale.com are DIFFERENT
                                     # companies; V6 must keep scraping those)
}


# Filled at startup from the broker_block table (DealLedgerScraper.__init__),
# so a domain blocked in the database is skipped without a code change.
# Found 2026-09-15: idx.michelephillipsrealtor.com was in broker_block yet
# wrote 500 rows, because nothing in this file read that table.
RUNTIME_BLOCKED: set = set()


def is_blocked(domain: str) -> bool:
    """
    True if this domain must never be scraped for business-for-sale listings:
    known junk (BLOCKLIST_DOMAINS), wrong vertical (CRE_LEASE_DOMAINS), or
    owned by the specialized pipeline (SPECIALIZED_DOMAINS — matched by
    domain-or-subdomain, since FCBB alone has ~10 city sites).

    Single source of truth for BOTH _load_brokers() (the CSV batch path) and
    the --broker single-URL path in main(). Those two used to disagree — the
    --broker branch built its broker dict inline and never called this check
    at all — which is exactly how pavilionservices.com, www.sagbrokerage.com
    and businessesforsale.nebba.com reached production: a manual --broker
    test run, not the daily CSV rotation. Confirmed from first_seen
    timestamps 2026-09-08 — all three wrote a full batch within minutes, an
    hour or more away from the 14:00 UTC cron, from domains that have never
    appeared in data/brokers_clean.csv in any commit.
    """
    domain = (domain or "").lower()
    bare = domain[4:] if domain.startswith("www.") else domain
    return (bare in BLOCKLIST_DOMAINS
            or bare in RUNTIME_BLOCKED
            or bare in CRE_LEASE_DOMAINS
            or any(bare == d or bare.endswith("." + d)
                   for d in SPECIALIZED_DOMAINS | MARKETPLACE_DOMAINS))


# Domains known to hard-block — start them on the proxy immediately.
PROXY_DOMAINS = {
    # Discovered via URL health sweep 2026-07-16 — these return 401/403/406/429 direct
    "hedgestone.com",
    "quietlight.com",                    # 403 — 423 listings at risk
    "progressivepracticesales.com",      # 403 — 210 listings
    "restaurantrealty.com",              # 401 — 126 listings
    "bristolgrouponline.com",
    "myersba.com",
    "krbrokers.com",
    "capitalbbw.com",
    "businessmodificationgroup.com",
    "cbcworldwide.com",
    "interbloomgroup.com",
    "firstsourcebb.com",
    "atlantahomes.us",
    "beehivebusinessbrokers.com",
    "prime100businessbrokers.com",
    "thedynastyba.com",
    "toddbusinesssolutions.com",
    "genuinebusinessadvisors.com",
    "autocenter-sales.com",
    "rambizgroup.com",
    "cabi.coloradobusinesses.com",
    "businessesforsale.nebba.com",
    "kmfbusinessadvisors.dealrelations.com",
    "realtyall.com",
    "affordablebusinessconcepts.com",
    "bisonbusiness.com",                 # 429 rate limited
    "poolroutebrokers.com",              # 406
    # Add more as you discover them
}

def _needs_proxy(domain: str) -> bool:
    """Return True if this domain should start on the proxy."""
    if not PROXY_AVAILABLE:
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

    @staticmethod
    def _proven(entry) -> int:
        """
        Evidence that this pattern extracts real listings.

        Deliberately NOT `count`. In the current schema `count` is how many
        elements the selector matched, which is not evidence of anything - a
        nav menu matches 18 elements cleanly. `score` comes out of
        _score_group, so those elements at least passed is_listing_element.
        `total_listings` in the older schema is real historical yield and is
        the strongest signal available.
        """
        if not isinstance(entry, dict):
            return 0
        if entry.get("total_listings"):
            return int(entry["total_listings"])
        if entry.get("score"):
            return int(entry["score"])
        return 0

    def store(self, domain, pattern):
        """
        Keep the better pattern, not the newer one.

        Both schemas live in this file - an older one keyed on
        pattern/success_count/total_listings, and the current
        container_selector/count/score. Whichever ran last used to win
        outright, which is how missionpeakbrokers.com lost a pattern
        extracting 12 real listings to a nav menu that matched 18 elements.

        Element count is not evidence. A new pattern has to at least match the
        proven yield of the one it replaces.
        """
        existing = self.patterns.get(domain)
        if existing:
            old_yield = self._proven(existing)
            new_yield = self._proven(pattern)
            if old_yield >= 3 and new_yield < old_yield:
                print(f"   ↩︎  keeping cached pattern for {domain} "
                      f"({old_yield} listings) over new guess ({new_yield})")
                return

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

    # A container qualifies as a listing grid only if:
    #  - at least MIN_LISTING_ELEMENTS of its repeated elements are genuine
    #    listings (title AND (price OR detail-link)), AND
    #  - EITHER at least MIN_PRICED of them carry a price (classic priced grid),
    #    OR most of them link to detail pages (price-on-detail-page grid).
    # Rationale (2026-08): many broker sites show NO price on the index — price
    # lives on the detail page. The old hard MIN_PRICED floor discarded those
    # entirely (a large share of NO_PATTERN). We now accept a price-less grid
    # IF its cards are detail-linked, which still excludes nav bars / filter
    # widgets / lease repeaters (those don't have per-item detail links).
    MIN_LISTING_ELEMENTS = 3
    MIN_PRICED = 3
    MIN_LINKED_FRAC = 0.6   # if unpriced, ≥60% of cards must link to a detail page
    SAMPLE = 15

    @classmethod
    def _score_group(cls, elements, url):
        """Count how many sampled elements are real listings; return (n_listing,
        n_priced) or None if the group doesn't clear the listing bar."""
        n_listing = 0
        n_priced = 0
        n_linked = 0
        for el in elements[:cls.SAMPLE]:
            if is_listing_element(el, url):
                n_listing += 1
                if any(v >= 1_000 for v in
                       parse_all_prices(el.get_text(" ", strip=True))):
                    n_priced += 1
                if has_detail_link(el, url):
                    n_linked += 1
        if n_listing < cls.MIN_LISTING_ELEMENTS:
            return None
        # Classic priced grid: enough cards carry a price.
        if n_priced >= cls.MIN_PRICED:
            return n_listing, n_priced
        # Price-on-detail-page grid: no price floor met, but the cards are
        # genuine detail-linked listings, not nav/filter noise.
        if n_linked >= max(cls.MIN_LISTING_ELEMENTS,
                           int(cls.MIN_LINKED_FRAC * n_listing)):
            return n_listing, n_priced
        return None

    @classmethod
    def detect(cls, html, url):
        soup = BeautifulSoup(html, "html.parser")
        candidates = []

        # Strategy 1: repeated classed elements that are genuine listings
        for tag in ["div", "article", "li", "tr", "a", "section"]:
            class_groups = defaultdict(list)
            for el in soup.find_all(tag):
                classes = el.get("class", [])
                if classes:
                    class_groups[css_selector(tag, classes)].append(el)

            for selector, group in class_groups.items():
                if len(group) < cls.MIN_LISTING_ELEMENTS:
                    continue
                scored = cls._score_group(group, url)
                if scored:
                    n_listing, n_priced = scored
                    candidates.append({
                        "container_selector": selector,
                        "count": len(group),
                        # Prefer selectors with more real, priced listings.
                        "score": n_listing + n_priced,
                        "sample_text": group[0].get_text(separator=" ", strip=True)[:200],
                    })

        # Strategy 1b: rows/cards with PER-ITEM UNIQUE classes. Platforms like
        # dealrelations emit <tr class="element_row74162"> — every row a
        # different class, so Strategy 1 sees no repetition. Group by the class
        # token with its trailing id suffix stripped, and select via a
        # [class*="prefix"] attribute selector.
        for tag in ["tr", "div", "article", "li"]:
            norm_groups = defaultdict(list)
            for el in soup.find_all(tag):
                classes = el.get("class", [])
                if not classes:
                    continue
                norm = tuple(c for c in
                             (re.sub(r'[-_]?[0-9a-fA-F]{2,}$', '', c) for c in classes)
                             if len(c) >= 4)
                if norm:
                    norm_groups[norm].append(el)
            for norm, group in norm_groups.items():
                if len(group) < cls.MIN_LISTING_ELEMENTS:
                    continue
                # Only relevant when the raw classes really do differ per item.
                raw = {css_selector(tag, el.get("class", [])) for el in group[:8]}
                if len(raw) < 2:
                    continue
                scored = cls._score_group(group, url)
                if scored:
                    n_listing, n_priced = scored
                    stable = max(norm, key=len)
                    candidates.append({
                        "container_selector": f'{tag}[class*="{stable}"]',
                        "count": len(group),
                        "score": n_listing + n_priced,
                        "sample_text": group[0].get_text(separator=" ", strip=True)[:200],
                    })

        # Strategy 2: known selectors (small boost for being a known shape)
        for selector in [
            "div.listing", "div.listing-item", "div.property-listing",
            "article.listing", "div.business-listing", "div.result",
            "div.search-result", "div.card", "div.listing-card",
            "tr.listing-row", "div.property-card", "li.listing",
            "div.item", "div.post", "div.entry",
        ]:
            try:
                elements = soup.select(selector)
                if len(elements) >= cls.MIN_LISTING_ELEMENTS:
                    scored = cls._score_group(elements, url)
                    if scored:
                        n_listing, n_priced = scored
                        candidates.append({
                            "container_selector": selector,
                            "count": len(elements),
                            "score": n_listing + n_priced + 5,
                            "sample_text": elements[0].get_text(separator=" ", strip=True)[:200],
                        })
            except Exception:
                pass

        if not candidates:
            return None
        # Drop navigation/menu/footer/widget containers: WordPress nav bars
        # repeat and link out, so they otherwise win as "listings" (myersba's
        # 61 menu links, svnmarinas' 53 'Advisory Services'). Better NO_PATTERN
        # than writing a site's nav bar as inventory.
        candidates = [c for c in candidates
                      if not _is_nav_selector(c.get("container_selector", ""))]
        if not candidates:
            return None
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[0]



def _sane_financials(price, cash_flow, revenue):
    """
    One number on a card cannot be three different financials.

    extract_price() takes the largest price on the card and _money_near()
    falls back to "the value just before the label", so a single page-level
    figure satisfies all three extractors at once. That is how
    companysellers.com wrote $1,000,000 as asking price AND cash flow AND
    revenue on 455 listings (73% of its index), and how 828 rows across 131
    domains ended up with three identical numbers (audit 2026-09-17).

    Rules, in order:
      * a value repeated across fields survives only as the asking price,
        which is the figure brokers publish most reliably;
      * cash flow at or above revenue means one of the two labels was
        misread — "profit" and "net income" match far more loosely than
        "gross revenue", so cash flow is the one dropped.

    Dropping a field is the right failure here: a missing number is honest,
    a wrong one is not.
    """
    if cash_flow is not None and cash_flow == price:
        cash_flow = None
    if revenue is not None and revenue == price:
        revenue = None
    if cash_flow is not None and revenue is not None and cash_flow == revenue:
        cash_flow = None
    if (cash_flow is not None and revenue is not None
            and revenue > 0 and cash_flow >= revenue):
        cash_flow = None
    return price, cash_flow, revenue



# Residential / IDX property feeds. Blocking these by DOMAIN only ever catches
# the instance you already found: idx.michelephillipsrealtor.com was blocked on
# 13 Sep and siriusrealtyservices.com — same IDX template, 1,411 rows, SC
# street-address titles — took its place within two days (audit 2026-09-17).
# So the test runs on the CONTENT of the card. An MLS number, a bedroom or
# bath count, or a "City, ST - 123 Street Name" title is a house, not a
# business for sale. Mirrored in SQL as public.is_residential_listing().
_MLS_RE      = re.compile(r"mls\s*#|\bmls\s+\d{5,}", re.I)
_BEDBATH_RE  = re.compile(r"\d+\s+(?:bed(?:room)?s?|full\s+baths?|half\s+baths?)\b", re.I)
_ADDR_TITLE  = re.compile(r"^[^,]{2,40},\s*[A-Za-z]{2}\s*[-\u2013]\s*\d+\s+\w", re.I)
_BDBA_TITLE  = re.compile(r"\b\d+\s*(?:bd|br|beds?|bedrooms?)\b.*\b(?:ba|baths?)\b", re.I)


def looks_residential(title: str, text: str) -> bool:
    """True when a card is a residential property listing, not a business."""
    t = title or ""
    body = text or ""
    return bool(_ADDR_TITLE.search(t) or _BDBA_TITLE.search(t)
                or _MLS_RE.search(body) or _BEDBATH_RE.search(body))


# ============================================================
# LISTING EXTRACTOR (V6)
# ============================================================

class ListingExtractor:

    @staticmethod
    def extract_price(text):
        vals = [v for v in parse_all_prices(text) if 1_000 <= v <= 100_000_000]
        return max(vals) if vals else None

    @staticmethod
    def _money_near(text, keywords):
        """
        Value associated with a financial label. Looks FORWARD first
        ("Profit: $90,952"), then falls back to the value just BEFORE the
        label ("$300,000 Cash Flow"). Forward-first is what lets us read
        "Profit:" as its own value instead of grabbing the preceding
        "Revenue: $161,605".
        """
        low = text.lower()
        for kw in keywords:
            idx = low.find(kw)
            if idx < 0:
                continue
            after = text[idx + len(kw): idx + len(kw) + 80]
            vals = [v for v in parse_all_prices(after) if v >= 1_000]
            if vals:
                return vals[0]
            before = text[max(0, idx - 60): idx]
            vals = [v for v in parse_all_prices(before) if v >= 1_000]
            if vals:
                return vals[-1]      # nearest value preceding the label
        return None

    @classmethod
    def extract_cash_flow(cls, text):
        return cls._money_near(text, [
            "cash flow", "sde", "ebitda", "seller discretionary", "net income",
            "owner benefit", "adjusted earnings", "owner's benefit", "profit",
        ])

    @classmethod
    def extract_revenue(cls, text):
        return cls._money_near(text, [
            "gross revenue", "gross sales", "annual sales", "total sales",
            "gross income", "annual revenue", "revenue",
        ])

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

        # Drop commercial-real-estate / lease brokers — out of scope for
        # DealLedger (Main Street business M&A, not CRE). malonecb is a
        # moodyscre-backed CRE site (prices as "$18.50 Annual/SF").
        domain = urlparse(base_url).netloc.lower()
        if any(d in domain for d in CRE_LEASE_DOMAINS):
            return None
        if looks_like_cre_or_lease(text):
            return None

        # Skip nav/hero/filter blocks: a real listing has a title AND
        # (a price OR a detail link). This is what stops junk-container
        # selectors from emitting empty-title, price-less rows.
        if not is_listing_element(element, base_url):
            return None

        best_href = _best_detail_link(element, base_url)
        detail_url = urljoin(base_url, best_href) if best_href else None

        # Don't treat pagination/anchor links as detail URLs
        if detail_url:
            d = detail_url.rstrip("/")
            b = base_url.rstrip("/")
            if d == b or "#" in d.split("?")[0][-5:]:
                detail_url = None

        title = best_card_title(element, base_url, firm_name=broker_name) or text[:100]
        # ONE RULE: a junk/blank title falls back to the listing URL slug, which
        # is where the real business name survives on CTA-titled cards.
        if is_junk_title(title, broker_name):
            title = title_from_slug(detail_url) or title

        # Junk gate — nav fragment, price fragment, status badge, CTA, legal
        # page, CRE/land: never emit these as a listing.
        if is_junk_listing(title, detail_url or base_url):
            return None

        # Residential/IDX row gate — see looks_residential() above.
        if looks_residential(title, text):
            return None

        asking_price = positive_or_none(cls.extract_price(text))
        cash_flow    = positive_or_none(cls.extract_cash_flow(text))
        revenue      = positive_or_none(cls.extract_revenue(text))
        asking_price, cash_flow, revenue = _sane_financials(
            asking_price, cash_flow, revenue)
        location     = LocationExtractor.extract(text)
        vertical     = cls.classify_vertical(text)

        _url = detail_url or base_url
        listing_id = stable_listing_id(
            title, _url, base_url, urlparse(base_url).netloc
        )

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
            "url":           _url,
            "url_is_listing_specific": bool(detail_url) and detail_url != base_url,
            "broker_name":   broker_name,
            "broker_domain": urlparse(base_url).netloc,
            "description":   text[:1000],
            # Sold/under-contract deals are imported as status='sold' (from a
            # badge/class/ribbon OR a title marker) so they don't inflate the
            # active count or get DOM'd as live inventory.
            "status":        "sold" if (looks_sold(element)
                                        or is_sold_or_pending(title)) else "active",
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
            listing["asking_price"] = positive_or_none(cls.extract_price(full_text))
        if not listing.get("cash_flow"):
            listing["cash_flow"] = positive_or_none(cls.extract_cash_flow(full_text))
        if not listing.get("revenue"):
            listing["revenue"] = positive_or_none(cls.extract_revenue(full_text))

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
# API HANDLERS  —  JS-loaded sites whose listings come from a JSON endpoint
# ============================================================
# Some brokers render an empty HTML shell and load listings from an XHR/JSON
# API. Rather than drive a headless browser, we detect the platform from the
# page source and hit the JSON endpoint directly. Each handler returns a list
# of listing dicts in the same schema as ListingExtractor.from_card.

def _make_api_listing(base_url, broker_name, title, detail_url=None,
                      asking_price=None, cash_flow=None, revenue=None,
                      city=None, state=None, description=None):
    now = datetime.now(timezone.utc).isoformat()
    url = detail_url or base_url
    listing_id = stable_listing_id(
        title, url, base_url, urlparse(base_url).netloc)
    text = f"{title or ''} {description or ''}"
    return {
        "id":            listing_id,
        "title":         (title or "")[:500],
        "asking_price":  positive_or_none(asking_price),
        "cash_flow":     positive_or_none(cash_flow),
        "revenue":       positive_or_none(revenue),
        "city":          city,
        "state":         state,
        "vertical":      ListingExtractor.classify_vertical(text),
        "url":           url,
        "url_is_listing_specific": bool(detail_url) and detail_url != base_url,
        "broker_name":   broker_name,
        "broker_domain": urlparse(base_url).netloc,
        "description":   (description or title or "")[:1000],
        "first_seen":    now,
        "last_seen":     now,
    }


def _state_from_location(loc):
    """Pull a 2-letter state out of a 'City, ST, US' style string."""
    if not loc:
        return None
    for part in (p.strip() for p in str(loc).split(",")):
        if len(part) == 2 and part.upper() in US_STATES:
            return part.upper()
    m = LocationExtractor.extract(str(loc))
    return m["state"] if m else None


def _handler_tupelosmb(url, html, broker_name, fetcher):
    """
    Tupelo SMB CRM platform. The broker page loads listings from
    crm.tupelosmb.com/api/public/listings?organizationId=<cuid>&take=&skip=
    The org cuid is the only 'cl…' id present in the page source.
    """
    m = re.search(r'\b(cl[a-z0-9]{20,})\b', html)
    if not m:
        return None
    org_id = m.group(1)
    out, skip, take = [], 0, 100
    while True:
        try:
            r = fetcher.session.get(
                "https://crm.tupelosmb.com/api/public/listings"
                f"?organizationId={org_id}&take={take}&skip={skip}",
                timeout=REQ_TIMEOUT)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break
        items = data.get("listings", []) if isinstance(data, dict) else []
        total = data.get("totalCount", 0) if isinstance(data, dict) else 0
        for it in items:
            if str(it.get("status", "")).upper() not in ("ACTIVE", ""):
                continue
            loc = it.get("locationStringShort") or it.get("locationString") or ""
            out.append(_make_api_listing(
                url, broker_name,
                title=it.get("headline"),
                asking_price=it.get("askingPrice"),
                cash_flow=it.get("cashFlow"),
                revenue=it.get("revenue"),
                state=_state_from_location(loc),
                city=(loc.split(",")[0].strip() if loc else None),
                description=it.get("headline")))
        skip += take
        if not items or skip >= total:
            break
    return out or None


# Registry: substring found in page source -> handler(url, html, broker_name, fetcher)
API_HANDLERS = [
    ("tupelosmb.com", _handler_tupelosmb),
]


def extract_via_api(url, html, broker_name, fetcher):
    """If the page is a known JS platform with a JSON feed, pull listings
    from the API directly. Returns a list of listing dicts, or None."""
    low = (html or "").lower()
    for signature, handler in API_HANDLERS:
        if signature in low:
            try:
                listings = handler(url, html, broker_name, fetcher)
                if listings:
                    clean = [l for l in listings
                             if not is_junk_listing(l.get("title"), l.get("url"))]
                    if clean:
                        return clean
            except Exception:
                pass
    return None


# ============================================================
# PAGE FETCHER (with proxy support)
# ============================================================

class PageFetcher:

    def __init__(self):
        # One sticky proxy session-id per run (fresh IP on each re-warm).
        self._sessid = f"dl{random.randint(100000, 999999)}"
        self.session = self._new_session()
        self.playwright   = None
        self.browser_plain = None   # no proxy
        self.browser_proxy = None   # with proxy
        # Domains that 403'd once — subsequent pages go straight to the proxy.
        self._proxy_domains_runtime = set()

    @staticmethod
    def _new_session():
        """A curl_cffi session impersonating a current Chrome (chrome131).
        chrome124 went stale ~6/30/2026 and started drawing Akamai 403s."""
        if HAS_CURL_CFFI:
            return cffi_requests.Session(impersonate="chrome131")
        return requests.Session()

    def _proxy_dict(self):
        """requests-style proxy dict for the current sticky session."""
        p = build_proxy_url(self._sessid)
        if not p:
            return None
        return {"http": p, "https": p}

    def _playwright_proxy(self):
        """Playwright proxy config (server + separate auth)."""
        if not PROXY_AVAILABLE:
            return None
        return {
            "server":   f"http://{PROXY_HOST}",
            "username": f"{PROXY_USER}__cr.us;sessid.{self._sessid}",
            "password": PROXY_PASS,
        }

    def _rewarm(self, url, use_proxy):
        """Fresh session + fresh sticky IP, then hit the site root so the
        anti-bot layer issues fresh sensor cookies against this fingerprint."""
        self._sessid = f"dl{random.randint(100000, 999999)}"
        self.session = self._new_session()
        try:
            pu = urlparse(url)
            root = f"{pu.scheme}://{pu.netloc}/"
            self.session.get(
                root, timeout=REQ_TIMEOUT, allow_redirects=True,
                proxies=self._proxy_dict() if use_proxy else None,
                headers={"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                         "Accept-Language": "en-US,en;q=0.9",
                         "Referer": "https://www.google.com/"},
            )
        except Exception:
            pass

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

    def _headers(self):
        # No User-Agent: curl_cffi sets one matching its TLS fingerprint.
        # (A mismatched UA is itself a bot signal.) Plain-requests fallback
        # gets a UA so it isn't obviously headless.
        h = {
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         "https://www.google.com/",
        }
        if not HAS_CURL_CFFI:
            h["User-Agent"] = random.choice(USER_AGENTS)
        return h

    def fetch_requests(self, url, timeout=REQ_TIMEOUT, use_proxy=False):
        """Single-shot GET (no retry). Kept for callers that want raw fetch."""
        resp = self.session.get(
            url, headers=self._headers(), timeout=timeout,
            allow_redirects=True,
            proxies=self._proxy_dict() if use_proxy else None,
        )
        resp.raise_for_status()
        return resp.text

    def _fetch_http(self, url, use_proxy=False, timeout=REQ_TIMEOUT):
        """
        GET with anti-block handling: on 403/429/503, escalate to the proxy
        (if not already on it), re-warm the session for fresh cookies, back
        off, and retry up to MAX_403_RETRIES. Raises requests.HTTPError with
        the last response attached when a block survives all retries, so the
        caller can classify it as HTTP_403.
        """
        domain = urlparse(url).netloc
        if domain in self._proxy_domains_runtime:
            use_proxy = True
        resp = None
        for attempt in range(MAX_403_RETRIES + 1):
            try:
                resp = self.session.get(
                    url, headers=self._headers(), timeout=timeout,
                    allow_redirects=True,
                    proxies=self._proxy_dict() if use_proxy else None,
                )
            except Exception:
                # Dead proxy IP / timeout — re-warm onto a fresh IP and retry.
                if attempt < MAX_403_RETRIES:
                    time.sleep(BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1.5))
                    self._rewarm(url, use_proxy)
                    continue
                raise

            if resp.status_code == 200:
                return resp.text

            if resp.status_code in (403, 429, 503):
                # First block on a direct fetch → escalate this domain to proxy.
                if not use_proxy and PROXY_AVAILABLE:
                    use_proxy = True
                    self._proxy_domains_runtime.add(domain)
                if attempt < MAX_403_RETRIES:
                    backoff = BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1.5)
                    time.sleep(backoff)
                    self._rewarm(url, use_proxy)
                    continue
                break  # exhausted retries → fall through to raise

            # Other non-200 (404/500/…): not a block, don't burn retries.
            resp.raise_for_status()

        raise requests.HTTPError(
            f"HTTP {resp.status_code if resp is not None else '???'} after "
            f"{MAX_403_RETRIES} proxy retries", response=resp)

    def fetch_playwright(self, url, timeout=30000, use_proxy=False):
        if not HAS_PLAYWRIGHT:
            raise RuntimeError("Playwright not installed")
        browser = self._get_browser(use_proxy)
        page = browser.new_page(user_agent=random.choice(USER_AGENTS))
        try:
            page.goto(url, timeout=timeout, wait_until="domcontentloaded")
            # Give XHR/fetch-driven listing widgets time to populate.
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass
            # Scroll to trigger lazy-loaded / infinite-scroll listing cards.
            for _ in range(4):
                page.mouse.wheel(0, 4000)
                page.wait_for_timeout(1200)
            page.wait_for_timeout(1500)
            return page.content()
        finally:
            page.close()

    @staticmethod
    def _has_listing_signal(html):
        """
        True if the fetched HTML already contains listing content (dollar
        amounts or listing keywords). JS-shell pages return a large but
        content-free HTML — those should escalate to Playwright even though
        they clear the size bar.
        """
        if not html:
            return False
        # Check VISIBLE text, not raw HTML — embedded JSON/CSS/script blobs
        # (Wix warmup data, Next.js props) are full of "$" tokens that would
        # otherwise mask a content-free JS shell as if it had listings.
        soup = BeautifulSoup(html, "html.parser")
        for junk in soup(["script", "style", "template", "noscript"]):
            junk.decompose()
        text = soup.get_text(" ", strip=True)
        prices = parse_all_prices(text)
        if len(prices) >= 2:
            return True
        low = text.lower()
        return bool(prices) and any(kw in low for kw in LISTING_KEYWORDS[:6])

    def fetch(self, url, use_proxy=False):
        requests_html = None
        try:
            # curl_cffi + proxy-escalation + 403 re-warm/retry.
            requests_html = self._fetch_http(url, use_proxy=use_proxy)
            # Only trust the HTML if it actually carries listing content.
            # A big-but-empty JS shell falls through to Playwright.
            if len(requests_html) > 3000 and self._has_listing_signal(requests_html):
                return requests_html, "requests"
        except requests.HTTPError as e:
            # Reached only after proxy retries were exhausted → genuinely
            # blocked. Propagate 403 for failure classification.
            if e.response is not None and getattr(e.response, "status_code", None) == 403:
                raise
        except Exception:
            pass

        if HAS_PLAYWRIGHT:
            try:
                # If the domain has been blocking, render through the proxy too.
                pw_proxy = use_proxy or (urlparse(url).netloc in self._proxy_domains_runtime)
                pw_html = self.fetch_playwright(url, use_proxy=pw_proxy)
                # Prefer whichever rendering actually has listing content.
                if self._has_listing_signal(pw_html) or not requests_html:
                    return pw_html, "playwright"
            except Exception:
                pass

        if requests_html and len(requests_html) > 3000:
            return requests_html, "requests"
        return self._fetch_http(url, use_proxy=True), "requests"

    def close(self):
        # After the SIGINT from the 285-minute guard the Playwright driver is
        # often already gone, and close() raised "Connection closed while
        # reading from the driver" (run #162). Shutdown must never fail.
        for label, closer in (("browser_plain", lambda: self.browser_plain and self.browser_plain.close()),
                              ("browser_proxy", lambda: self.browser_proxy and self.browser_proxy.close()),
                              ("playwright",    lambda: self.playwright and self.playwright.stop())):
            try:
                closer()
            except Exception as e:
                print(f"⚠️  {label} close ignored: {e}")


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

    # Query params (and path form) used for numbered pagination when a site
    # has NO explicit next-link (numbered pages, JS pagination, "Load More").
    _PAGE_PARAMS   = ("page", "paged", "p", "pg", "wpv_paged")
    _OFFSET_PARAMS = ("offset", "start")

    @staticmethod
    def _with_param(base_url, param, value):
        pu = urlparse(base_url)
        q = {k: list(v) for k, v in parse_qs(pu.query, keep_blank_values=True).items()}
        q[param] = [str(value)]
        return urlunparse((pu.scheme, pu.netloc, pu.path, pu.params,
                           urlencode(q, doseq=True), pu.fragment))

    @classmethod
    def candidate_page_urls(cls, base_url, page_num, page_size):
        """Candidate URLs for `page_num` (>=2), each tagged with a template so
        the winner can be reused for later pages. Tries ?page=N / ?paged=N /
        ?p=N / ?pg=N / ?wpv_paged=N, ?offset=N*size / ?start=N*size, /page/N/."""
        out = []
        for param in cls._PAGE_PARAMS:
            out.append((cls._with_param(base_url, param, page_num), ("param", param)))
        off = (page_num - 1) * max(page_size, 1)
        for param in cls._OFFSET_PARAMS:
            out.append((cls._with_param(base_url, param, off), ("offset", param)))
        pu = urlparse(base_url)
        path = pu.path.rstrip("/")
        out.append((urlunparse((pu.scheme, pu.netloc, f"{path}/page/{page_num}/",
                                pu.params, pu.query, pu.fragment)), ("path", None)))
        return out

    @classmethod
    def build_from_template(cls, base_url, template, page_num, page_size):
        kind, param = template
        if kind == "param":
            return cls._with_param(base_url, param, page_num)
        if kind == "offset":
            return cls._with_param(base_url, param, (page_num - 1) * max(page_size, 1))
        if kind == "path":
            pu = urlparse(base_url)
            path = pu.path.rstrip("/")
            return urlunparse((pu.scheme, pu.netloc, f"{path}/page/{page_num}/",
                               pu.params, pu.query, pu.fragment))
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

        first_seen: DO NOT trust this code to protect it. Both extractors
        hardcode "first_seen": now, so l.get("first_seen", now) below can
        never see a prior value — the default branch is unreachable. Combined
        with PostgREST upsert (which writes every column it is given) that
        reset first_seen on every re-scrape of an existing row, silently, from
        the 26 Aug revert until 13 Sep. 46,326 rows had to be repaired.

        The invariant now lives in the database, where no client can bypass it:

            trg_listings_direct_lock_first_seen  (BEFORE UPDATE)
              new.first_seen := least(new.first_seen, old.first_seen)

        first_seen may be corrected earlier; it can never move later. The value
        sent below therefore only takes effect on INSERT of a genuinely new row.
        If you remove that trigger, you reintroduce the bug.
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
                "asking_price":  positive_or_none(l.get("asking_price")),
                "cash_flow":     positive_or_none(l.get("cash_flow")),
                "revenue":       positive_or_none(l.get("revenue")),
                "vertical":      l.get("vertical", "other"),
                "description":   (l.get("description") or "")[:2000],
                "status":        l.get("status") or "active",
                "needs_review":  bool(l.get("needs_review", False)),
                "source":        "broker_direct",
                # INSERT-only in practice; clamped by
                # trg_listings_direct_lock_first_seen on UPDATE.
                "first_seen":    l.get("first_seen", now),
                "last_seen":     now,
                "updated_at":    now,
                "created_at":    l.get("first_seen", now),
            })

        # Postgres refuses an INSERT ... ON CONFLICT batch that contains the
        # same conflict key twice: "ON CONFLICT DO UPDATE command cannot affect
        # row a second time". It rejects the WHOLE batch, not the duplicate -
        # so up to 100 good listings were being silently discarded every time
        # a broker produced two cards that resolved to the same id. That fired
        # three times in a single 100-broker run.
        #
        # Dedupe by id first, keeping the last occurrence (the later card is
        # usually the more complete one - detail-page enrichment runs in order).
        deduped: dict[str, dict] = {}
        for row in rows:
            deduped[row["id"]] = row
        dropped = len(rows) - len(deduped)
        if dropped:
            print(f"   ⓘ  collapsed {dropped} duplicate ids before upsert")
        rows = list(deduped.values())

        written = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            try:
                self.client.table("listings_direct").upsert(
                    batch, on_conflict="id", ignore_duplicates=False
                ).execute()
                written += len(batch)
            except Exception as exc:
                # One bad row must not cost the other 99. Retry singly so the
                # rest land and the offender is named.
                print(f"   ⚠️  batch upsert failed ({exc}); retrying rows individually")
                for row in batch:
                    try:
                        self.client.table("listings_direct").upsert(
                            [row], on_conflict="id", ignore_duplicates=False
                        ).execute()
                        written += 1
                    except Exception as row_exc:
                        print(f"      row {row.get('id')} "
                              f"({(row.get('title') or '')[:50]}): {row_exc}")
            time.sleep(0.1)
        return written


def _clip(value, limit):
    """Truncate to `limit` chars, mapping empty/None to NULL."""
    if not value:
        return None
    return str(value)[:limit]


class CrawlFailureWriter:
    """
    Persists per-broker crawl failures to Supabase `crawl_failures`.

    Until now a failure only ever reached data/snapshots/<date>/failures.json,
    which CI throws away with the workspace — so the table stayed at 0 rows and
    there was no way to ask "why does this broker keep failing, and since
    when?" across runs. The local file is still written exactly as before; this
    is purely additive.

    Attribution is by domain -> broker_sources.id, the FK the table already
    has. ~99% of crawled domains resolve. The rest are inserted with a NULL FK
    and their domain/url preserved in `message`, so no row is ever anonymous.
    """

    MESSAGE_LIMIT   = 2000
    TRACEBACK_LIMIT = 2000
    HTML_LIMIT      = 2000

    def __init__(self, client):
        self.client     = client
        self.source_ids = self._load_source_ids()
        self.unresolved: set[str] = set()
        print(f"🗂️  crawl_failures: {len(self.source_ids)} broker_sources domains loaded")

    @staticmethod
    def _norm(domain: str) -> str:
        d = (domain or "").strip().lower()
        return d[4:] if d.startswith("www.") else d

    def _load_source_ids(self) -> dict:
        """domain -> broker_sources.id, paged (PostgREST caps rows/request)."""
        ids, start, page = {}, 0, 1000
        while True:
            resp = (self.client.table("broker_sources")
                    .select("id,domain")
                    .range(start, start + page - 1)
                    .execute())
            rows = resp.data or []
            for r in rows:
                d = self._norm(r.get("domain"))
                if d and d not in ids:
                    ids[d] = r["id"]
            if len(rows) < page:
                break
            start += page
        return ids

    def record(self, failures: list) -> int:
        """Insert a batch of failure dicts. Returns rows written."""
        if not failures:
            return 0

        rows = []
        for f in failures:
            domain = self._norm(f.get("domain")
                                or urlparse(f.get("url") or "").netloc)
            source_id = self.source_ids.get(domain)
            message   = f.get("error") or ""
            if not source_id:
                # Nothing to hang the FK on — keep identity in the text so the
                # row is still answerable when someone queries it later.
                self.unresolved.add(domain)
                message = (f"[{domain or 'unknown-domain'}] "
                           f"{f.get('url') or ''} :: {message}")

            rows.append({
                "broker_source_id": source_id,
                "failure_type":     f.get("type") or "FETCH_ERROR",
                "http_status":      f.get("http_status"),
                "stage":            f.get("stage"),
                "render_mode":      f.get("render_mode"),
                "proxy_used":       f.get("proxy_used"),
                "message":          message[:self.MESSAGE_LIMIT] or None,
                "traceback":        _clip(f.get("traceback"), self.TRACEBACK_LIMIT),
                "html_sample":      _clip(f.get("html_sample"), self.HTML_LIMIT),
                "observed_at":      (f.get("observed_at")
                                     or datetime.now(timezone.utc).isoformat()),
            })

        written = 0
        for i in range(0, len(rows), 100):
            batch = rows[i:i + 100]
            self.client.table("crawl_failures").insert(batch).execute()
            written += len(batch)
            time.sleep(0.1)
        return written


# ============================================================
# MAIN SCRAPER
# ============================================================

class OrderingError(RuntimeError):
    """--stale-first could not order the brokers. Fatal: main() exits 3.

    A silent fallback to CSV order is worse than a failed job — it ran for
    at least 2026-09-11..23 with every nightly crawl hitting CSV rows 1-110.
    """


class DealLedgerScraper:

    def __init__(self, output_dir="data/snapshots", use_supabase=True):
        self.output_dir = output_dir
        self.pattern_cache = PatternCache()
        self.fetcher = PageFetcher()
        self.supabase_writer = None
        self.failure_writer  = None

        if use_supabase:
            try:
                self.supabase_writer = SupabaseWriter()
                print("✅ Supabase connected")
            except Exception as e:
                print(f"⚠️  Supabase unavailable: {e} — local files only")

            if self.supabase_writer:
                try:
                    rows = (self.supabase_writer.client.table("broker_block")
                            .select("broker_domain").limit(10000).execute().data or [])
                    for r in rows:
                        d = (r.get("broker_domain") or "").strip().lower()
                        if d:
                            RUNTIME_BLOCKED.add(d[4:] if d.startswith("www.") else d)
                    print(f"🚫 broker_block: {len(RUNTIME_BLOCKED)} domains")
                except Exception as e:
                    print(f"⚠️  broker_block unavailable: {e} — code blocklists only")
                try:
                    self.failure_writer = CrawlFailureWriter(self.supabase_writer.client)
                except Exception as e:
                    # Never let failure logging take down a listings run.
                    print(f"⚠️  crawl_failures unavailable: {e} — failures.json only")

        fp = "chrome131 (curl_cffi)" if HAS_CURL_CFFI else "plain requests"
        if PROXY_AVAILABLE:
            print(f"🔀 Proxy enabled — DataImpulse US sticky; escalate on 403. Fingerprint: {fp}")
        else:
            print(f"ℹ️  No proxy configured (set PROXY_USER/PROXY_PASS). Fingerprint: {fp}")

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
        self.embed_brokers: list[dict] = []  # iframe/embed brokers (prospect list)

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
        skipped = 0
        for _, row in df.iterrows():
            url = str(row[url_col]).strip()
            if not url.startswith("http"):
                continue
            domain = urlparse(url).netloc
            # Skip known non-listing sites (agent directories, junk aggregators).
            if is_blocked(domain):
                skipped += 1
                continue
            name = (str(row[name_col]).strip()
                    if name_col and pd.notna(row.get(name_col))
                    else domain)
            brokers.append({"name": name, "url": url, "domain": domain})

        msg = f"📋 Loaded {len(brokers)} brokers from {csv_path}"
        if skipped:
            msg += f" ({skipped} blocklisted skipped)"
        print(msg)
        return brokers

    # Brokers with a purpose-built scraper in scrapers/run_specialized.py.
    # specialized_scrape.yml runs those every day at 08:00 UTC and they hold
    # the majority of the active index, so the generic V6 crawl must not spend
    # its nightly budget on them as well.
    #
    # The WordPress-REST set is read from the same CSV run_specialized.py
    # registers from, so it cannot drift. The named franchise scrapers are
    # listed here because they are classes, not rows; they change rarely, and
    # a stale entry only costs one redundant crawl.
    _FRANCHISE_DOMAINS = (
        "tworld.com", "sunbeltnetwork.com", "fcbb.com", "murphybusiness.com",
        "vrbusinessbrokers.com", "hedgestone.com", "linkbusiness.com",
        "bodnergroup.com", "wesellrestaurants.com", "vestedbb.com",
        "routesforsale.net",
    )

    def _specialized_domains(self) -> set:
        """Bare domains already covered by the daily specialized run."""
        out = {d for d in self._FRANCHISE_DOMAINS}
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "..", "data", "wp_rest_brokers.csv")
        try:
            with open(path) as f:
                for row in _csv.DictReader(f):
                    d = (row.get("domain") or "").strip().lower()
                    if d:
                        out.add(d[4:] if d.startswith("www.") else d)
        except Exception as e:
            print(f"⚠️  Could not read wp_rest_brokers.csv ({e}) — "
                  f"excluding franchise domains only")
        return out

    def _order_by_staleness(self, brokers: list[dict]) -> list[dict]:
        """
        Sort brokers so the least-recently-scraped come first.

        Reads max(last_seen) per broker_domain from listings_direct.
        Brokers with no rows there (never scraped) get an empty-string key,
        which sorts to the very front — so dark brokers are picked up first.

        This is READ-ONLY against listings_direct and never touches the
        BizBuySell `listings` table.

        FAILURE IS FATAL (2026-09-24). This used to catch everything and fall
        back to CSV order, so a broken ordering looked exactly like a working
        one: --top-n just took CSV rows 1-110 every night and the job stayed
        green. Now any failure raises OrderingError and main() exits 3.

        Keys are BARE domains (www. stripped) on both sides: the
        broker_last_seen() RPC strips it server-side, and every lookup here
        goes through _bare(). Before, "www.x.com" in the CSV never matched
        "x.com" in listings_direct and the broker looked never-scraped.
        """
        if not self.supabase_writer:
            raise OrderingError("--stale-first requested but Supabase is unavailable")

        def _bare(d):
            d = (d or "").strip().lower()
            return d[4:] if d.startswith("www.") else d

        try:
            latest: dict[str, str] = {}
            yield_by_domain: dict[str, int] = {}
            # One RPC, grouped server-side: bare_domain, max(last_seen), count(*).
            # Paged only because PostgREST caps any response at 1000 rows and
            # there are ~775 domains today; ordered by bare_domain so pages
            # are stable.
            page_size = 1000
            start = 0
            while True:
                rows = (self.supabase_writer.client
                        .rpc("broker_last_seen", {})
                        .range(start, start + page_size - 1)
                        .execute().data) or []
                for row in rows:
                    d = row.get("bare_domain")
                    if d:
                        latest[d] = row.get("last_seen") or ""
                        yield_by_domain[d] = int(row.get("n") or 0)
                if len(rows) < page_size:
                    break
                start += page_size
            if not latest:
                raise OrderingError("broker_last_seen() returned no rows")
            print(f"📊 broker_last_seen(): {len(latest)} domains, "
                  f"{sum(yield_by_domain.values())} listings_direct rows")


            # PRODUCERS FIRST, unless explicitly told otherwise.
            #
            # Pure staleness ordering sorts never-scraped brokers to the very
            # front, so every run spends itself on the cohort that has never
            # once succeeded - 70% NO_PATTERN and almost no listings, while
            # brokers holding hundreds of live listings go unrefreshed for
            # days. Staleness is the tiebreaker; proven yield is the sort.
            #
            # ORDER=stale restores the old behaviour for a deliberate sweep of
            # the dark ones. ORDER=never crawls only never-scraped brokers.
            order_mode = os.environ.get("ORDER", "weekly").lower()

            if order_mode == "never":
                brokers = [b for b in brokers
                           if yield_by_domain.get(_bare(b["domain"]), 0) == 0]
                print(f"📊 ORDER=never — {len(brokers)} never-scraped brokers only")
                return brokers

            # WEEKLY ROTATION (default).
            #
            # ORDER=yield sorted producers by descending yield and took the top
            # --top-n. With ~470 producers and --top-n 100 that is the SAME 100
            # brokers every night, deterministically: across the 8 runs to
            # 2026-09-16 the committed snapshots touched 190 distinct domains
            # in total and 36 of them appeared in 7 of the 8. The queue could
            # not rotate, so a broker added by discovery was never reached.
            #
            # A broker with 400 listings does not turn over in 24 hours. So:
            # drop the brokers the daily specialized run already covers, then
            # take the STALEST 1/7 of what is left. Every tail producer gets
            # crawled once a week, on the same nightly budget, and the slots
            # that frees go to never-scraped brokers.
            #
            # Staleness rather than a hash bucket, so a missed night catches up
            # by itself and no bucket bookkeeping has to be stored anywhere.
            if order_mode == "weekly":
                spec = self._specialized_domains()

                csv_order = brokers
                tail = [b for b in brokers
                        if yield_by_domain.get(_bare(b["domain"]), 0) > 0
                        and _bare(b["domain"]) not in spec]
                skipped = sum(1 for b in brokers
                              if yield_by_domain.get(_bare(b["domain"]), 0) > 0
                              and _bare(b["domain"]) in spec)
                tail.sort(key=lambda b: latest.get(_bare(b["domain"]), ""))

                days = max(1, int(os.environ.get("REFRESH_CYCLE_DAYS", "7")))
                slice_n = -(-len(tail) // days)          # ceil
                due = tail[:slice_n]
                print(f"📅 ORDER=weekly — {len(tail)} tail producers, "
                      f"{skipped} left to the specialized run, "
                      f"refreshing the stalest {len(due)} tonight "
                      f"(1/{days} cycle)")

                quota = int(os.environ.get("NEW_BROKER_QUOTA", "40") or 0)
                fresh = []
                if quota > 0:
                    cooled = self._recently_failed_domains(days=7)
                    seen_fresh = set()
                    for b in reversed(csv_order):
                        dom = b["domain"]
                        if (yield_by_domain.get(_bare(dom), 0) == 0
                                and _bare(dom) not in cooled
                                and _bare(dom) not in spec
                                and dom not in seen_fresh):
                            fresh.append(b)
                            seen_fresh.add(dom)
                            if len(fresh) >= quota:
                                break
                    print(f"🆕 NEW_BROKER_QUOTA={quota} — {len(fresh)} "
                          f"never-scraped brokers ({len(cooled)} on 7-day "
                          f"failure cooldown)")
                # REFRESH FIRST, discovery second (2026-09-17). The nightly
                # run is killed at the 285-minute guard often enough that
                # order decides what actually happens: with never-scraped
                # brokers at the front, a truncated run spends its whole
                # budget on the cohort that mostly yields NO_PATTERN and the
                # weekly refresh silently doesn't happen. The audit found 344
                # of 485 live domains unrefreshed for more than 7 days, some
                # for 70. Due-first means a short night costs us discovery,
                # which can wait, instead of freshness, which can't.
                #
                # FILL THE BUDGET (2026-09-24). due + fresh is ~110 brokers,
                # sized for one --top-n 100 job. The workflow now runs four
                # shards (400/day), so after the priority prefix append the
                # rest of the tail (stalest first) and then the rest of the
                # never-scraped cohort. --top-n still slices, so the head of
                # the list is unchanged for a single 100-broker run.
                picked = {id(b) for b in due + fresh}
                rest_tail = [b for b in tail[slice_n:] if id(b) not in picked]
                rest_new = [b for b in reversed(csv_order)
                            if id(b) not in picked
                            and yield_by_domain.get(_bare(b["domain"]), 0) == 0
                            and _bare(b["domain"]) not in spec]
                ordered, seen = [], set()
                for b in due + fresh + rest_tail + rest_new:
                    k = _bare(b["domain"])
                    if k not in seen:
                        seen.add(k)
                        ordered.append(b)
                return ordered

            if order_mode == "yield":
                csv_order = brokers
                def key(b):
                    d = b["domain"]
                    produced = yield_by_domain.get(_bare(d), 0)
                    # Never-scraped go last in this mode, not first.
                    return (0 if produced else 1, -produced, latest.get(_bare(d), ""))
                brokers = sorted(brokers, key=key)
                producing = sum(1 for b in brokers
                                if yield_by_domain.get(_bare(b["domain"]), 0) > 0)
                print(f"📊 ORDER=yield — {producing} producing brokers first, "
                      f"{len(brokers) - producing} never-scraped last")

                # NEW-BROKER QUOTA. With --top-n 100 and ~670 producers,
                # "never-scraped last" means a broker added to the CSV is
                # never reached at all (found 2026-09-15: the first IBBA
                # discovery batch would have sat unscraped indefinitely).
                # Reserve a few slots at the front for never-scraped brokers,
                # newest CSV rows first (discovery appends to the end), and
                # skip any that already failed in the last 7 days so one dead
                # site can't hold a slot forever and the backlog rotates.
                quota = int(os.environ.get("NEW_BROKER_QUOTA", "5") or 0)
                if quota > 0:
                    cooled = self._recently_failed_domains(days=7)
                    fresh, seen_fresh = [], set()
                    for b in reversed(csv_order):
                        dom = b["domain"]
                        if (yield_by_domain.get(_bare(dom), 0) == 0
                                and _bare(dom) not in cooled
                                and dom not in seen_fresh):
                            fresh.append(b)
                            seen_fresh.add(dom)
                            if len(fresh) >= quota:
                                break
                    picked = {id(b) for b in fresh}
                    brokers = fresh + [b for b in brokers if id(b) not in picked]
                    print(f"🆕 NEW_BROKER_QUOTA={quota} — {len(fresh)} never-scraped "
                          f"brokers moved to the front ({len(cooled)} on 7-day "
                          f"failure cooldown): "
                          + ", ".join(b["domain"] for b in fresh[:10]))
                return brokers

            # Empty string sorts before any ISO timestamp => never-scraped first
            brokers.sort(key=lambda b: latest.get(_bare(b["domain"]), ""))
            never = sum(1 for b in brokers if _bare(b["domain"]) not in latest)
            print(f"🔄 Ordered by staleness — {len(latest)} known domains, "
                  f"{never} never-scraped brokers moved to front")
        except OrderingError:
            raise
        except Exception as e:
            raise OrderingError(f"Staleness ordering failed: {type(e).__name__}: {e}") from e
        return brokers

    def _stored_listings(self, ids: list) -> dict:
        """id -> stored enriched fields from listings_direct (read-only)."""
        if not (SKIP_KNOWN_DETAILS and self.supabase_writer and ids):
            return {}
        out = {}
        try:
            client = self.supabase_writer.client
            for i in range(0, len(ids), 100):
                rows = (client.table("listings_direct")
                        .select("id,title,url,city,state,asking_price,cash_flow,"
                                "revenue,description")
                        .in_("id", ids[i:i + 100]).execute().data or [])
                for r in rows:
                    out[r["id"]] = r
        except Exception as e:
            print(f"   ⚠️  stored-listing lookup failed ({e}) — fetching all details")
            return {}
        return out

    def _recently_failed_domains(self, days: int = 7) -> set:
        """Bare domains with a crawl_failures row in the last `days` days.

        Read-only. Returns an empty set on any error so ordering never breaks
        a run.
        """
        try:
            client = self.supabase_writer.client
            since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
            ids, start = set(), 0
            while True:
                rows = (client.table("crawl_failures")
                        .select("broker_source_id")
                        .gte("observed_at", since)
                        .range(start, start + 999).execute().data or [])
                ids.update(r["broker_source_id"] for r in rows if r.get("broker_source_id"))
                if len(rows) < 1000:
                    break
                start += 1000
            if not ids:
                return set()
            domains, start = set(), 0
            while True:
                rows = (client.table("broker_sources").select("id,domain")
                        .range(start, start + 999).execute().data or [])
                for r in rows:
                    if r["id"] in ids:
                        d = (r.get("domain") or "").lower()
                        domains.add(d[4:] if d.startswith("www.") else d)
                if len(rows) < 1000:
                    break
                start += 1000
            return domains
        except Exception as e:
            print(f"⚠️  failure-cooldown lookup failed ({e}) — no cooldown applied")
            return set()

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
        """crawl_run wrapper around _scrape_broker_inner.

        The inner method has SEVEN exit points that all `return []` and never
        re-raise, so a try/except around the call site can't tell failure from
        an empty broker. Rather than instrument all seven, classify from the
        side effects the inner method already produces:

          - len(self.failures) grew  -> a real failure (covers all six failure
            paths: HTTPError, both NO_PATTERN sites, VALIDATION_REJECT,
            TITLE_REPETITION_REJECT, and the catch-all)
          - listings returned        -> 'ok'
          - neither                  -> 'empty' (e.g. the EMBED_BBS return at
            2785, which is not a failure but is not coverage either)

        Deliberately NOT keyed on stats['brokers_success'], which is incremented
        before the validation and yield gates — both of which reject and return
        []. Keying on it would log a successful crawl for a broker whose
        listings were thrown away, telling v_dom_direct it had coverage on a day
        it wrote nothing.

        Note stats['brokers_failed'] is ALSO wrong for this: the two gate
        rejections append to self.failures but never increment it.
        """
        run_id = start_run("v6", broker.get("domain"))
        failures_before = len(self.failures)
        try:
            listings = self._scrape_broker_inner(broker)
        except Exception as e:                       # inner shouldn't raise, but
            finish_run(run_id, "failed", error=str(e))
            raise

        if len(self.failures) > failures_before:
            ftype = self.failures[-1].get("type", "UNKNOWN")
            finish_run(run_id, "failed", error=ftype)
        elif listings:
            finish_run(run_id, "ok", len(listings), len(listings))
        else:
            # 'empty' is not an allowed status (crawl_run_status_check:
            # running/ok/failed/partial/backfill). The PATCH was rejected and
            # the row stayed 'running' forever. Zero listings is not a
            # coverage window, so it closes as failed.
            finish_run(run_id, "failed", 0, 0, error="EMPTY")
        return listings

    def _scrape_broker_inner(self, broker: dict) -> list[dict]:
        name      = broker["name"]
        url       = broker["url"]
        domain    = broker["domain"]
        use_proxy = _needs_proxy(domain)
        t_start = time.monotonic()

        def over_budget():
            return BROKER_TIME_BUDGET > 0 and time.monotonic() - t_start > BROKER_TIME_BUDGET

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

            # ── API handler (JS sites with a JSON feed) ───────────────────
            # If this is a known JS platform, pull listings straight from its
            # JSON endpoint and skip DOM pattern detection entirely.
            api_listings = extract_via_api(url, html, name, self.fetcher)
            if api_listings:
                print(f"   🔌 API handler → {len(api_listings)} listings")
                p  = sum(1 for l in api_listings if l.get("asking_price"))
                st = sum(1 for l in api_listings if l.get("state"))
                print(f"   💰 price={p}  state={st}/{len(api_listings)}")
                self.stats["brokers_success"] += 1
                self.stats["detail_pages_fetched"] += 0
                return api_listings

            # ── Pattern detection ─────────────────────────────────────────
            cached = self.pattern_cache.get(domain)
            if cached:
                pattern = cached
                print(f"   📦 Cached pattern: {pattern['container_selector']}")
            else:
                pattern = PatternDetector.detect(html, url)

                # Task C — pattern-aware Playwright escalation. If the plain
                # fetch yielded no detectable pattern, the listing grid is
                # very likely JS-rendered (a shell whose cards are injected by
                # client-side JS). Re-render with Playwright and re-detect
                # before giving up. Route through the proxy if this domain has
                # been blocking. This also covers the case where curl_cffi
                # returns a content-ful shell that passes the price-signal
                # check but whose grid only materializes after JS runs.
                rendered_html = None  # reused by iframe unwrap to avoid double render
                if not pattern and method != "playwright" and HAS_PLAYWRIGHT:
                    try:
                        pw_proxy = use_proxy or (domain in self.fetcher._proxy_domains_runtime)
                        pw_html = self.fetcher.fetch_playwright(url, use_proxy=pw_proxy)
                        rendered_html = pw_html
                        pw_pattern = PatternDetector.detect(pw_html, url)
                        if pw_pattern:
                            html, method, pattern = pw_html, "playwright", pw_pattern
                            print(f"   🎭 Playwright escalation recovered a pattern "
                                  f"({len(pw_html):,} bytes)")
                    except Exception:
                        pass

                # ── iframe unwrap ─────────────────────────────────────────
                # Still no pattern? The listings may live inside an <iframe>
                # (third-party feed or BizBuySell widget). Fetch the iframe's
                # src directly and detect on THAT. Recovers Wix/Squarespace
                # "listing_feeds" embeds and similar. Many sites inject the
                # iframe via JS, so it is ABSENT from the raw HTML — we search
                # the raw HTML first, then the Playwright-RENDERED DOM where the
                # JS-injected iframe actually appears. BizBuySell iframes are
                # recorded for the prospect list but skipped for data.
                if not pattern:
                    def _try_iframes(source_html, source_url, _depth=0):
                        """Return (html, pattern, iframe_url) on recovery, or
                        ('bbs', None, ifr_url) if a BBS embed was tagged, else None.

                        Recurses one level: some builders (Wix "custom HTML"
                        embeds) wrap the real widget in an iframe whose OWN
                        document is just a shim that in turn iframes BizBuySell
                        or the actual feed — excellencebusinessbrokers.com is
                        exactly this shape. A single-level search never sees
                        the inner iframe and reports NO_PATTERN even though
                        the page is a known, correctly-unscrapable BBS embed.
                        """
                        for ifr_url in _find_listing_iframes(source_html, source_url)[:3]:
                            if _iframe_is_bizbuysell(ifr_url):
                                return ("bbs", None, ifr_url)
                            try:
                                ip = use_proxy or (domain in self.fetcher._proxy_domains_runtime)
                                ih, _ = self.fetcher.fetch(ifr_url, use_proxy=ip)
                                ipat = PatternDetector.detect(ih, ifr_url)
                                if ipat:
                                    return (ih, ipat, ifr_url)
                                if _depth < 1:
                                    nested = _try_iframes(ih, ifr_url, _depth + 1)
                                    if nested is not None:
                                        return nested
                            except Exception:
                                continue
                        return None

                    # 1) raw HTML iframes
                    result = _try_iframes(html, url)
                    # 2) if nothing, look in the Playwright-RENDERED DOM where
                    #    JS-injected iframes appear (capstar/excellence-style).
                    #    Reuse the render from the escalation step if we have it;
                    #    otherwise render now.
                    if result is None and method != "playwright" and HAS_PLAYWRIGHT:
                        try:
                            if rendered_html is None:
                                pw_proxy = use_proxy or (domain in self.fetcher._proxy_domains_runtime)
                                rendered_html = self.fetcher.fetch_playwright(url, use_proxy=pw_proxy)
                            result = _try_iframes(rendered_html, url)
                        except Exception:
                            pass

                    if result is not None:
                        if result[0] == "bbs":
                            ifr_url = result[2]
                            print(f"   🔗 BizBuySell embed detected → tagged, skipping data")
                            self.stats["failure_types"]["EMBED_BBS"] += 1
                            self.embed_brokers.append(
                                {"broker": name, "url": broker["url"], "iframe": ifr_url,
                                 "provider": "bizbuysell"})
                            # Correctly classified — don't fall through to the
                            # "if pattern:" check below, which would ALSO log
                            # this as NO_PATTERN (pattern is still None here)
                            # and double-count it. That double-count is what
                            # put already-identified BBS embeds like
                            # manhattan.biz on the NO_PATTERN worklist.
                            return []
                        else:
                            ifr_html, ifr_pattern, ifr_url = result
                            html, method, pattern = ifr_html, "iframe", ifr_pattern
                            url = ifr_url
                            print(f"   🖼️  iframe unwrap recovered a pattern "
                                  f"({len(ifr_html):,} bytes) from {ifr_url[:60]}")
                            self.embed_brokers.append(
                                {"broker": name, "url": broker["url"],
                                 "iframe": ifr_url, "provider": "feed"})

                if pattern:
                    predicted = self.pattern_cache.predict(html, url)
                    if (predicted and predicted.get("score", 0) > pattern.get("score", 0)
                            and not _is_nav_selector(predicted.get("container_selector", ""))):
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
            # Follows explicit next-links; when a site has none (numbered / JS
            # pagination), falls back to constructing ?page=N / /page/N/ /
            # ?offset= URLs, locks onto whichever template yields new cards, and
            # stops when a page adds nothing new or repeats page 1.
            unique_cards: list[dict] = []
            seen_ids: set[str] = set()
            first_page_ids: set[str] = set()
            page_size = 0
            page_num = 1
            max_pages = 50
            current_url = url
            pending_html = html          # page 1 already fetched
            template = None              # locked pagination template once found

            def _cards_from(html_text, page_url):
                soup = BeautifulSoup(html_text, "html.parser")
                try:
                    els = soup.select(pattern["container_selector"])
                except Exception:
                    els = []
                out = []
                for el in els:
                    c = ListingExtractor.from_card(el, page_url, name)
                    if c:
                        out.append(c)
                return soup, out

            MAX_CARDS_PER_BROKER = 2500

            while page_num <= max_pages:
                if over_budget():
                    print(f"   ⏱️  {BROKER_TIME_BUDGET}s budget reached on page {page_num} "
                          f"- keeping {len(unique_cards)} cards")
                    break
                if len(unique_cards) >= MAX_CARDS_PER_BROKER:
                    print(f"   ⚠️  hit {MAX_CARDS_PER_BROKER}-card ceiling for this "
                          f"broker - stopping (suspect pagination loop)")
                    break
                if pending_html is not None:
                    cur_html, pending_html = pending_html, None
                else:
                    time.sleep(random.uniform(1, 3))
                    try:
                        cur_html, _ = self.fetcher.fetch(current_url, use_proxy=use_proxy)
                    except Exception:
                        break

                soup, page_cards = _cards_from(cur_html, current_url)
                if not page_cards:
                    break
                page_ids = {c["id"] for c in page_cards}

                # Content fingerprint of the page, independent of how ids are
                # built. The id-set check below cannot fire when ids embed the
                # page URL - quietlight.com returned the same 227 cards on all
                # 50 pages and every one counted as new, producing 11,440 rows
                # for ~300 real listings. Titles are what actually repeat.
                page_sig = hash(frozenset(
                    (c.get("title") or "")[:80] for c in page_cards
                ))
                if page_num == 1:
                    first_page_ids = page_ids
                    first_page_sig = page_sig
                    page_size = len(page_cards)
                    seen_sigs = {page_sig}
                else:
                    if page_sig in seen_sigs:
                        print(f"   ↩︎  page {page_num} repeats earlier content - stopping")
                        break
                    seen_sigs.add(page_sig)

                    if not (page_ids - seen_ids) or page_ids == first_page_ids:
                        break  # nothing new / pagination looped back to page 1

                    # A site that ignores the page parameter serves page 1
                    # forever. Titles repeating is the tell.
                    prev_titles = {(c.get("title") or "")[:80] for c in unique_cards}
                    page_titles = {(c.get("title") or "")[:80] for c in page_cards}
                    if page_titles and len(page_titles - prev_titles) / len(page_titles) < 0.2:
                        print(f"   ↩︎  page {page_num} is {100 - int(100*len(page_titles - prev_titles)/len(page_titles))}% "
                              f"repeat titles - stopping")
                        break

                new = [c for c in page_cards if c["id"] not in seen_ids]
                unique_cards.extend(new)
                seen_ids |= page_ids
                if page_num > 1:
                    print(f"   📄 Page {page_num}: {len(new)} new cards")

                # ── decide next page ──────────────────────────────────────
                nxt = PaginationHandler.find_next_page(soup, current_url)
                if nxt:
                    current_url, template = nxt, None
                    page_num += 1
                    continue
                if template:
                    current_url = PaginationHandler.build_from_template(
                        url, template, page_num + 1, page_size)
                    page_num += 1
                    continue
                # No explicit next-link and no locked template yet — discover one.
                discovered = False
                for cand, tmpl in PaginationHandler.candidate_page_urls(
                        url, page_num + 1, page_size):
                    try:
                        chtml, _ = self.fetcher.fetch(cand, use_proxy=use_proxy)
                    except Exception:
                        continue
                    _, ccards = _cards_from(chtml, cand)
                    cids = {c["id"] for c in ccards}
                    if cids and (cids - seen_ids) and cids != first_page_ids:
                        template, current_url, pending_html = tmpl, cand, chtml
                        page_num += 1
                        discovered = True
                        break
                if not discovered:
                    break

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
            with_detail = [
                c for c in unique_cards
                if c.get("_detail_url") and c["_detail_url"] != url
            ]

            # Listings already enriched on an earlier run: carry the stored
            # detail fields forward instead of fetching the page again.
            stored = self._stored_listings([c["id"] for c in with_detail])
            reused = 0
            fresh = []
            for c in with_detail:
                prev = stored.get(c["id"])
                prev_desc = (prev or {}).get("description") or ""
                if prev and (prev.get("state") or len(prev_desc) > len(c.get("description") or "")):
                    if len(prev_desc) > len(c.get("description") or ""):
                        c["description"] = prev_desc
                    if len(prev.get("title") or "") > len(c.get("title") or ""):
                        c["title"] = prev["title"]
                    for f in ("asking_price", "cash_flow", "revenue", "state"):
                        if not c.get(f) and prev.get(f):
                            c[f] = prev[f]
                    if not c.get("city") and prev.get("city"):
                        c["city"] = prev["city"]
                    c["url"] = c.pop("_detail_url")
                    c["vertical"] = ListingExtractor.classify_vertical(
                        (c.get("description") or "") + " " + (c.get("title") or ""))
                    reused += 1
                else:
                    fresh.append(c)
            if reused:
                print(f"   ♻️  {reused} known listings reuse stored details")
            detail_candidates = fresh[:MAX_DETAIL_PAGES]

            # A list page that came back fine over plain HTTP doesn't need a
            # headless browser for its detail pages; fetch() would escalate
            # any detail page with fewer than two prices to Playwright (~30s).
            plain_details = method not in ("playwright",)

            enriched = 0
            states_gained = 0

            for listing in detail_candidates:
                if over_budget():
                    print(f"   ⏱️  {BROKER_TIME_BUDGET}s budget reached - "
                          f"{len(detail_candidates) - enriched} detail pages skipped")
                    break
                detail_url = listing.pop("_detail_url")
                try:
                    time.sleep(random.uniform(*DETAIL_DELAY))
                    if plain_details:
                        detail_html = self.fetcher._fetch_http(detail_url, use_proxy=use_proxy)
                    else:
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

            # Guard: url_is_listing_specific must mean "unique to this
            # listing", not just "differs from the list-page URL". A card
            # whose best-guess link (_best_detail_link's candidates[0]
            # fallback) resolves to a shared page - a category link, a
            # generic CTA, anything that isn't actually per-listing - passes
            # the per-card check yet ends up identical across many distinct
            # titles. Same shape as the aria.net index-page bug (CLAUDE.md
            # principle 7), just caught later, once the whole broker's set is
            # known. Demote it here rather than leave every reader downstream
            # to rediscover it.
            url_titles: dict[str, set[str]] = {}
            for l in unique_cards:
                if l.get("url_is_listing_specific"):
                    url_titles.setdefault(l["url"], set()).add((l.get("title") or "")[:80])
            shared_urls = {u for u, titles in url_titles.items() if len(titles) > 1}
            if shared_urls:
                demoted = sum(1 for l in unique_cards if l["url"] in shared_urls)
                print(f"   ⚠️  {len(shared_urls)} URL(s) shared by 2+ titles - "
                      f"demoting {demoted} cards' url_is_listing_specific to False")
                for l in unique_cards:
                    if l["url"] in shared_urls:
                        l["url_is_listing_specific"] = False

            if enriched:
                print(f"   🔍 {enriched} detail pages → {states_gained} states gained")

            p  = sum(1 for l in unique_cards if l.get("asking_price"))
            cf = sum(1 for l in unique_cards if l.get("cash_flow"))
            st = sum(1 for l in unique_cards if l.get("state"))
            print(f"   💰 price={p}  cf={cf}  state={st}/{len(unique_cards)}")

            self.stats["brokers_success"] += 1

            # VALIDATION GATE (2026-08): the loosened detector accepts price-less
            # grids, which can occasionally match a category/browse grid instead
            # of real listings. Validate the extracted set before it is written.
            # A set that fails is treated as NO_PATTERN, not written, and flagged
            # for review — protecting data quality as detection recall increases.
            verdict, reason = validate_listing_set(unique_cards, url)
            if verdict == "reject":
                print(f"   🚫 VALIDATION FAILED ({reason}) — not writing, flagged for review")
                self.stats["failure_types"]["VALIDATION_REJECT"] += 1
                self.failures.append({"broker": name, "url": url,
                                      "error": f"validation: {reason}",
                                      "type": "VALIDATION_REJECT"})
                return []
            if verdict == "review":
                print(f"   ⚠️  VALIDATION SOFT ({reason}) — writing but flagged for review")
                for l in unique_cards:
                    l["needs_review"] = True

            # YIELD-SHAPE GATE (2026-09-08, CLAUDE.md principle 7): catches the
            # next pavilion/aria/nebba before anyone has to notice it by hand.
            yv, yreason = title_repetition_gate(unique_cards)
            if yv == "reject":
                print(f"   🚫 YIELD GATE FAILED ({yreason}) — not writing, flagged for review")
                self.stats["failure_types"]["TITLE_REPETITION_REJECT"] += 1
                self.failures.append({"broker": name, "url": url,
                                      "error": f"yield gate: {yreason}",
                                      "type": "TITLE_REPETITION_REJECT"})
                return []

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

        total = len(brokers)
        # INCREMENTAL WRITE (2026-08): previously the whole batch was upserted
        # ONCE, after all 250 brokers finished — so a run that was cancelled or
        # crashed mid-way wrote NOTHING, and the DB stayed empty for ~30-40 min
        # even on a healthy run. Now we flush to Supabase every FLUSH_EVERY
        # brokers, so rows land progressively and partial runs still persist.
        FLUSH_EVERY = 5
        pending = []          # listings accumulated since last flush
        total_written = 0

        def _flush(tag=""):
            nonlocal pending, total_written
            if self.supabase_writer and pending:
                try:
                    n = self.supabase_writer.upsert(pending)
                    total_written += n
                    print(f"   📤 flushed {n} listings to Supabase "
                          f"({total_written} total this run){tag}", flush=True)
                except Exception as e:
                    print(f"   ❌ Supabase flush failed: {e}", flush=True)
            pending = []

        timings = []
        try:
            for i, broker in enumerate(brokers, 1):
                print(f"—— broker {i}/{total} —————————————————————————", flush=True)
                t0 = time.monotonic()
                listings = self.scrape_broker(broker)
                secs = time.monotonic() - t0
                timings.append((round(secs), broker["domain"], len(listings)))
                print(f"   ⏱️  {secs:.0f}s, {len(listings)} listings", flush=True)
                self.all_listings.extend(listings)
                pending.extend(listings)
                for l in listings:
                    self.stats["verticals"][l.get("vertical", "other")] += 1

                # Flush periodically so writes land as we go, not all at the end.
                if i % FLUSH_EVERY == 0:
                    _flush(f" [after {i}/{total}]")

                time.sleep(random.uniform(1, 2))   # trimmed from (2,5): proxy rotates IPs anyway
        except KeyboardInterrupt:
            # The workflow's `timeout --signal=INT` lands here. Before this,
            # the interrupt skipped the final flush and _save_results(), so a
            # timed-out run lost every broker since its last flush and never
            # wrote crawl_failures (none recorded 12-16 Sep).
            print("\n⏹️  interrupted - flushing and saving what was collected", flush=True)
            self.stats["interrupted"] = True

        # Final flush for the remainder past the last batch boundary.
        _flush(" [final]")
        self.stats["slowest_brokers"] = sorted(timings, reverse=True)[:15]
        self.stats["seconds_total"] = sum(t[0] for t in timings)

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

        if self.supabase_writer:
            print(f"\n✅ Supabase: {total_written} rows upserted across the run", flush=True)

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

        # failures.json above is discarded with the CI workspace at the end of
        # every run, which is why crawl_failures sat at 0 rows despite this
        # file existing on every run. Persist the same records to Supabase so
        # "why does this broker keep failing, and since when" is answerable
        # across runs, not just within one.
        if self.failure_writer:
            try:
                n = self.failure_writer.record(self.failures)
                print(f"🗂️  crawl_failures: {n} rows written "
                      f"({len(self.failure_writer.unresolved)} unresolved domains)")
            except Exception as e:
                print(f"⚠️  crawl_failures write failed: {e} — failures.json only")

        # Embed/iframe brokers — the prospect list of brokers whose listings
        # live in a third-party or BizBuySell widget (candidates for a
        # DealLedger-powered custom site).
        with open(os.path.join(snap, "embed_brokers.json"), "w") as f:
            json.dump(self.embed_brokers, f, indent=2, default=str)
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
        if s.get("slowest_brokers"):
            n = max(1, s["brokers_attempted"])
            print(f"\nTime: {s.get('seconds_total', 0) // 60} min, "
                  f"{s.get('seconds_total', 0) // n}s per broker. Slowest:")
            for secs, dom, cnt in s["slowest_brokers"]:
                print(f"  {secs:>5}s  {dom}  ({cnt} listings)")
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
    parser.add_argument("--stale-first", action="store_true",
                        help="Order brokers by least-recently-scraped (reads "
                             "listings_direct) BEFORE applying --top-n, so the "
                             "daily batch rotates through the whole registry")
    parser.add_argument("--output",      default="data/snapshots")
    parser.add_argument("--shard",       metavar="I/N",
                        help="Take every Nth broker starting at I (0-based) from "
                             "the ordered list BEFORE --top-n. Round-robin, so "
                             "each shard starts on the most-due brokers and a "
                             "shard dying early loses low-priority tail only.")
    parser.add_argument("--plan-out",    metavar="CSV",
                        help="Order (+ --top-n), write the resulting broker list "
                             "to CSV and exit without scraping. The workflow's "
                             "plan job uses this so all shards slice ONE "
                             "ordering instead of each re-reading a "
                             "listings_direct the other shards are writing to.")
    parser.add_argument("--no-supabase", action="store_true")
    parser.add_argument("--write",       action="store_true",
                        help="Allow --broker to write to Supabase. Without this, "
                             "--broker always runs with Supabase disabled — it is a "
                             "test gate, not a production write path. Incident "
                             "2026-09-08: pavilionservices.com, www.sagbrokerage.com "
                             "and businessesforsale.nebba.com all wrote a full batch "
                             "to production from a manual --broker run, because "
                             "use_supabase defaulted to True and nothing forced it off.")
    args = parser.parse_args()

    print("=" * 60)
    print("DEALLEDGER SCRAPER V6")
    print("=" * 60)

    use_supabase = not args.no_supabase
    if args.broker and not args.write:
        use_supabase = False

    scraper = DealLedgerScraper(
        output_dir=args.output,
        use_supabase=use_supabase,
    )

    try:
        if args.broker:
            domain = urlparse(args.broker).netloc
            if is_blocked(domain):
                print(f"🚫 {domain} is blocked (BLOCKLIST_DOMAINS / CRE_LEASE_DOMAINS / "
                      f"SPECIALIZED_DOMAINS) — refusing to run --broker against it.")
                sys.exit(1)
            print(f"🧪 SINGLE BROKER TEST"
                  + ("" if use_supabase else " — Supabase disabled (pass --write to write to production)")
                  + "\n")
            brokers = [{"name": args.name, "url": args.broker, "domain": domain}]
        else:
            brokers = scraper._load_brokers(args.brokers)

            # Order by staleness BEFORE slicing, so --top-n takes the most
            # stale / never-scraped brokers rather than the same first N rows.
            if args.stale_first:
                try:
                    brokers = scraper._order_by_staleness(brokers)
                except OrderingError as e:
                    print(f"❌ {e}", flush=True)
                    print("::error::Staleness ordering failed — refusing to fall "
                          "back to CSV order", flush=True)
                    sys.exit(3)
                if not brokers:
                    print("❌ Staleness ordering returned 0 brokers", flush=True)
                    sys.exit(3)

            if args.shard:
                try:
                    i, n = (int(x) for x in args.shard.split("/"))
                    assert n > 0 and 0 <= i < n
                except Exception:
                    print(f"❌ --shard must be I/N with 0 <= I < N, got {args.shard!r}")
                    sys.exit(2)
                brokers = brokers[i::n]
                print(f"🧩 shard {i}/{n}: {len(brokers)} brokers")

            if args.test:
                brokers = brokers[:5]
                print(f"\n🧪 TEST MODE: {len(brokers)} brokers")
            elif args.top_n:
                brokers = brokers[:args.top_n]
                print(f"\n📊 TOP {len(brokers)} brokers"
                      + (" (stale-first)" if args.stale_first else ""))
            elif args.all:
                print(f"\n🚀 ALL {len(brokers)} brokers")
            else:
                print("Specify --test, --top-n N, or --all")
                sys.exit(1)

            if args.plan_out:
                os.makedirs(os.path.dirname(os.path.abspath(args.plan_out)), exist_ok=True)
                pd.DataFrame(
                    [{"broker_name": b["name"], "listing_url": b["url"],
                      "domain": b["domain"]} for b in brokers],
                    columns=["broker_name", "listing_url", "domain"],
                ).to_csv(args.plan_out, index=False)
                print(f"🗺️  plan: wrote {len(brokers)} ordered brokers to {args.plan_out}")
                return

        scraper.run(brokers)
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    main()
