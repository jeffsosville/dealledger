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
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path
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
# Broker's own displayed count per crawl (2026-11-11). Pure module, no I/O.
from displayed_count import DisplayedCountTracker, verdict_label

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

# ── TLS/HTTP2 fingerprint (2026-11-12, decision 10) ─────────────────────────
# Default curl_cffi impersonation target for every PageFetcher request.
# chrome131 → chrome145: laundromat-broker.com (nginx) resets any request whose
# UA says Chrome <= 136 (curl 92), and chrome124 went stale ~30 Jun 2026 the
# same way — an old fingerprint fails quietly, one server at a time.
# DL_IMPERSONATE overrides it for a whole run with no code change, e.g.
#   DL_IMPERSONATE=chrome131  (the pre-2026-11-12 default — rollback)
# An unknown value is NOT silently accepted: the run logs it and uses the
# default, and the startup line names the fingerprint actually in use.
DEFAULT_IMPERSONATE = "chrome145"
_IMPERSONATE_ENV = os.environ.get("DL_IMPERSONATE", "").strip()


def _known_impersonate(name):
    """True if the installed curl_cffi knows this impersonation target."""
    if not HAS_CURL_CFFI:
        return False
    try:
        from curl_cffi.requests import BrowserType
        return name in BrowserType.__members__ or name in {
            m.value for m in BrowserType}
    except Exception:
        return True   # can't introspect — let curl_cffi decide at request time


def _resolve_impersonate(env_value):
    """(target, source) for the run: the env override if valid, else default."""
    if env_value:
        if _known_impersonate(env_value):
            return env_value, "DL_IMPERSONATE"
        print(f"⚠️  DL_IMPERSONATE={env_value!r} is not a curl_cffi target — "
              f"using default {DEFAULT_IMPERSONATE}")
        return DEFAULT_IMPERSONATE, "default (DL_IMPERSONATE invalid)"
    return DEFAULT_IMPERSONATE, "default"


def _reset_fallback_for(primary):
    """Fingerprint for the one-shot retry after an HTTP/2 stream reset: the
    NEXT family, so the retry never repeats the fingerprint that was reset.
    Chrome/Edge primary → Safari 26; anything else → chrome145."""
    if primary.startswith(("chrome", "edge")):
        return "safari2601"
    return "chrome145"


IMPERSONATE, IMPERSONATE_SOURCE = _resolve_impersonate(_IMPERSONATE_ENV)

# Per-domain pin when the default regresses a specific broker (bare host →
# curl_cffi target). Empty on 2026-11-12: no regression in the check set.
IMPERSONATE_DOMAIN_OVERRIDES = {}


def fingerprint_banner():
    """One-line description of the fetch fingerprint in use (startup log)."""
    if not HAS_CURL_CFFI:
        return "plain requests (curl_cffi missing)"
    pins = f", {len(IMPERSONATE_DOMAIN_OVERRIDES)} domain pin(s)" if IMPERSONATE_DOMAIN_OVERRIDES else ""
    return (f"{IMPERSONATE} (curl_cffi, {IMPERSONATE_SOURCE}; "
            f"h2-reset fallback {_reset_fallback_for(IMPERSONATE)}{pins})")

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

# Playwright / plain-requests User-Agents. Chrome 145-era (updated 2026-11-12
# from Chrome/120 — servers that gate on Chrome <= 136 reset those). Same OS mix.
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
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
    "full listing", "view full listing", "see full listing",
}

# A dangling separator left by a page-title template with an empty site name
# ("Medical Clinic for Sale in Collier, FL |" on healthcarebizsales.com, 2026-11-10).
# Only a trailing '|' with nothing after it; titles with real text after the
# pipe, and every other trailing character, are unchanged.
_DANGLING_PIPE_RE = re.compile(r"\s*\|+\s*$")


def strip_dangling_sep(t):
    return _DANGLING_PIPE_RE.sub("", t) if t else t


# Status marker a broker puts at the START of a title ("SOLD! Dental GP",
# "SOLD-GP/Pediatric MD Practice", "UNDER CONTRACT: Pharmacy"). Uppercase, or
# any case when punctuation follows ("Sold! pharmacy", "Sold – home healthcare
# company"), so "Sold With Real Estate" is not a marker.
# Applied ONLY to TITLE_STATUS_MARKER_DOMAINS (owner decision 11, 2026-11-11):
# the 11-10 snapshot has 22 not-yet-sold titles at other brokers that carry
# such a marker, and those keep whatever the generic path gives them.
TITLE_STATUS_MARKER_DOMAINS = {"healthcarebizsales.com"}
_STATUS_WORD = r"(?:sold|under[\s-]*contract|sale[\s-]*pending|pending)"
_TITLE_STATUS_PREFIX_RE = re.compile(
    r"^\s*((?:SOLD|UNDER[\s-]*CONTRACT|SALE[\s-]*PENDING|PENDING)(?:\s*[!:\-–—…]+)?(?=\W|$)"
    r"|(?i:" + _STATUS_WORD + r")\s*[!:\-–—…]+)")


def title_status_marker(t):
    """The verbatim leading status marker of a title, or None."""
    m = _TITLE_STATUS_PREFIX_RE.match(t or "")
    return m.group(1).strip() if m else None


# Owner decision 6: UNDER CONTRACT / PENDING / SALE PENDING mean the listing is
# still ACTIVE (broker_status_text keeps the label); only SOLD means sold.
# Local until the shared status-constants module lands (scrapers/listing_status.py).
_MARKER_SOLD_RE = re.compile(r"^\s*sold\b", re.IGNORECASE)


def title_marker_status(marker):
    """'sold' for a SOLD marker, 'active' for any other title status marker."""
    return "sold" if _MARKER_SOLD_RE.match(marker or "") else "active"


def domain_title_status_marker(domain, t):
    """title_status_marker(t), but only for a TITLE_STATUS_MARKER_DOMAINS broker."""
    if (domain or "").lower().removeprefix("www.") not in TITLE_STATUS_MARKER_DOMAINS:
        return None
    return title_status_marker(t)

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
# This is the pre-2026-11-12 single-signal rule, kept verbatim: with
# CRE_GATE_MODE = "legacy" any match rejects the card, exactly as before.
_LEASE_CRE_RE = re.compile(
    r'for\s+lease|for\s+sublease|annual\s*/\s*sf|\$\s*[\d.,]+\s*/\s*sf|'
    r'\bpsf\b|per\s+sf\b|price\s*/\s*sf|cap\s*rate|\bnnn\b|triple\s*net|'
    r'lease\s*rate|rentable\s+(?:sf|area)',
    re.IGNORECASE,
)

# CRE gate, two-signal (2026-11-12, principle 8). The only caller is
# ListingExtractor.from_card(); API handlers and specialized scrapers never
# ran this gate and still do not.
#
# Why: the single-signal rule above rejected operating businesses that state
# their premises lease on the card. peakbusinessbrokersllc.com's Torrance
# staffing agency ($180,000) died on "Monthly Lease Rate: $4,200";
# laundromat-broker.com lost 6 of its laundromat/restaurant cards to "NNN"
# ("$3145 rent includes all NNN"). A business's rent, lease rate, lease term
# or NNN figure is a fact ABOUT the business, not evidence the card is a
# property. Measured 2026-11-12 over 6 brokers' index pages: 7 listing cards
# rejected, 7 false rejects, 0 true rejects (j3-cre-gate-notes.md, job 3).
#
# The rule now:
#   * STRONG phrases say the listing IS a lease / property-only offering.
#     One is enough (unchanged from legacy for "for lease" / "for sublease" /
#     "$/yr per SF" pricing; listing_status.py drops a "For Lease" status
#     label for the same reason, decision 5).
#   * PROPERTY vocabulary (lease figures, NNN, $/SF, cap rate, tenant, office
#     or retail space, land/lot/building for sale, investment property ...)
#     rejects ONLY when the card also carries NO operating-business evidence
#     (cash flow / SDE / EBITDA / revenue / sales / profit / inventory / FF&E
#     / employees / customers / equipment / "business for sale" ...). An
#     asking price is not business evidence: properties have prices too.
#     A bare "tenant", "strip mall" or "zoned" is NOT property vocabulary:
#     laundromat-broker.com's cards say "shared with one other tenant" and
#     "Strip mall has plenty of parking" about the business's own premises
#     (3 false rejects in the first draft of this rule). Nor is a bare
#     "shopping center" (dental practices and pizzerias sit in them) or
#     "ground lease" (a waterfront restaurant on a ground lease is a business).
#
# CRE_GATE_MODE picks the rule; the old behaviour is one constant flip away:
#   "legacy"      the pre-2026-11-12 rule: any _LEASE_CRE_RE match rejects.
#   "narrow"      legacy vocabulary only, but the lease-FIGURE terms (NNN,
#                 lease rate, $/SF, PSF, cap rate, triple net) need the
#                 two-signal test. Fixes the measured false rejects and adds
#                 no new rejects.
#   "two_signal"  "narrow" plus the property vocabulary below and the
#                 "<building|land|...> only" phrases. Rejects property rows
#                 that used to pass (offices, condos, land, multi-tenant,
#                 investment property, multifamily); see the notes for the
#                 48 snapshot rows this would newly drop.
# Shipped "narrow" (coordinator, 2026-11-12): two_signal waits for the owner
# to review the 59 snapshot rows it would newly drop (j3-cre-gate-notes.md).
CRE_GATE_MODE = "narrow"              # "narrow" | "two_signal" | "legacy"

# One phrase is enough: the listing IS a lease / per-SF rental offering.
_CRE_STRONG_LEGACY_RE = re.compile(
    r'\bfor\s+(?:sub)?lease\b|annual\s*/\s*sf|/\s*sf\s*/\s*(?:yr|year)|'
    r'rentable\s+(?:sf|area)',
    re.IGNORECASE,
)
# One phrase is enough (two_signal only): the listing says it is property only.
_CRE_STRONG_ONLY_RE = re.compile(
    r'\b(?:building|real\s+estate|property|land|space)\s+only\b|'
    r'\bno\s+business\s+(?:included|for\s+sale)\b',
    re.IGNORECASE,
)
# Lease FIGURES: a business quotes its own rent this way ("Monthly Lease Rate:
# $4,200", "$3145 rent includes all NNN"). Property evidence only when the card
# has no operating-business evidence.
_CRE_LEASE_FIGURE_RE = re.compile(
    r'\$\s*[\d.,]+\s*/\s*sf|\bpsf\b|per\s+sf\b|price\s*/\s*sf|cap\s*rate|'
    r'\bnnn\b|triple\s*net|lease\s*rate',
    re.IGNORECASE,
)
# Property vocabulary (two_signal only; these used to pass the gate entirely).
_CRE_PROPERTY_RE = re.compile(
    r'\b(?:office|retail|warehouse|industrial|flex|medical\s+office|'
    r'commercial)\s+(?:space|suites?|condo)\b|'
    r'\b(?:sq\.?\s*ft\.?|square\s+feet|sf)\s+(?:available|for\s+lease)\b|'
    r'\bspace\s+(?:available|for\s+rent)\b|'
    r'\b(?:land|lot|lots|acreage|parcels?|building)\s+for\s+sale\b|'
    r'\bvacant\s+(?:land|lot|building|space|unit)\b|\bbuildable\b|'
    r'\binvestment\s+property\b|\b(?:multi|single)[-\s]tenant\b|'
    r'\btenants?\s+in\s+place\b|\brent\s+roll\b|\boccupancy\s+rate\b|'
    r'\bnet[-\s]leased?\b|\bnoi\b|\bshopping\s+(?:center|centre)\s+for\s+sale\b|'
    r'\b(?:100|\d{2})\s*%\s*(?:leased|occupied)\b|\bfully\s+leased\b|'
    r'\bavailable\s+(?:square\s+feet|sq\.?\s*ft\.?|sf)\b|\bbuilding\s+type\b|'
    r'\bmulti-?family\b|\bapartment\s+(?:complex|building|portfolio)\b|'
    r'\b(?:approved|raw|vacant|undeveloped)\s+land\b|\bland\s+approved\b|'
    r'\bsale\s*(?:or|/)\s*lease\b',
    re.IGNORECASE,
)
_CRE_BUSINESS_RE = re.compile(
    r'cash\s*flow|\bsde\b|\bebitda\b|seller\W?s?\s+discretionary|'
    r'owner\W?s?\s+(?:benefit|earnings|compensation|income)|'
    r'net\s+(?:income|profit)|gross\s+(?:revenue|sales|profit)|\brevenues?\b|'
    r'\b(?:annual|monthly|weekly|yearly|total|gross)\s+sales\b|\bsales\s*:|'
    r'\bprofit(?:s|able)?\b|\bp\s*&\s*l\b|\binventory\b|\bff\s*&\s*e\b|'
    r'\bemploye\w*|\bstaff(?:ed|ing)?\b|\bcustomers\b|\bclients?\b|'
    r'\bbusiness\s+(?:for\s+sale|opportunity)\b|\b(?:asset|stock)\s+sale\b|'
    r'seller\s+financ|\bturn[-\s]?key\b|\bfranchise\b|\bgoodwill\b|'
    r'years?\s+in\s+business|\bestablished\s+(?:in\s+)?(?:\d|since)|'
    r'\bequipment\b|\bmachines\b|\battendants?\b|\boperator\b|'
    r'\b(?:semi[-\s])?absentee\b|\bwash\s*(?:&|and)\s*fold\b|'
    r'\brun\s+(?:the\s+)?business\b|\bpractice\b|\bcompany\b|'
    r'\bgallons?\b|\b(?:inside|fuel|gas|store|merchandise)\s+sales?\b|'
    r'\b(?:space|property|properties|real\s+estate|building|land)\s+included\b',
    re.IGNORECASE,
)


def cre_gate_reason(text):
    """Why a card reads as commercial real estate / a lease, or None.

    Returns "strong:<match>" (one phrase is enough), "property:<match>"
    (lease figure / property vocabulary and no operating-business evidence)
    or, in legacy mode, "legacy:<match>". None means not CRE by this gate."""
    text = text or ""
    if CRE_GATE_MODE == "legacy":
        m = _LEASE_CRE_RE.search(text)
        return f"legacy:{m.group(0)}" if m else None
    wide = CRE_GATE_MODE == "two_signal"
    m = _CRE_STRONG_LEGACY_RE.search(text) or (wide and _CRE_STRONG_ONLY_RE.search(text))
    if m:
        return f"strong:{m.group(0)}"
    m = _CRE_LEASE_FIGURE_RE.search(text) or (wide and _CRE_PROPERTY_RE.search(text))
    if m and not _CRE_BUSINESS_RE.search(text):
        return f"property:{m.group(0)}"
    return None


def looks_like_cre_or_lease(text):
    """True for commercial-RE / lease listings. A business that states its own
    rent / lease rate / NNN / square footage is NOT CRE (see cre_gate_reason)."""
    return cre_gate_reason(text) is not None


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
            and not is_cta_text(longest) \
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
_NON_DETAIL_HREF_TOKENS = (
    "addtofavorites", "favorites","/author/", "/category/", "/tag/", "portfolio_category",
                           "/wp-content/", "javascript:", "mailto:", "tel:")

# Taxonomy archive links that sit INSIDE a listing card, before the card's own
# detail link (real-links, job 3, 2026-11-12). A substring token in
# _DETAIL_HREF_TOKENS ("/listing", "/business") also matches the archive, so
# the archive won:
#   indianabusinessadvisors.com (Advanced Classifieds & Directory Pro): the
#     card's category chip /listing-category-2/manufacturing/ outranked
#     /acadp_listings/<slug>/ - 124 listings stored as 4 category URLs.
#   clandbiz.com (Houzez theme): 8 cards whose first link is the status chip
#     /status/business/ collapsed into one row; the rest lead with /label/<x>/.
# 30 days of snapshot URLs (16,541 distinct): every URL matching this pattern
# is an archive page (18 on 12 domains), none is a listing.
_TAXONOMY_HREF_RE = re.compile(
    r"/(?:status|label|labels|agent|agents|agency|type|city|area|feature|features"
    r"|(?:listing|property|business)[-_](?:category|categories|type|types|tag|tags"
    r"|status|label|location|region|city|area|feature)[^/]*)/",
    re.I)


def _is_taxonomy_href(href):
    path = urlparse(href or "").path
    return bool(_TAXONOMY_HREF_RE.search(path))


_ONCLICK_HREF_RE = re.compile(r"""(?:location\.href|window\.location(?:\.href)?)\s*=\s*["']([^"']+)["']""", re.I)

def _onclick_detail_link(element):
    for el in [element] + element.find_all(attrs={"onclick": True}):
        m = _ONCLICK_HREF_RE.search(el.get("onclick") or "")
        if m and not m.group(1).lower().startswith(("javascript:", "mailto:", "tel:", "#")):
            return m.group(1)
    return None


def _best_detail_link(element, base_url=""):
    """The <a href> most likely to be THIS card's own detail page.

    Prefers a link matching _DETAIL_HREF_TOKENS; among the rest, skips known
    non-detail anchors (byline, category/tag archive); falls back to the
    first live link so behavior is unchanged when nothing better is found.
    """
    candidates = []
    # The card element itself can be the link: <a class="bfs-row"
    # href="/businesses-for-sale/10023/"> around the whole card
    # (companysellers.com - 67 cards, every one stored under the index URL,
    # because find_all() never returns the element it is called on).
    anchors = element.find_all("a", href=True)
    if element.name == "a" and element.get("href"):
        anchors = [element] + anchors
    for a in anchors:
        href = (a.get("href") or "").strip()
        if not href or href.startswith("#"):
            continue
        low = href.lower()
        if low.startswith(("javascript:", "mailto:", "tel:")):
            continue
        candidates.append(href)
    if not candidates:
        return None
    # An image file is a lightbox/thumbnail link, not a detail page, even when
    # its path matches a detail token: benchmarkbusinessgroup.com's card links
    # /businesses/<id>/image1/.../Photo_A.jpg BEFORE /business-for-sale/<slug>
    # (job 3, 2026-11-12). Only demoted: a card whose ONLY link is an image
    # keeps it (atlantarestaurantsbroker, childcaredirect - unchanged).
    # Taxonomy archive chips (_is_taxonomy_href) are demoted the same way.
    pages = [h for h in candidates if not _IMAGE_HREF_RE.search(h)]
    for href in pages:
        if (any(tok in href.lower() for tok in _DETAIL_HREF_TOKENS)
                and not _is_taxonomy_href(href)):
            return href
    for href in pages:
        if (not any(tok in href.lower() for tok in _NON_DETAIL_HREF_TOKENS)
                and not _is_taxonomy_href(href)):
            return href
    return candidates[0]


def _enclosing_link(element, max_up=4):
    """The <a href> the card sits inside, when the card has no link of its own.

    Cards built as <a href="/detail"><div>...</div></a> are matched by a
    selector on an inner div (jobahcri.com: div.flex.flex-1.flex-col.p-6
    inside <a href="/listings/<slug>">; baltimorebusinessbrokers.com:
    div.row inside <a href="/business-detail/<id>">), so every card was
    stored under the index URL. The enclosing anchor is the card's own click
    target - unambiguous, unlike the sibling/wrapper search in
    _wrapper_detail_link, which stays opt-in per domain. Only called when the
    card itself holds no usable <a href>."""
    node = element
    for _ in range(max_up):
        node = node.parent
        if node is None or node.name in ("body", "html", "[document]"):
            return None
        if node.name == "a":
            href = (node.get("href") or "").strip()
            low = href.lower()
            if (not href or href.startswith("#")
                    or low.startswith(("javascript:", "mailto:", "tel:"))):
                return None
            return href
    return None


_IMAGE_HREF_RE = re.compile(r"\.(?:jpe?g|png|gif|webp|svg|bmp|tiff?)(?:[?#]|$)", re.I)


# Brokers whose list-page card carries no <a href> of its own: the link sits on
# a wrapper or a sibling (e.g. a mobile twin `div.mob` next to the desktop
# block), or only in an attribute. Without it every card was saved under the
# search-page URL, the insert guard quarantined them, and the real rows were
# never refreshed (restaurantrealty.com: 189 published rows unseen since
# 2026-09-13). Opt-in per domain so no other broker's link choice changes.
# calhouncompanies.com (2026-11-11): the whole card is <a class="node business
# teaser" href="/business/..."> around div.business-info.group, so every row
# was saved under find-a-business?page=N (11 www URLs, ~96 rows keyed on title).
WRAPPER_LINK_DOMAINS = {"restaurantrealty.com", "calhouncompanies.com"}
_ATTR_LISTING_HREF_RE = re.compile(r"""((?:https?://[^\s"'<>]+)?/listing/\?listing=[^\s"'<>]+)""", re.I)


def _unique_detail_hrefs(node):
    seen = []
    anchors = node.find_all("a", href=True)
    if node.name == "a" and node.get("href"):
        anchors = [node] + anchors
    for a in anchors:
        href = (a.get("href") or "").strip()
        low = href.lower()
        if (href and any(tok in low for tok in _DETAIL_HREF_TOKENS)
                and not any(tok in low for tok in _NON_DETAIL_HREF_TOKENS)
                and href not in seen):
            seen.append(href)
    return seen


def _wrapper_detail_link(element, base_url=""):
    """Find THIS card's detail link outside the card itself. Only accepts a
    link when exactly one distinct detail href is in scope, so a wrapper that
    holds several cards can never hand one card another card's URL."""
    bare = urlparse(base_url).netloc.lower().removeprefix("www.")
    if bare not in WRAPPER_LINK_DOMAINS:
        return None
    # 1. a listing URL kept in an attribute (data-href, data-url, onclick...)
    for el in [element] + element.find_all(True):
        for v in el.attrs.values():
            v = " ".join(v) if isinstance(v, list) else str(v)
            m = _ATTR_LISTING_HREF_RE.search(v)
            if m:
                return m.group(1)
    # 2. immediate siblings (desktop/mobile twin blocks)
    for sib in (element.find_previous_sibling(), element.find_next_sibling()):
        if sib is not None and hasattr(sib, "find_all"):
            hrefs = _unique_detail_hrefs(sib)
            if len(hrefs) == 1:
                return hrefs[0]
    # 3. up to three wrappers, stopping as soon as a wrapper spans >1 listing
    node = element
    for _ in range(3):
        node = node.parent
        if node is None or node.name in ("body", "html", "[document]"):
            break
        hrefs = _unique_detail_hrefs(node)
        if len(hrefs) == 1:
            return hrefs[0]
        if len(hrefs) > 1:
            break
    return None


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
                             is_junk_title, title_from_slug, is_site_page,
                             listing_url_pattern_filter, is_cta_text)
except ImportError:  # allow import as a package (scrapers.dealledger_scraper_v6)
    from scrapers.junk_filter import (is_listing_junk as is_junk_listing,
                                      is_sold_or_pending, is_junk_title, title_from_slug,
                                      is_site_page, listing_url_pattern_filter, is_cta_text)


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


# Labelled card status field (job 3, 2026-11-12). Some brokers print the deal's
# status as a labelled field on the card - lbaweb.com: <span class="tag">Status:
# <em>Sold</em></span> on 382 of 593 cards - which looks_sold() never sees (the
# span's class is "tag", the value's "text-secondary") and is_sold_or_pending()
# cannot see (it reads the title). The value is classified by the shared phrase
# set (scrapers/listing_status.py, decisions 5 + 6, principle 15).
#
# Precision rules (principle 8 - measured over ~1,000 cached broker pages, see
# claude/staging/2026-11-12/j3-lbaweb-notes.md):
#  - the label is its own text node at the start of a line: "Status:",
#    "Listing Status:", "Availability:" ... with the COLON required; the value
#    is the rest of that node or the next text node. A bare "Status" heading
#    is a filter widget on several sites, and "status" in prose is never read.
#  - the value must be an exact phrase of the shared set (lookup_status);
#    anything else ("Withdraw", "Owner retiring") is not evidence -> None.
#  - a plain active value ("Available", "For Sale") is returned but never
#    overrides the generic sold rules (see from_card): it adds nothing.
_STATUS_FIELD_LABEL_RE = re.compile(
    r"^(?:(?:listing|business|deal|sale|property)\s+)?(?:status|availability)"
    r"\s*:\s*(.*)$", re.I)
try:
    from listing_status import lookup_status as _sf_lookup, more_final as _sf_more_final
except Exception:                    # module not present (j3 not applied): rule is inert
    _sf_lookup = _sf_more_final = None
_SF_DROP = "DROP"                    # == listing_status.DROP
# domain -> label -> {(title, detail url)} of cards not emitted (Coming Soon /
# For Lease field); a set, because a page's cards can be extracted more than
# once. Printed per broker by the crawl loop so a drop is never silent.
_STATUS_FIELD_DROPS = defaultdict(lambda: defaultdict(set))


def card_status_field(element, domain=None):
    """(status, label) from a labelled Status / Availability field on a card,
    or None when the card has no such field or its value is not a known
    status phrase. status is 'sold' / 'active' / 'inactive' / listing_status.DROP;
    label is the broker's verbatim value (None for a plain active value).
    Several fields on one card: the most final status wins."""
    if _sf_lookup is None or element is None:
        return None
    lines = [ln for ln in (t.strip() for t in element.get_text("\n").split("\n")) if ln]
    best = None
    for i, ln in enumerate(lines):
        m = _STATUS_FIELD_LABEL_RE.match(ln)
        if not m:
            continue
        value = m.group(1).strip() or (lines[i + 1] if i + 1 < len(lines) else "")
        hit = _sf_lookup(value, domain) if value else None
        if hit is None:
            continue
        if best is None or _sf_more_final(hit[0], best[0]) != best[0]:
            best = hit
    return best


# Status COLUMN of a listing table (job 3, table rows, 2026-11-12). A <tr> card
# carries its status as a plain cell under a "Status" column header -
# kendallcapitalgroup.com: Sold / Active / Under LOI as the first cell of 12
# rows; accountingfirmsold.com: "Available" / "Sold" / "Sale Pending" /
# "On Hold" in a data-th="Status" cell of 214 rows. Neither is a badge, a class
# or a "Status:" label, so every row was written status='active'.
# Precision (principle 8): only a cell whose OWN column header is a status
# header (the table's <th> row, or the cell's data-th / data-title /
# data-label attribute) is read, and its value must be an exact phrase of the
# shared set (listing_status.lookup_status) - the whole cell, or one line of
# it ("Available<br>Request Info" -> "Available"). Prose cells are never read.
_STATUS_COLUMN_HEADERS = {"status", "listing status", "availability", "sale status",
                          "deal status", "business status"}
_CELL_HEADER_ATTRS = ("data-th", "data-title", "data-label", "data-colname")


def _norm_header(text):
    return re.sub(r"[^a-z]+", " ", (text or "").lower()).strip()


def _status_column_cell(row):
    """The <td>/<th> of this <tr> under a status column header, or None."""
    cells = row.find_all(["td", "th"], recursive=False)
    for c in cells:
        for attr in _CELL_HEADER_ATTRS:
            if c.get(attr) and _norm_header(c.get(attr)) in _STATUS_COLUMN_HEADERS:
                return c
    table = row.find_parent("table")
    if table is None:
        return None
    header = None
    for tr in table.find_all("tr"):
        if tr.find_parent("table") is not table:
            continue
        if tr.find_all("th", recursive=False) and not tr.find_all("td", recursive=False):
            header = tr
        break                               # only the table's FIRST row can be its header
    if header is None or header is row:
        return None
    heads = header.find_all("th", recursive=False)
    if len(heads) != len(cells) or any(h.get("colspan") for h in heads) \
            or any(c.get("colspan") for c in cells):
        return None                         # columns would not line up
    for h, c in zip(heads, cells):
        if _norm_header(h.get_text(" ", strip=True)) in _STATUS_COLUMN_HEADERS:
            return c
    return None


def table_status_cell(element, domain=None):
    """(status, label) from a listing table's Status column, or None - same
    contract as card_status_field(): status 'sold' / 'active' / 'inactive' /
    listing_status.DROP, label = the broker's verbatim cell text (None for a
    plain active value). Only for <tr> cards."""
    if _sf_lookup is None or element is None or getattr(element, "name", None) != "tr":
        return None
    cell = _status_column_cell(element)
    if cell is None:
        return None
    lines = [ln for ln in (t.strip() for t in cell.get_text("\n").split("\n")) if ln]
    for value in [" ".join(lines)] + lines:
        hit = _sf_lookup(value, domain) if value else None
        if hit is not None:
            return hit
    return None



# ── Generic card status through the shared policy (job 3, decision 6) ──────
# Before: sold = looks_sold(element) or is_sold_or_pending(title), so an
# "Under Contract" / "Sale Pending" / "Off Market" badge or title was written
# status='sold'. Decision 6: that family is still ACTIVE and the broker's words
# are kept verbatim (broker_status_text); only SOLD is sold; Coming Soon and
# For Lease are not emitted (decision 5). The phrase -> status mapping is
# scrapers/listing_status.py; without it (j3 not applied) the old rule stands.
# Detection is unchanged in scope: the same badge/class/ribbon/img places
# looks_sold() reads, and title markers in the shapes is_sold_or_pending()
# already accepts, plus a leading/trailing marker in any case when
# punctuation sets it off ("Sold! Pizzeria", "ATM Route – $29,000 – Sold!").
try:
    from listing_status import (lookup_status as _ls_lookup, more_final as _ls_more_final,
                                DROP as _LS_DROP)
except Exception:          # module absent: legacy rule (principle 15 fallback)
    _ls_lookup = None
    _LS_DROP = "DROP"

# Title marker phrases (case-insensitive). Deliberately narrower than the
# module's table: free-text words such as "closed", "lease", "hold", "new"
# are statuses in a status field but ordinary words in a title
# ("Closed Gas Station Opportunity", "… - New Lease").
_TM_PHRASE = (r"(?:just[\s-]*sold|recently[\s-]*sold|just[\s-]*closed|sold"
              r"|under[\s-]*contract|contract[\s-]*pending|sale[\s-]*pending|pending[\s-]*sale"
              r"|pending|under[\s-]*loi|loi[\s-]*accepted|loi|under[\s-]*offer"
              r"|has[\s-]*accepted[\s-]*offer|accepted[\s-]*offer|offer[\s-]*accepted"
              r"|under[\s-]*agreement|(?:temporarily[\s-]+)?off[\s-]*market"
              r"|coming[\s-]*soon|for[\s-]*lease|leased)")
_TM_PUNCT = r"[!:\-–—…|.,·/]"
# "SOLD! Pizzeria", "Sold: Pharmacy", "Sold…Law firm", "UNDER LOI -- Home Care"
_TM_LEAD_RE = re.compile(r"^[\s*\"'“]*(" + _TM_PHRASE + r")[\s*]*" + _TM_PUNCT, re.I)
# "Route – $29,000 – Sold!", "Car Wash | Coming Soon", "… / Off Market", "… *SOLD*"
_TM_TRAIL_RE = re.compile(r"(?:" + _TM_PUNCT + r"|\s\*)\s*\*?\s*(" + _TM_PHRASE
                          + r"|sold\s+(?:for|at)\s*\$?\s*[\d][\d,.\s$]*(?:k|m|mm|million)?)[\s*!.]*$",
                          re.I)
# "(SOLD)", "(Under Contract)", "(LOI)", "[Pending]" anywhere
_TM_PAREN_RE = re.compile(r"[(\[]\s*([^()\[\]]{2,40}?)\s*[)\]]")
# unambiguous status phrases anywhere, any case ("PENNSYLVANIA SKC-1562 UNDER
# CONTRACT", "… Temporarily Off Market IN THE HEART …") - the same set
# is_sold_or_pending() reads anywhere, plus the rest of the contract family
_TM_ANY_RE = re.compile(r"\b(under[\s-]*contract|contract[\s-]*pending|sale[\s-]*pending"
                        r"|under[\s-]*loi|has[\s-]*accepted[\s-]*offer|accepted[\s-]*offer"
                        r"|(?:temporarily[\s-]+)?off[\s-]*market|just[\s-]*closed"
                        r"|recently[\s-]*sold)\b", re.I)
_TM_CAPS_SOLD_RE = re.compile(r"\bSOLD\b")          # all-caps SOLD anywhere (today's rule)
_TM_TEMPORARILY = re.compile(r"^temporarily[\s-]+", re.I)
# Owner decision 11: an any-case SOLD marker ("Sold! Pizzeria", "Sold:
# Pharmacy", "Route – Sold!") decides status only at TITLE_STATUS_MARKER_DOMAINS.
# Elsewhere a title is sold only where HEAD's is_sold_or_pending() says so
# (all-caps SOLD, "(sold …)", "– Sold" suffix, just closed, recently sold).
# True = extend the any-case rule to every broker (27 rows at 5 brokers on
# the 2026-11-11 snapshot, j3-v6status-measure.csv) - an owner decision.
ANYCASE_SOLD_MARKERS = False
# A "title" that is only a status phrase plus a generic word is a section
# heading scraped as a card ("Under Contract Listings", "OFF-Market Listing"),
# not a listing: it keeps today's status rather than becoming active.
_TM_HEADING_RESIDUE = {"", "listing", "listings", "business", "businesses", "deals", "properties"}
# "Sold With Real Estate", "Sold As-Is" are descriptions, not markers (same
# spare as junk_filter.is_sold_or_pending)
_TM_SPARE = re.compile(r"\bsold\s+(?:with|as[\s-]?is)\b", re.I)


def _tm_lookup(phrase, domain):
    """lookup_status for a title marker; 'temporarily off market' -> the
    'off market' entry, label kept verbatim."""
    hit = _ls_lookup(phrase, domain)
    if hit is None and _TM_TEMPORARILY.match(phrase or ""):
        core = _ls_lookup(_TM_TEMPORARILY.sub("", phrase), domain)
        if core:
            hit = (core[0], re.sub(r"\s+", " ", phrase).strip())
    if hit is None and re.match(r"\s*sold\b", phrase or "", re.I):
        hit = ("sold", re.sub(r"\s+", " ", phrase).strip())     # "(SOLD & SETTLED)"
    return hit


def title_status(title, domain=None):
    """(status, verbatim marker) for a status marker in a card title, or None.

    status is 'sold' | 'active' (under-contract family, label kept) | DROP
    (Coming Soon / For Lease). The most final marker wins: "(SOLD) Under
    Contract" is sold. Requires listing_status; None without it.
    """
    t = (title or "").strip()
    if not t or _ls_lookup is None or _TM_SPARE.search(t):
        return None
    hits = []
    for rx in (_TM_LEAD_RE, _TM_TRAIL_RE):
        m = rx.search(t)
        if m:
            hits.append(_tm_lookup(m.group(1).strip(), domain))
    for m in _TM_PAREN_RE.finditer(t):
        h = _tm_lookup(m.group(1), domain)
        if h and h[0] != "inactive" and h[1] is not None:   # a status, not "(Sundays)"
            hits.append(h)
    for m in _TM_ANY_RE.finditer(t):
        hits.append(_tm_lookup(m.group(1), domain))
    if _TM_CAPS_SOLD_RE.search(t):
        hits.append(("sold", "SOLD"))
    hits = [h for h in hits if h and h[0] in ("sold", "active", _LS_DROP)]
    if not (ANYCASE_SOLD_MARKERS or is_sold_or_pending(t)
            or (domain or "").lower().removeprefix("www.") in TITLE_STATUS_MARKER_DOMAINS):
        hits = [h for h in hits if h[0] != "sold"]           # decision 11
    if not hits:
        return None
    return max(hits, key=lambda h: {"active": 0, _LS_DROP: 2, "sold": 3}[h[0]])


def badge_status(element, domain=None):
    """looks_sold()'s detection, returning (status, verbatim badge text) via
    listing_status instead of a bool; None when no badge/class/img marker."""
    if _ls_lookup is None:
        return ("sold", None) if looks_sold(element) else None
    hits = []
    for el in [element, *element.find_all(True)]:
        for c in el.get("class", []) or []:
            m = _SOLD_CLASS_RE.search(c)
            if m:
                hits.append(_ls_lookup(m.group(1), domain) or ("sold", c))
    for el in element.select("[class*='badge'], [class*='ribbon'], [class*='status'], "
                             "[class*='label'], [class*='flag'], [class*='overlay']"):
        txt = el.get_text(" ", strip=True)
        m = _SOLD_TEXT_RE.search(txt)
        if m:
            hits.append(_ls_lookup(txt, domain) or _ls_lookup(m.group(1), domain)
                        or ("sold", m.group(1)))
    for img in element.find_all("img"):
        alt = f"{img.get('alt','')} {img.get('title','')}".strip()
        m = _SOLD_TEXT_RE.search(alt)
        if m:
            hits.append(_ls_lookup(alt, domain) or _ls_lookup(m.group(1), domain)
                        or ("sold", m.group(1)))
    hits = [h for h in hits if h and h[0] in ("sold", "active", _LS_DROP)]
    if not hits:
        return None
    return max(hits, key=lambda h: {"active": 0, _LS_DROP: 2, "sold": 3}[h[0]])


def card_status(element, title, domain):
    """(status, broker_status_text-or-None) for a generic V6 card.

    Order: a per-domain rule (TITLE_STATUS_MARKER_DOMAINS, decision 11 - and
    any STATUS_FIELD_DOMAINS rule, which belongs in this same slot) decides
    alone; otherwise the most final of the badge and the title marker, mapped
    through listing_status. status DROP = do not emit. Without the module:
    today's rule (looks_sold or is_sold_or_pending -> sold), no label.
    """
    marker = domain_title_status_marker(domain, title)
    if marker:
        return title_marker_status(marker), marker
    if _ls_lookup is None:
        return ("sold" if (looks_sold(element) or is_sold_or_pending(title))
                else "active"), None
    hits = [h for h in (badge_status(element, domain), title_status(title, domain)) if h]
    if not hits:
        # never lose a sold that today's rule finds and the shapes above miss
        return ("sold", None) if is_sold_or_pending(title) else ("active", None)
    status, label = max(hits, key=lambda h: {"active": 0, _LS_DROP: 2, "sold": 3}[h[0]])
    if status == "active" and label and _is_status_heading(title):
        return ("sold" if (looks_sold(element) or is_sold_or_pending(title))
                else "active"), None
    return status, label


def _is_status_heading(title):
    """'Under Contract Listings' -> True: nothing but status words + a generic noun."""
    rest = _TM_ANY_RE.sub(" ", title or "")
    rest = re.sub(_TM_PHRASE, " ", rest, flags=re.I)
    rest = re.sub(r"[^a-z]+", " ", rest.lower()).strip()
    return rest in _TM_HEADING_RESIDUE


def restatus_from_title(listing):
    """After the title was REPLACED (stored-row reuse, detail <h1>): read the
    final title's status marker. Only ever raises the status (active -> sold),
    or adds the label to an unlabelled active row - never reopens a sold row,
    never drops a row here. mnresortsalesnorth.com: card "Red Wing Resort",
    detail h1 "Red Wing Resort (SOLD)" - status had been decided on the card
    title, so 16 (SOLD) rows stayed active for 40 nightly runs."""
    if listing.get("_status_from_field"):
        return listing          # a labelled "Status:" field decided it (card_status_field)
    t = listing.get("title")
    if _ls_lookup is None:
        if listing.get("status", "active") == "active" and is_sold_or_pending(t):
            listing["status"] = "sold"
        return listing
    hit = title_status(t, listing.get("broker_domain"))
    if not hit or hit[0] == _LS_DROP:
        return listing
    cur = listing.get("status") or "active"
    if hit[0] == "sold" and cur == "active":
        listing["status"] = "sold"
        if hit[1]:
            listing["broker_status_text"] = hit[1]
    elif hit[0] == "active" and cur == "active" and hit[1] and not listing.get("broker_status_text"):
        listing["broker_status_text"] = hit[1]
    return listing


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


_SIMPLE_TR_SELECTOR_RE = re.compile(r"^tr(?:\.[A-Za-z0-9_-]+)+$")


def widen_striped_rows(html, selector, base_url=""):
    """
    A zebra-striped table emits two row classes - `tr.odd` / `tr.even`, or an
    ASP.NET GridView's RowStyle / AlternatingRowStyle - and the detector groups
    rows by exact class, so it picks ONE of them and every other listing in
    the table is never read. commercialroutesforsale.com: 61 rows split
    31 `even` / 30 `odd`, the cache held `tr.even`, and every crawl saw half
    the table (2026-11-11).

    Given a single-class-set `tr` selector, add the other classed `tr`
    siblings under the same parent(s) whose rows are mostly real listings
    (is_listing_element). Returns a comma selector (document order is kept by
    select()), or the selector unchanged. Rows only: a div/li grid's other
    sibling classes are usually featured/sold variants, which is a different
    decision. Each matched row still goes through from_card()'s own gates.
    """
    if not selector or not _SIMPLE_TR_SELECTOR_RE.match(selector):
        return selector
    try:
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select(selector)
    except Exception:
        return selector
    if len(rows) < PatternDetector.MIN_LISTING_ELEMENTS:
        return selector
    parents = {id(r.parent): r.parent for r in rows if r.parent is not None}
    groups = defaultdict(list)
    for parent in parents.values():
        for sib in parent.find_all("tr", recursive=False):
            classes = sib.get("class", [])
            if not classes:
                continue
            sel = css_selector("tr", classes)
            if sel != selector:
                groups[sel].append(sib)
    extra = []
    for sel, sibs in groups.items():
        if _is_nav_selector(sel):
            continue
        n_ok = sum(1 for el in sibs if is_listing_element(el, base_url))
        if n_ok >= 1 and n_ok * 2 >= len(sibs):
            extra.append(sel)
    if not extra:
        return selector
    return ", ".join([selector] + sorted(extra))


_SOLD_SECTION_RE = re.compile(r"\b(?:sold|closed|completed)\b", re.I)
_FOR_SALE_RE = re.compile(r"\bfor\s+sale\b|\bavailable\b", re.I)


def _table_heading(table):
    h = table.find_previous(["h1", "h2", "h3", "h4", "h5", "h6"])
    return h.get_text(" ", strip=True) if h else ""


def _is_sold_section(table):
    """'Recent Businesses Sold', 'Recently Closed Transactions' - and not
    'Businesses For Sale' / 'Available'."""
    head = _table_heading(table)
    return bool(head and _SOLD_SECTION_RE.search(head) and not _FOR_SALE_RE.search(head))


def _table_row_candidates(soup):
    """[(selector, data rows)] for PatternDetector Strategy 1c: one entry per
    distinct listing-table selector whose tables are all plain data tables
    (see the Strategy 1c comment). The selector names the table by id (else
    its classes) and takes its OWN rows only (child combinators), so a table
    pasted into a description cell is not read as rows of the listing table."""
    out, seen = [], set()
    for table in soup.find_all("table"):
        tid = table.get("id") or ""
        if re.fullmatch(r"[A-Za-z][\w-]*", tid):
            tsel = f"table#{tid}"
        elif table.get("class"):
            tsel = css_selector("table", table["class"])
        else:
            continue
        if tsel in seen:
            continue
        seen.add(tsel)
        try:
            tables = soup.select(tsel)
        except Exception:
            continue
        rows, layout = [], False
        for t in tables:
            if (t.find_parent("table") or _in_site_chrome(t) or _is_nav_selector(tsel)
                    or _is_sold_section(t)):
                layout = True
                break
            own = [r for r in t.find_all("tr") if r.find_parent("table") is t
                   and r.find_all("td", recursive=False)]
            # a layout table: most of its rows hold another table
            if own and sum(1 for r in own if r.find("table")) * 2 >= len(own):
                layout = True
                break
            # a data row names something: a fee schedule or a figures-only
            # table ("$1,000,000 | $80,000 | $120,000") is not a listing table
            rows += [r for r in own
                     if len(_WORD3_RE.findall(r.get_text(" ", strip=True))) >= 3]
        if layout or len(rows) < PatternDetector.MIN_LISTING_ELEMENTS:
            continue
        out.append((f"{tsel} > tr, {tsel} > tbody > tr", rows))
    return out


_WORD3_RE = re.compile(r"[A-Za-z]{3,}")


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
    r"my account|newsletter|cookie (?:policy|settings|preferences|consent|notice)|"
    r"privacy policy|terms of)\b",
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
    # A table's header row (<th> cells only) is never a listing - kendall's
    # header links its column sorts (?sort=view_status), which read as detail
    # links (job 3, table rows).
    if element.name == "tr" and not element.find_all("td", recursive=False):
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
    if has_price or has_detail_link(element, base_url):
        return True
    # WRAPPER_LINK_DOMAINS: the card's link sits on a wrapper, so a card with
    # no parseable price ("Make Offer", "79,000", "$120,00O") was dropped -
    # 8 of restaurantrealty.com's 124 listings on every crawl (2026-11-11).
    # The wrapper also holds the agent / email / status-badge blocks, so the
    # link counts only for the block whose title is the one the URL names,
    # and only for the outermost such block (not a title-only copy nested in
    # an identical container, which would add a second card per listing).
    if any(p.name == element.name and p.get("class") == element.get("class")
           for p in element.parents):
        return False
    wrapped = _wrapper_detail_link(element, base_url)
    return bool(wrapped) and _title_matches_link(title, wrapped)


class WrapperSkipLog:
    """Collects drop_wrapper_matches() skips over one broker run so the
    '🧺 wrapper skipped' line prints once per broker, not once per page parse
    (peak printed it on every parse: page 1, pagination probes, re-reads).
    The figure is the most skipped in any one parse — the same page parsed
    three times is still one wrapper, so the counts are not summed."""

    def __init__(self):
        self.max_per_parse = 0
        self.parses = 0

    def add(self, n):
        if n:
            self.parses += 1
            self.max_per_parse = max(self.max_per_parse, n)

    def summary(self):
        if not self.parses:
            return None
        return (f"   🧺 {self.max_per_parse} wrapper match(es) holding other cards skipped"
                f" (seen in {self.parses} page parse(s))")


def drop_wrapper_matches(els, is_card=None, min_inner=3):
    """Drop selector matches that hold several other matches which are cards.

    An element holding several matched listing cards is the list wrapper, not
    a card. peakbusinessbrokersllc.com's selector (div.elementor-container...)
    matches the page section that holds all 33 listings as well as each
    listing. Until 2026-11-12 that wrapper died in from_card() only by
    accident: its text included the Torrance card's "Monthly Lease Rate",
    which the old CRE gate rejected. With the gate narrowed, the wrapper
    emitted a 34th row (a 24,508-character "card" titled from the first
    listing inside it).

    Deliberately narrow (principle 8): dropping every match that merely
    contains other matches lost real listings on paradigmbb.com (17),
    buffalobusinessbrokers.com (4) and buysellgeo.com (2), whose cards hold
    nested same-selector blocks. A match is dropped only when it holds at
    least `min_inner` SIDE-BY-SIDE matches that are themselves cards
    (is_card(e) true, and no other such card is their ancestor).
    Returns (kept, dropped_count)."""
    ids = {id(e): e for e in els}
    inner = defaultdict(list)
    for e in els:
        for p in e.parents:
            if id(p) in ids:
                inner[id(p)].append(e)
    drop = set()
    for pid, kids in inner.items():
        if len(kids) < min_inner:
            continue
        cards = [k for k in kids if (is_card(k) if is_card else True)]
        # only side-by-side cards count: a chain of nested matches that all
        # read as the same card (buysellgeo's div[class*="elementor"]) is one
        card_ids = {id(k) for k in cards}
        top = [k for k in cards if not any(id(p) in card_ids for p in k.parents)]
        if len(top) >= min_inner:
            drop.add(pid)
    kept = [e for e in els if id(e) not in drop]
    return kept, len(els) - len(kept)


def _cta_site_link(element, title, text, detail_url, base_url):
    """True if a card that is_site_page() rejects only because of its LINK
    is a real listing: the link is a same-site page (contact, homepage, ...),
    no anchor carries the card's title (the link is a button, not the
    listing's name), the title itself is neither a site-page title nor a
    price fragment, and the card states a price. Anything short of that
    keeps the site-page drop."""
    if not detail_url or not title or is_site_page(title, ""):
        return False
    if parse_all_prices(title):      # a price fragment picked as the title
        return False
    bare = lambda h: (h or "").lower().split(":")[0].removeprefix("www.")
    if bare(urlparse(detail_url).netloc) != bare(urlparse(base_url).netloc):
        return False
    tnorm = re.sub(r"\W+", " ", title).strip().lower()
    for a in element.find_all("a", href=True):
        at = re.sub(r"\W+", " ", a.get_text(" ", strip=True)).strip().lower()
        if at and tnorm and (tnorm in at or at in tnorm) and len(at) >= 6:
            return False
    return any(v >= 1_000 for v in parse_all_prices(text))


def _title_matches_link(title, href):
    """True if the listing URL's slug (the text after '<number>-' in its last
    path segment or query value) is the start of the title, or vice versa.
    Slugs are often cut short at an apostrophe ("5421-Phil")."""
    import html as html_lib
    from urllib.parse import unquote
    pu = urlparse(href or "")
    # everything after the first '=': titles in these URLs contain '&'
    raw = unquote(pu.query.split("=", 1)[-1] if pu.query
                  else pu.path.rstrip("/").split("/")[-1])
    slug = re.sub(r"[^a-z0-9]", "", re.sub(r"^\s*\d+-", "", html_lib.unescape(raw)).lower())
    norm = re.sub(r"[^a-z0-9]", "", html_lib.unescape(title or "").lower())
    if len(slug) < 4 or len(norm) < 4:
        return False
    return norm.startswith(slug[:40]) or slug.startswith(norm[:40])



# Identity key for brokers whose detail URL embeds mutable text.
# restaurantrealty.com links /listing/?listing=<number>-<full title>, and the
# title carries the price ("$750k Down"), so a broker's price edit produced a
# new URL and a new id: 682 superseded rows on 156 URLs (2026-11-11). The
# BBCRM backend resolves a listing on the number alone (the title part is
# ignored), so the number is the identity. Only the id key changes; the
# stored url stays the link the site itself publishes. Opt-in per domain.
ID_KEY_RULES = {
    "restaurantrealty.com": (re.compile(r"[?&]listing=(\d+)"),
                             "https://restaurantrealty.com/listing/?listing={}"),
}


def listing_id_key(url):
    """url reduced to its stable identity form for ID_KEY_RULES domains."""
    if not url:
        return url
    host = urlparse(url).netloc.lower()
    rule = ID_KEY_RULES.get(host[4:] if host.startswith("www.") else host)
    if not rule:
        return url
    m = rule[0].search(url)
    return rule[1].format(m.group(1)) if m else url


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
        key = listing_id_key(url)
    else:
        key = f"{broker_domain}|{(title or '').strip().lower()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# ── Synthetic URLs for cards with no detail link (2026-11-11, rev 2026-11-12) ─
# ~100 brokers render every listing inline with no per-listing page, so every
# card used to be stored with url = the index URL (peakbusinessbrokersllc.com:
# 33 cards on the page, 1 distinct URL in listings_direct).
#
# GATED (owner decision 4, 2026-11-12): only the brokers in
# SYNTHETIC_URL_DOMAINS (bucket (b) of collapse/link_classification.csv) get
# synthetic URLs and the identity below; BROKER_REF_ONLY_DOMAINS get the
# broker-ref rule and nothing else. Every other broker is unchanged:
# url = index URL, id = sha256(domain|title.lower()).
#
# For an allowlisted card with no detail link (decisions 1-2):
#   - the card prints a broker reference number (Listing # 1088, ID: 875354,
#     FL0914, CDT 123, ES-01, ...):
#         url = <index_url>#dl-<slug(ref)>        id = sha256(url)[:16]
#     (stable_listing_id() keyed on that url, as for any detail URL)
#   - no ref, location parsed:
#         url = <index_url>#dl-<slug(title)>-<slug(city-state)>
#         id  = sha256(domain|title.lower()|slug(city-state))[:16]
#   - no ref, no location: id = sha256(domain|title.lower())[:16] - today's id,
#     so an allowlisted broker that prints no locations keeps its ids; the URL
#     is <index_url>#dl-<slug(title)>.
# Built only from what the card prints (ref, title, city, state): no price
# (decision 3 - a price is not identity, principle 4), no row order, dates or
# counters. A slug collision between two different ids is logged and left.
#
# url_is_listing_specific stays False: a fragment is not a per-listing page,
# and DB triggers / listing_dates read that flag as "a real page".
#
# The URL (and, for a ref card, the final id) is assigned at ONE point -
# assign_synthetic_urls() at the END of _scrape_broker_inner and of the API
# branch - so the site-page / listing-URL-pattern gates and the pagination
# guard still see the plain index URL. The card's id is set in from_card /
# _make_api_listing (same helper for both writers, principle 15) so the page
# loop and the same-id dedupe already see the new identity.
SYNTHETIC_SLUG_MAX = 80
SYNTHETIC_FRAGMENT = "dl-"
_SYNTH_FLAG = "_no_detail_link"

# Bucket (b), link_classification.csv (2026-11-11): every listing inline, no
# per-listing link. Bare hosts; matched with and without "www.".
SYNTHETIC_URL_DOMAINS = frozenset({
    "accountingfirmsold.com", "americandentallabsales.com",
    "blackbookadvisers.com", "bostonrestaurantgroup.com", "bsalesgroup.com",
    "buffalobusinessbrokers.com", "buysellgeo.com",
    "cannabisbusinessbrokers.net", "coinlaundrysales.com",
    "commercialindustrialconsultants.com", "corpinvestnm.com",
    "dental360advisors.com", "dentalbroker.com", "elitesalonsales.com",
    "feldmanbrokerage.com", "focal5.com", "goodmanonline.com",
    "laundromat-broker.com", "listyourhomesocal.com",
    "medicalpracticesusa.com", "onesbs.com", "patriotbusinessadvisors.com",
    "peakbusinessbrokersllc.com", "philsellsbiz.com",
    "pinnaclepartnerservice.com", "practicebrokers.com", "reedtindal.com",
    "richbizbrokers.com", "selahdirective.com", "shermangroup.net",
    "thesynergybroker.com", "vermontbusinessbrokers.com",
    "vistapoolroutes.com",
})
# Bucket (c): the broker-ref rule only. naz-business.com prints a 4-digit ref
# on all 81 cards; 16 link out to BizBuySell (never stored - marketplace gate),
# the rest have no link. A card with no ref keeps today's behaviour.
BROKER_REF_ONLY_DOMAINS = frozenset({"naz-business.com"})

# Refs printed WITHOUT a label, per broker (the generic rule below never
# guesses at a bare number). Each one checked against the live page 2026-11-12.
BROKER_REF_PATTERNS = {
    # table row opens with the Listing Number column: "CA2633 Los Angeles ...",
    # also printed "OH 2083" and "WI1084a"
    "accountingfirmsold.com": re.compile(r"^\s*([A-Z]{2}\s?\d{3,5}[A-Za-z]?)\b"),
    # GoDaddy card "2246 Asking Price: $ 200,000": the ref is the field
    # immediately before the price label
    "naz-business.com": re.compile(r"(?:^|\s)(\d{4})\s+Asking\s+Price\s*:", re.I),
    # "CDT 123" practice codes
    "dentalbroker.com": re.compile(r"\b(CDT\s*#?\s*\d{2,5})\b"),
    # confidential codes "ES-01", "HF-02", "TA-03"
    "selahdirective.com": re.compile(r"\b([A-Z]{2}-\d{2})\b"),
}
# Labelled refs, any enabled broker: a label, then one short alnum token.
# "Listing #1088", "Listing No. 12", "Listing ID DS8887", "ID: 875354",
# "Ref # 44", "#3070". Never a price ($ / thousands separators), a phone
# number (digits continue after - . or space), a licence/suite number or a
# year (1950-2035).
_REF_TOKEN = r"([A-Z]{0,4}-?\d{2,7}[A-Z]?)(?![\w,]|[-.\s]?\d)"
_BROKER_REF_LABELLED = re.compile(
    r"(?:\blisting\s*(?:#|no\b\.?|number\b|id\b)|\bref(?:erence)?\s*(?:#|no\b\.?|number\b|:)"
    r"|\bID\s*[:#])\s*[:#.]?\s*" + _REF_TOKEN, re.I)
_BROKER_REF_HASH = re.compile(r"(?<![\w$#&])#\s?(\d{3,6})(?![\w,]|[-.\s]?\d)")
_REF_NOT_AFTER = re.compile(
    r"\b(?:dre|bre|lic(?:ense|\.)?|licen[cs]e|suite|ste\.?|unit|apt\.?|lot|"
    r"room|box|route|hwy|highway|no\.?)\s*$", re.I)


def _bare_host(domain) -> str:
    d = (domain or "").lower()
    return d[4:] if d.startswith("www.") else d


def synthetic_urls_enabled(domain) -> bool:
    return _bare_host(domain) in SYNTHETIC_URL_DOMAINS


def broker_ref_enabled(domain) -> bool:
    h = _bare_host(domain)
    return h in SYNTHETIC_URL_DOMAINS or h in BROKER_REF_ONLY_DOMAINS


def _is_yearlike(tok: str) -> bool:
    return tok.isdigit() and len(tok) == 4 and 1950 <= int(tok) <= 2035


def extract_broker_ref(text, domain) -> str | None:
    """The broker's own reference number printed on the card, or None.
    Only for ref-enabled brokers. Per-broker unlabelled pattern first, then a
    labelled token. First match wins (the card's own ref leads its text).
    Whitespace inside the ref is dropped ("OH 2083" == "OH2083"), so a
    spacing change on the page does not change the URL or id."""
    if not text or not broker_ref_enabled(domain):
        return None
    pat = BROKER_REF_PATTERNS.get(_bare_host(domain))
    if pat:
        m = pat.search(text)
        if m and not _is_yearlike(m.group(1)):
            return re.sub(r"\s+", "", m.group(1))
    for rx in (_BROKER_REF_LABELLED, _BROKER_REF_HASH):
        for m in rx.finditer(text):
            tok = m.group(1)
            before = text[max(0, m.start() - 12):m.start()]
            if _is_yearlike(tok) or _REF_NOT_AFTER.search(before):
                continue
            if text[max(0, m.start(1) - 1):m.start(1)] == "$":
                continue
            return tok
    return None


def synthetic_slug(*parts, max_len: int = SYNTHETIC_SLUG_MAX) -> str:
    """lowercase, ascii-folded, non-alphanumerics collapsed to one '-',
    trimmed, capped at max_len (re-trimmed so it never ends in '-')."""
    raw = "-".join(str(p) for p in parts if p not in (None, ""))
    folded = unicodedata.normalize("NFKD", raw).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", folded.lower()).strip("-")
    return slug[:max_len].strip("-")


def synthetic_listing_url(index_url: str, title, city=None, state=None) -> str:
    """index_url + '#dl-' + slug(title)[-slug(city-state)]. Any fragment
    already on index_url is replaced. The length cap applies to the TITLE part
    only: peakbusinessbrokersllc.com puts a whole description in the title
    slot, and capping the joined string cut the location off."""
    base = (index_url or "").split("#", 1)[0]
    slug = "-".join(p for p in (synthetic_slug(title), synthetic_slug(city, state)) if p)
    return f"{base}#{SYNTHETIC_FRAGMENT}{slug}" if slug else base


def broker_ref_url(index_url: str, ref) -> str:
    """index_url + '#dl-' + slug(ref)."""
    base = (index_url or "").split("#", 1)[0]
    return f"{base}#{SYNTHETIC_FRAGMENT}{synthetic_slug(ref)}"


def no_link_identity(base_url, title, city=None, state=None, text=None) -> dict | None:
    """Identity for a card with NO detail link, or None when the broker is not
    gated in (caller then keeps today's stable_listing_id(), unchanged).
    Returns {"id", "ref"}; the URL is assigned later by assign_synthetic_urls().
    For a ref card the id here is already sha256(base_url#dl-<ref>), and is
    recomputed there on the broker's index URL (same value on page 1)."""
    netloc = urlparse(base_url).netloc
    ref = extract_broker_ref(text or "", netloc)
    if ref and synthetic_slug(ref):
        url = broker_ref_url(base_url, ref)
        return {"id": stable_listing_id(title, url, base_url, netloc),
                "ref": synthetic_slug(ref)}
    if not synthetic_urls_enabled(netloc):
        return None
    loc = synthetic_slug(city, state)
    if not loc:
        return {"id": stable_listing_id(title, base_url, base_url, netloc), "ref": None}
    key = f"{netloc}|{(title or '').strip().lower()}|{loc}"
    return {"id": hashlib.sha256(key.encode()).hexdigest()[:16], "ref": None}


def assign_synthetic_urls(cards: list[dict], index_url: str) -> dict:
    """The single late assignment point. Every card carrying the internal
    _no_detail_link marker (set only for gated brokers) gets its synthetic URL
    on index_url; a ref card's id is re-keyed on that final URL. Pops the
    marker. Never sets url_is_listing_specific True. Two cards with the same
    URL but different ids are a collision: logged, both left as they are
    (no price tiebreak - decision 3). Returns counts for logging/tests."""
    synth = [(c, m) for c in cards if (m := c.pop(_SYNTH_FLAG, None))]
    for c, m in synth:
        if m.get("ref"):
            c["url"] = broker_ref_url(index_url, m["ref"])
            c["id"] = stable_listing_id(c.get("title"), c["url"], index_url,
                                        urlparse(index_url).netloc)
        else:
            c["url"] = synthetic_listing_url(index_url, c.get("title"),
                                             c.get("city"), c.get("state"))
        c["url_is_listing_specific"] = False
    synth = [c for c, _ in synth]
    groups: dict[str, set] = {}
    for c in synth:
        groups.setdefault(c["url"], set()).add(c["id"])
    for url_, ids in groups.items():
        if len(ids) > 1:
            print(f"   ⚠️  synthetic URL collision ({len(ids)} ids, left as is): {url_}")
    tiebroken = 0   # price tiebreak removed (decision 3, 2026-11-12); always 0
    still = Counter(c["url"] for c in synth)
    unresolved = sum(n for n in still.values() if n > 1)
    return {"synthetic": len(synth), "price_tiebreaks": tiebroken,
            "unresolved_collisions": unresolved}


# One host per broker for listing URLs. stable_listing_id() keys on the URL,
# so a site reachable as both example.com and www.example.com yields two ids
# for one listing. Map bare domain -> the host the site itself declares
# (301 target + <link rel=canonical>). calhouncompanies.com: bare 301s to
# www and detail pages carry rel=canonical https://www.calhouncompanies.com/...;
# the DB held 36 bare + 11 www URLs for the same inventory (2026-11-11).
CANONICAL_HOSTS = {"calhouncompanies.com": "www.calhouncompanies.com"}


def canonical_host(netloc: str) -> str:
    n = (netloc or "").lower()
    return CANONICAL_HOSTS.get(n[4:] if n.startswith("www.") else n, netloc or "")


# Query parameters that say where a listing sat on the index, not what it is
# (principle 4). marlingroup.com links page-2 listings as ?pid=459&pageno=2
# and page-1 ones as ?pid=459, so a listing that moved a page when a new one
# was added got a new URL and a new id (pid 458, 459 each stored twice,
# 2026-11 snapshots). Opt-in per domain; the pid alone opens the listing.
VOLATILE_URL_PARAMS = {"marlingroup.com": ("pageno",)}


def drop_volatile_params(url):
    if not url:
        return url
    pu = urlparse(url)
    host = pu.netloc.lower()
    drop = VOLATILE_URL_PARAMS.get(host[4:] if host.startswith("www.") else host)
    if not drop or not pu.query:
        return url
    from urllib.parse import parse_qsl, urlencode
    q = [(k, v) for k, v in parse_qsl(pu.query, keep_blank_values=True) if k not in drop]
    return urlunparse(pu._replace(query=urlencode(q)))


def canonical_url(url):
    """url with its host replaced by CANONICAL_HOSTS' choice; else unchanged."""
    if not url:
        return url
    pu = urlparse(url)
    host = canonical_host(pu.netloc)
    if not pu.netloc or host == pu.netloc:
        return url
    return urlunparse(pu._replace(netloc=host))


# Card shows several labelled figures and the largest is the revenue
# (calhouncompanies.com: "Price $450,000 Gross Sales $926,280" stored as a
# $926,280 asking price). For these domains the asking price is ONLY a figure
# sitting directly on a price label; a card with no Price field (2 of 106 on
# calhoun) has no asking price, rather than its gross sales.
# Opt-in per domain: on other brokers a corrected price would be recorded as
# a price change once, so widen only after measuring.
# jennessey.com (2026-11-12, j3-nopattern): Wix cards print "Asking Price:
# $700,000" beside sales figures, and the largest-figure rule stored $1.3M.
# It had no rows yet (NO_PATTERN every night), so no price change is recorded.
LABELLED_PRICE_DOMAINS = {"calhouncompanies.com", "jennessey.com"}
_LABELLED_PRICE_RE = re.compile(
    r"(?<![a-z])(?:asking\s+price|list\s+price|price)\s*:?\s*\$\s*([\d,]+(?:\.\d+)?)",
    re.I)


# "Equipment Price $1,000,000" on a calhoun detail page is not the ask. Only
# checked on detail pages: on a card the word before "Price" is the Category
# value ("Category Power Equipment Price $1,500,000"), which is the real ask.
_PRICE_QUALIFIERS = {"equipment", "inventory", "estate", "building", "property",
                     "land", "lease", "ff&e", "furniture", "stock", "purchase"}


def labelled_asking_price(text, skip_qualified=False):
    text = text or ""
    for m in _LABELLED_PRICE_RE.finditer(text):
        prev = text[max(0, m.start() - 20):m.start()].split()
        if skip_qualified and prev and prev[-1].lower().strip(":|-") in _PRICE_QUALIFIERS:
            continue
        try:
            v = float(m.group(1).replace(",", ""))
        except ValueError:
            continue
        if 1_000 <= v <= 100_000_000:
            return v
    return None


# Same opt-in shape as LABELLED_PRICE_DOMAINS above, but by element, not
# by regex over card text. Brokers whose auto-detected "card" is not one whole listing, and whose
# cards label each figure in its own element. Opt-in per domain: "card" is used
# instead of the pattern cache and detection; "fields" read each figure from
# its own element and never fall back to the card's largest dollar amount (a
# card with no Price element has no asking price, not its gross sales).
# healthcarebizsales.com: each <article class="... type-business ..."> holds two
# <div class="col-full"> halves — heading + "More Details" -> /business/<slug>/,
# and the figures + "Inquire Here" -> /inquiry/?sub=<title>. Detection picked
# the halves (2026-11-10: 125 rows, 62 titled "Inquire Here", one listing per
# two rows), or one category's articles only (2026-11-11: 63 of 135, every
# article carries its own business-category-* class). Largest-$ took Gross
# Sales as the price on every card where sales exceed the asking price.
# therouteexchange.com (WordPress Participants Database, 2026-11-12): detection
# picked the "Full Listing" link field (one <div> inside each record), so the
# card never saw its own Status / Asking Price fields. The record is
# <section id="record-N">; the page holds three lists - Available, Pending and
# Sold Routes - and its pager (?listpage=N&instance=3) pages the SOLD list only.
# "status" is read from the record's own status field (SOLD -> sold).
CARD_LAYOUT_DOMAINS = {
    "therouteexchange.com": {
        "card": "section[id^='record-']",
        "fields": {"asking_price": ".pdb-field-asking_price .pdb-field-data",
                   "cash_flow": ".pdb-field-gross_annual_income .pdb-field-data"},
        "status": ".pdb-field-status .pdb-field-data",
        "title": "slug",
    },
    # practiceconcepts.com (BusinessBroker.net "will_paginate" site, 2026-11-12):
    # detection picked the figures cell (Revenue / State / Reference) of each
    # <li class="element_rowNNNN"> record - no title, no link, no status - so
    # all 15 cards became one "REFERENCE" row and page 2 "repeated" page 1.
    # The record carries the title, the /listings/<slug> link and a status
    # cell ("Sold", 12 of 19 on 2026-11-12).
    "practiceconcepts.com": {
        "card": "li[class^='element_row']",
        "fields": {"revenue": ".details li:first-child p"},
        "status": ".listing-status",
        "title": ".link-title",
    },
    "healthcarebizsales.com": {
        "card": "article.type-business",
        "fields": {"asking_price": "span.price", "cash_flow": "span.aae",
                   "revenue": "span.gar"},
    },
    # baltimorebusinessbrokers.com (real-links, job 3, 2026-11-12): each card
    # is <a class="card business-listing-card--list" href="/business-detail/
    # <id>">. The cached selector div.row matched the two rows inside every
    # card (no link of their own -> index URL), the page wrapper (holding all
    # ten cards -> the first card's URL with the wrapper's first price) and
    # the header row (the ?data=10 page-size link). Card only; figures stay on
    # the generic path.
    "baltimorebusinessbrokers.com": {
        "card": "a.business-listing-card--list",
    },
}


def card_layout(domain):
    return CARD_LAYOUT_DOMAINS.get((domain or "").lower().removeprefix("www."))


def card_selector_override(domain):
    return (card_layout(domain) or {}).get("card")


def card_fields_from_layout(element, fields):
    """{field: value} read from each field's own element; None when absent."""
    out = {}
    for f, sel in fields.items():
        el = element.select_one(sel)
        vals = [v for v in parse_all_prices(el.get_text(" ", strip=True))
                if v >= 1_000] if el else []
        out[f] = vals[0] if vals else None
    return out


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

# Incomplete certificate chain on the broker's server (curl error 60).
INSECURE_TLS_DOMAINS = {"goldstarbbaz.com"}


def _bare(netloc):
    n = (netloc or "").lower()
    return n[4:] if n.startswith("www.") else n
DETAIL_DELAY     = (0.5, 1.5)   # seconds between detail fetches
# Infinite-scroll rounds per Playwright render (stops early once the page
# height is unchanged for two rounds; never fewer than four rounds).
SCROLL_MAX_ROUNDS = int(os.environ.get("SCROLL_MAX_ROUNDS", "30"))

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
# Index pages in a row that the site's own pager linked to but that added
# nothing, before the walk stops (unionbaygroup.com: 2). Guessed URLs get none.
PAGER_STALL_MAX = int(os.environ.get("PAGER_STALL_MAX", "2"))
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
    "businessbroker.net",
    "bizben.com",
    "dealstream.com",
    "flippa.com",
}


def is_marketplace_url(url: str) -> bool:
    """True if url is on a marketplace domain or any of its subdomains."""
    host = (urlparse(url or "").netloc or "").lower().split(":")[0]
    host = host[4:] if host.startswith("www.") else host
    return any(host == d or host.endswith("." + d) for d in MARKETPLACE_DOMAINS)


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
    "reputedbrokerage.com",          # EREPropertyScraper (run_specialized key
                                     # 'reputedbrokerage'; property-status archives)
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

    def replace(self, domain, pattern):
        """Overwrite an entry that was proven dead on this run (its selector
        extracted 0 cards from the live page). store()'s keep-the-better rule
        compares historical yield, and a dead entry's history is exactly what
        must not win: ninbb.com's Brilliant Directories selector and
        momentumprojects.com's Beaver Builder one held their slots for weeks
        that way (2026-11-12)."""
        self.patterns[domain] = pattern
        self.save()

    def other_spelling(self, domain):
        """The entry stored under the www / bare twin of `domain`, if any.
        Only a last resort when detection found nothing: 49 producing brokers
        have an entry only under the other spelling and detect fine without
        it, so it must never pre-empt detection (calhouncompanies.com's twin
        key held a stale selector that matched one card per page)."""
        d = (domain or "").lower()
        twin = d[4:] if d.startswith("www.") else "www." + d
        return self.get(twin)

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


def predicted_beats_detected(predicted, detected, html=None, url=None, broker_name=None):
    """Should another broker's cached selector (PatternCache.predict) replace
    this page's own detection? Today's rule (the stored score of the other
    broker's entry beats this page's detected score, nav selectors excluded),
    plus: never a BARE TAG.

    predict() picks the cached selector that matches the most elements on
    this page, and accountingfirmsold.com's cached bare `tr` (stored score 30,
    the maximum) matches any page with a few table rows. It won on
    benchmarkbusinessgroup.com's search-filter table (1 junk row - a Google
    Maps link - instead of 5 linked cards a page), dfwbc.com's page-layout row,
    kendallcapitalgroup.com's header row and ctacquisitions.com's article
    tables (job 3, 2026-11-12). A bare `tr` / `div` says nothing about what it
    selects. (Re-scoring every prediction on the page was measured too: it
    moved 15 of 82 saved broker pages, several for the worse - not done.)"""
    if not predicted:
        return False
    sel = (predicted.get("container_selector") or "").strip()
    if not sel or re.fullmatch(r"[a-z][a-z0-9]*", sel) or _is_nav_selector(sel):
        return False
    if not predicted.get("score", 0) > (detected or {}).get("score", 0):
        return False
    # Yield guard (job 3 NOPATTERN, 2026-11-12): a winning prediction that
    # extracts NOTHING from this page, or fewer cards than the detected
    # pattern, is dropped. Generic wrappers won on stored score and extracted
    # 0 cards: aiellobrokers (div.row), calcurbs (div.col), doababrokers,
    # jpar (div.entry-content ul li); krbrokers.com's div.col-sm-12 extracted
    # 2 junk cards over the detected div.fwre-list-item's 10. Only the
    # winning prediction is counted (not a re-score of every prediction), and
    # a tie keeps today's choice.
    dsel = (detected or {}).get("container_selector")
    if html and url and dsel and dsel != sel:
        firm = broker_name or urlparse(url).netloc
        n_pred = pattern_card_count(html, url, sel, firm)
        n_det = pattern_card_count(html, url, dsel, firm)
        if n_pred == 0 or n_pred < n_det:
            print(f"   🧠 ML prediction {sel} rejected: {n_pred} cards vs {n_det} detected")
            # the detected pattern was hidden behind the prediction until
            # now: the caller gates it like a re-detection (no history)
            if isinstance(detected, dict):
                detected["_exposed_by_yield_guard"] = True
            return False
    return True


def pattern_card_count(html, url, selector, broker_name, stop_at=None):
    """Cards ListingExtractor.from_card() accepts from `html` under `selector`
    (stops early at `stop_at`). Selector counts and detection scores are not
    yield: a cached or predicted selector can match elements and still
    extract nothing. Never raises."""
    try:
        els = BeautifulSoup(html, "html.parser").select(selector)
    except Exception:
        return 0
    n = 0
    for el in els:
        try:
            if ListingExtractor.from_card(el, url, broker_name):
                n += 1
                if stop_at and n >= stop_at:
                    break
        except Exception:
            continue
    return n


def healed_pattern_problem(html, url, selector, broker_name):
    """Why a pattern re-detected after a dead cache entry must not be used,
    or None. Page 1 must give at least 3 distinct listings, and at least 3 of
    them must carry their own detail URL or a price."""
    try:
        els = BeautifulSoup(html, "html.parser").select(selector)
    except Exception:
        return "selector does not parse"
    cards = []
    for el in els:
        try:
            c = ListingExtractor.from_card(el, url, broker_name)
        except Exception:
            c = None
        if c:
            cards.append(c)
    ids = {c["id"] for c in cards}
    if len(ids) < 3:
        return f"{len(ids)} distinct listing(s) on page 1"
    linked = {c["url"] for c in cards if c.get("url_is_listing_specific")}
    priced = {c["id"] for c in cards if c.get("asking_price")}
    if len(linked) < 3 and len(priced) < 3:
        return f"only {len(linked)} detail URL(s) and {len(priced)} priced card(s)"
    return None


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
    def _fallback_candidates(cls, soup, url):
        """Shapes strategies 1-2 cannot express. Consulted ONLY when they
        found no candidate that survives the nav filter (i.e. detect() would
        return None today), so a broker that detects today keeps exactly the
        pattern it gets today (principle 8). (Class-less table rows, the
        other shape found in this class - alpinebusinessbrokers.com,
        lightandraphael.com - are j3-table_rows' Strategy 1c, not here.)

        Cards whose class list carries one per-card token. Wix Studio
        stamps every card `comp-<id>` beside its shared classes
        (districtbusinessbrokers.com: `_root_1w52m_1 … builder-root
        comp-mux2zidb`), so no two cards share a class string, and the id
        part is not a hex suffix for strategy 1b to strip. Group on the
        classes the element shares with at least MIN_LISTING_ELEMENTS
        elements of its tag and select on those.
        """
        candidates = []

        def _add(selector, group, bonus=0):
            scored = cls._score_group(group, url)
            if scored:
                n_listing, n_priced = scored
                candidates.append({
                    "container_selector": selector,
                    "count": len(group),
                    "score": n_listing + n_priced + bonus,
                    "sample_text": group[0].get_text(separator=" ", strip=True)[:200],
                })

        # shared classes once per-card tokens are dropped
        for tag in ["div", "article", "li", "section"]:
            els = [el for el in soup.find_all(tag) if el.get("class")]
            freq = Counter(c for el in els for c in set(el["class"]))
            groups = defaultdict(list)
            for el in els:
                own = list(dict.fromkeys(el["class"]))
                shared = [c for c in own if freq[c] >= cls.MIN_LISTING_ELEMENTS]
                if shared and len(shared) < len(own):
                    groups[tuple(shared)].append(el)
            for shared, group in groups.items():
                if len(group) < cls.MIN_LISTING_ELEMENTS:
                    continue
                sel = css_selector(tag, list(shared))
                try:
                    matched = soup.select(sel)
                except Exception:
                    continue
                _add(sel, matched)

        # Rank by what each selector really extracts, not by score: on a Wix
        # Studio page the row wrapper (three cards per row) out-scores the
        # card, and extracting it keeps only the first card of every row
        # (districtbusinessbrokers.com: 10 of 12). Most distinct listings
        # wins; then most cards carrying their own detail URL (the text
        # block inside a card extracts every title but not the card's
        # "View Listing" link); then fewer duplicate cards (a selector
        # matching both a wrapper and its cards emits the wrapper's copy
        # with the wrong title); then score.
        firm = urlparse(url).netloc
        for c in candidates:
            ids, urls = [], set()
            try:
                for el in soup.select(c["container_selector"]):
                    card = ListingExtractor.from_card(el, url, firm)
                    if card:
                        ids.append(card["id"])
                        if card.get("url_is_listing_specific"):
                            urls.add(card["url"])
            except Exception:
                pass
            c["_rank"] = (len(set(ids)), len(urls), -(len(ids) - len(set(ids))), c["score"])
        candidates = [c for c in candidates if c["_rank"][0] > 0]
        candidates.sort(key=lambda c: c["_rank"], reverse=True)
        for c in candidates:
            c.pop("_rank", None)
        return candidates[:1]

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

        # Strategy 1c: the rows of a listing <table>, addressed through the
        # table (job 3, 2026-11-12). Rows with no class (accountingfirmsold,
        # prohorizons) or a per-row class Strategy 1b cannot group (TablePress
        # row-2, row-3 ...; Ninja Tables ninja_table_row_0 ...) were never a
        # candidate: keatepartners.com was NO_PATTERN, prohorizons.com only
        # worked because another broker's cached bare `tr` was predicted.
        # Selector = the table's own class (else id) + " tr"; header rows are
        # rejected by is_listing_element. Not a candidate: layout tables
        # (holding, or held by, another table), tables in site chrome, and a
        # table under a "sold" heading ("Recent Businesses Sold") - its rows
        # carry no per-row status, so they would be written as live listings.
        for sel, rows in _table_row_candidates(soup):
            scored = cls._score_group(rows, url)
            if scored:
                n_listing, n_priced = scored
                candidates.append({
                    "container_selector": sel,
                    "count": len(rows),
                    "score": n_listing + n_priced,
                    "sample_text": rows[0].get_text(separator=" ", strip=True)[:200],
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

        # Drop navigation/menu/footer/widget containers: WordPress nav bars
        # repeat and link out, so they otherwise win as "listings" (myersba's
        # 61 menu links, svnmarinas' 53 'Advisory Services'). Better NO_PATTERN
        # than writing a site's nav bar as inventory.
        candidates = [c for c in candidates
                      if not _is_nav_selector(c.get("container_selector", ""))]
        if not candidates:
            # Nothing but nav survived: only now try the fallback shapes.
            candidates = [c for c in cls._fallback_candidates(soup, url)
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
    # Revenue under 5% of the asking price is a weekly or monthly figure read
    # as annual revenue (commercialroutesforsale.com: "Net $1,100 a week"
    # stored as revenue on a $175,000 route, 2026-11-09). No Main Street
    # business asks 20x its annual sales.
    if (revenue is not None and price is not None and price > 0
            and revenue < 0.05 * price):
        revenue = None
    return price, cash_flow, revenue




# ============================================================
# DETAIL-PAGE FIELD MAPS (agents/detail_maps.py writes them)
# ============================================================
# Some brokers' cards carry several dollar figures and extract_price() takes
# the largest, so the REVENUE lands in asking_price (kensingtoncompany.com:
# "$1.6M" stored for a $499K woodworking shop, 2026-11-09). For a domain whose
# map has a live asking_price, the detail page's mapped figures win, and a
# listing that isn't re-fetched keeps its STORED financials instead of the
# card's guess — otherwise every crawl would flip the price and the price
# detector would record fake changes. Self-contained: CI doesn't install the
# anthropic package that agents/detail_maps.py imports.
_DETAIL_MAP_DIR = Path(__file__).resolve().parent.parent / "data" / "detail_maps"
_FIN_MAP_CACHE: dict = {}


def _fin_map(netloc: str):
    """The map for this domain if it can supply asking_price, else None."""
    key = (netloc or "").lower()
    if key in _FIN_MAP_CACHE:
        return _FIN_MAP_CACHE[key]
    m = None
    for cand in (key, key[4:] if key.startswith("www.") else "www." + key):
        path = _DETAIL_MAP_DIR / f"{cand}.json"
        if path.exists():
            try:
                m = json.loads(path.read_text())
            except Exception:
                m = None
            break
    # Opt-in only: `detail_maps.py backfill --repair --fix-price --write` sets
    # price_from_map when it corrects stored prices from this map, so the
    # crawler and the database switch source together (no flip-flopping).
    ok = (m and m.get("price_from_map") is True
          and m.get("status") in ("validated", "partial")
          and "asking_price" not in m.get("weak_fields", [])
          and m["fields"].get("asking_price", {}).get("method") in ("css", "label")
          and not m["fields"]["asking_price"].get("not_disclosed"))
    _FIN_MAP_CACHE[key] = m if ok else None
    return _FIN_MAP_CACHE[key]


def _map_label_value(soup, label):
    pat = re.compile(re.escape(label.rstrip(": ").strip()), re.I)
    for s in soup.find_all(string=pat):
        node = s.parent
        after = node.get_text(" ", strip=True)
        mm = pat.search(after)
        tail = after[mm.end():] if mm else ""
        if tail.strip(" :-"):
            return tail.strip(" :-")
        sib = node.find_next_sibling() or (node.parent and node.parent.find_next_sibling())
        if sib:
            return sib.get_text(" ", strip=True)
    return None


def _apply_fin_map(m, html: str) -> dict:
    """{asking_price, revenue, cash_flow} the map finds on this detail page."""
    soup, out = BeautifulSoup(html, "html.parser"), {}
    skip = set(m.get("weak_fields", []))
    for f in ("asking_price", "revenue", "cash_flow"):
        spec = m["fields"].get(f) or {}
        if f in skip or spec.get("not_disclosed"):
            continue
        raw = None
        try:
            if spec.get("method") == "css" and spec.get("css"):
                el = soup.select_one(spec["css"])
                raw = el.get_text(" ", strip=True) if el else None
            elif spec.get("method") == "label" and spec.get("label"):
                raw = _map_label_value(soup, spec["label"])
        except Exception:
            raw = None
        vals = [v for v in parse_all_prices(raw or "") if v >= 1_000]
        if vals:
            out[f] = vals[0]
    return out


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
            t = strip_dangling_sep(h1.get_text(strip=True))
            if len(t) > 5:
                return t[:500]
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            return strip_dangling_sep(og["content"])[:500] or None
        tag = soup.find("title")
        if tag:
            return strip_dangling_sep(tag.get_text(strip=True))[:500] or None
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

    # (title, label) of cards not emitted because their title/badge status is
    # DROP (Coming Soon / For Lease). The broker loop reads and resets it.
    status_drops: list = []

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

        best_href = (_onclick_detail_link(element) or _best_detail_link(element, base_url)
                     or _enclosing_link(element))
        detail_url = urljoin(base_url, best_href) if best_href else None

        # Don't treat pagination/anchor links as detail URLs
        if detail_url:
            d = detail_url.rstrip("/")
            b = base_url.rstrip("/")
            if d == b or "#" in d.split("?")[0][-5:]:
                detail_url = None
        if not detail_url:
            wrapped = _wrapper_detail_link(element, base_url)
            if wrapped:
                detail_url = urljoin(base_url, wrapped)
        detail_url = canonical_url(detail_url)
        detail_url = drop_volatile_params(detail_url)

        # Marketplace gate (2026-10-08): some brokers list a deal on their own
        # page but link the card out to its marketplace copy (bizbuysell.com,
        # loopnet.com, ...). That card is the marketplace's page, not the
        # broker's: never record it, and never fetch it as a detail page.
        # Found as 76 active public rows with bizbuysell.com URLs from
        # springfieldstrategies.com, papadop.com, lonestarba.com and others.
        # Broker-ref brokers (2026-11-12): a card that links out to the
        # marketplace but prints the broker's own ref (naz-business.com) is
        # the broker's listing - keep it as a no-link card keyed on the ref,
        # and never store the marketplace link. No ref -> dropped as before.
        if (detail_url and is_marketplace_url(detail_url)
                and extract_broker_ref(text, domain)):
            detail_url = None
        if detail_url and is_marketplace_url(detail_url):
            return None

        title = strip_dangling_sep(
            best_card_title(element, base_url, firm_name=broker_name) or text[:100])
        # ONE RULE: a junk/blank title falls back to the listing URL slug, which
        # is where the real business name survives on CTA-titled cards.
        if is_junk_title(title, broker_name):
            title = title_from_slug(detail_url) or title
        # A layout whose records carry no title text, only field labels
        # ("Route Type", "City"): the detail URL's slug is the name.
        # No detail link = no name and no identity: not emitted.
        # Any other "title" value is the selector of the record's own title
        # element (used when it holds a real name).
        title_rule = (card_layout(domain) or {}).get("title")
        if title_rule == "slug":
            slug_title = title_from_slug(detail_url) if detail_url else None
            if not slug_title:
                return None
            title = slug_title
        elif title_rule:
            t_el = element.select_one(title_rule)
            t_txt = strip_dangling_sep(t_el.get_text(" ", strip=True)) if t_el else ""
            if t_txt and not is_junk_title(t_txt, broker_name):
                # A name that reads like a price fragment to the junk gate
                # ("$500K Gross Coastal Orange County, CA Optometry") falls
                # back to the URL slug rather than dropping the listing.
                title = (t_txt if not is_junk_listing(t_txt, detail_url or base_url)
                         else (title_from_slug(detail_url) or t_txt))

        # Junk gate — nav fragment, price fragment, status badge, CTA, legal
        # page, CRE/land: never emit these as a listing.
        if is_junk_listing(title, detail_url or base_url):
            return None

        # Site-page gate (2026-10-05): the broker's own services/contact/
        # valuation/blog/social pages are not listings. Checked against the
        # card's own detail URL only — the list-page URL says nothing.
        if is_site_page(title, detail_url or ""):
            if not _cta_site_link(element, title, text, detail_url, base_url):
                return None
            # The card is a listing whose only link is a button to the
            # broker's contact page or homepage ("Learn More" -> /contact on
            # nextstep-capital.com, "Contact Us for More Info" -> / on
            # jennessey.com). Dropping it dropped every card on the page,
            # every night. Keep it as a no-link card; never store the link.
            detail_url = None

        # Residential/IDX row gate — see looks_residential() above.
        if looks_residential(title, text):
            return None

        fields = (card_layout(domain) or {}).get("fields")
        if fields:
            got = card_fields_from_layout(element, fields)
            asking_price = positive_or_none(got.get("asking_price"))
            cash_flow    = positive_or_none(got.get("cash_flow"))
            revenue      = positive_or_none(got.get("revenue"))
        else:
            asking_price = positive_or_none(cls.extract_price(text))
            cash_flow    = positive_or_none(cls.extract_cash_flow(text))
            revenue      = positive_or_none(cls.extract_revenue(text))
        if not fields and _bare(domain) in LABELLED_PRICE_DOMAINS:
            labelled = labelled_asking_price(text)
            if labelled:
                asking_price = labelled
            elif asking_price and asking_price == revenue:
                asking_price = None      # no Price field: undisclosed, not revenue
        asking_price, cash_flow, revenue = _sane_financials(
            asking_price, cash_flow, revenue)
        location     = LocationExtractor.extract(text)
        vertical     = cls.classify_vertical(text)

        _url = detail_url or base_url
        # No detail link on a gated broker: ref / title+location identity
        # (no_link_identity); everyone else, today's stable_listing_id().
        synth = (no_link_identity(base_url, title,
                                  location["city"] if location else None,
                                  location["state"] if location else None, text)
                 if not (detail_url and detail_url != base_url) else None)
        listing_id = synth["id"] if synth else stable_listing_id(
            title, _url, base_url, urlparse(base_url).netloc
        )

        now = datetime.now(timezone.utc).isoformat()
        # Status, most specific evidence first:
        #  1. an allowlisted broker's own title marker (TITLE_STATUS_MARKER_DOMAINS);
        #  2. a labelled "Status: <value>" field on the card (card_status_field),
        #     else a table row's Status column (table_status_cell) - the
        #     broker's own statement, so it beats a title marker or badge,
        #     except that a plain active value ("Available") adds nothing;
        #  3. card_status(): the most final of badge/class and title marker,
        #     mapped through listing_status (decision 6: the under-contract
        #     family stays active with the broker's words).
        # Coming Soon / For Lease are not emitted (decision 5), counted per broker.
        status, status_marker = card_status(element, title, domain)
        from_field = False
        field = ((card_status_field(element, domain) or table_status_cell(element, domain))
                 if not domain_title_status_marker(domain, title) else None)
        if field and (field[1] is not None or field[0] != "active"):
            if field[0] == _SF_DROP:
                _STATUS_FIELD_DROPS[_bare(domain)][field[1]].add((title, detail_url))
                return None
            status, status_marker, from_field = field[0], field[1], True
        # A layout broker's own status element (CARD_LAYOUT_DOMAINS "status"):
        # the value of that one element, judged by the shared phrase set.
        # Unknown values ("Avail") add nothing; SOLD -> sold; Pending -> active
        # with the broker's words (decision 6).
        st_sel = (card_layout(domain) or {}).get("status")
        st_el = element.select_one(st_sel) if st_sel else None
        st_hit = (_sf_lookup(st_el.get_text(" ", strip=True), domain)
                  if (st_el is not None and _sf_lookup) else None)
        if st_hit and (st_hit[1] is not None or st_hit[0] != "active"):
            if st_hit[0] == _SF_DROP:
                _STATUS_FIELD_DROPS[_bare(domain)][st_hit[1]].add((title, detail_url))
                return None
            status, status_marker, from_field = st_hit[0], st_hit[1], True
        if status == _LS_DROP:
            cls.status_drops.append((title[:80], status_marker))
            return None

        # "Price: Market Price" / "Asking Price: Call for price" on the card:
        # the broker's verbatim phrase, judged by the shared rule. Labelled
        # values only — never prose — and only when no price parsed.
        broker_price_text = (None if asking_price
                             else find_labelled_withheld_price(text))

        return {
            "id":            listing_id,
            "title":         title[:500],
            "asking_price":  asking_price,
            "cash_flow":     cash_flow,
            "revenue":       revenue,
            "city":          location["city"]  if location else None,
            "state":         location["state"] if location else None,
            "vertical":      vertical,
            "broker_price_text": broker_price_text,
            "url":           _url,
            "url_is_listing_specific": bool(detail_url) and detail_url != base_url,
            "broker_name":   broker_name,
            "broker_domain": urlparse(base_url).netloc,
            "description":   text[:1000],
            # Sold deals (badge/class/ribbon, title marker or status field) are
            # imported as status='sold' so they don't inflate the active count
            # or get DOM'd as live inventory; under contract / pending / off
            # market stay active (decision 6) with the broker's label below.
            "status":        status,
            # restatus_from_title() must not re-decide a field-decided row
            **({"_status_from_field": True} if from_field else {}),
            # the broker's own words, verbatim ("SOLD!", "UNDER CONTRACT:");
            # title-marked rows of a TITLE_STATUS_MARKER_DOMAINS broker, or
            # the value of a labelled card status field (card_status_field)
            **({"broker_status_text": status_marker} if status_marker else {}),
            "first_seen":    now,
            "last_seen":     now,
            "_detail_url":   detail_url,
            # Gated no-link card: assign_synthetic_urls() gives it
            # <index>#dl-<slug> once the broker's whole set is known.
            **({_SYNTH_FLAG: synth} if synth else {}),
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

        # Financials — full page text has much more context. Not for a
        # CARD_LAYOUT_DOMAINS broker: its card figures are labelled, and the
        # page's largest-$ guess is exactly what that config exists to avoid.
        labelled = bool((card_layout(listing.get("broker_domain")) or {}).get("fields"))
        if not labelled and not listing.get("asking_price"):
            if _bare(listing.get("broker_domain")) in LABELLED_PRICE_DOMAINS:
                listing["asking_price"] = positive_or_none(
                    labelled_asking_price(full_text, skip_qualified=True))
            else:
                listing["asking_price"] = positive_or_none(cls.extract_price(full_text))
        if not labelled and not listing.get("cash_flow"):
            listing["cash_flow"] = positive_or_none(cls.extract_cash_flow(full_text))
        if not labelled and not listing.get("revenue"):
            listing["revenue"] = positive_or_none(cls.extract_revenue(full_text))
        # Two shapes a detail page produces that the card check would have
        # caught, and nothing else: one figure read under both labels, and a
        # weekly figure read as annual revenue. commercialroutesforsale.com's
        # page labels the WEEKLY net "Net Income: $2,000" next to an empty
        # "Total Sales:", so both fields read $2,000 on a $219,000 route.
        # Deliberately not a full _sane_financials pass: that also drops
        # revenue == asking_price, which on inbargroup.com dropped the detail
        # page's revenue on 30 of 61 rows (the card price had taken that figure).
        cf, rv = listing.get("cash_flow"), listing.get("revenue")
        price = listing.get("asking_price")
        if cf is not None and cf == rv:
            listing["cash_flow"] = None
            if rv == price:   # one figure under all three labels
                listing["revenue"] = rv = None
        if rv is not None and price and rv < 0.05 * price:
            listing["revenue"] = None

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
    text = f"{title or ''} {description or ''}"
    synth = (no_link_identity(base_url, title, city, state, text)
             if not (detail_url and detail_url != base_url) else None)
    listing_id = synth["id"] if synth else stable_listing_id(
        title, url, base_url, urlparse(base_url).netloc)
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
        # Same rule as from_card (principle 15): gated no-link listing ->
        # synthetic URL assigned by assign_synthetic_urls().
        **({_SYNTH_FLAG: synth} if synth else {}),
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


def _tupelo_detail_url(index_url, listing_id):
    """The Tupelo widget's per-listing URL: index path + ?listingId=<id>.
    None when the feed item has no id (the row then keeps the index URL and
    the domain|title identity, as before)."""
    lid = str(listing_id).strip() if listing_id is not None else ""
    if not lid:
        return None
    from urllib.parse import quote
    p = urlparse(index_url)
    page = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    return f"{page}?listingId={quote(lid, safe='')}"


def _handler_tupelosmb(url, html, broker_name, fetcher):
    """
    Tupelo SMB CRM platform. The broker page loads listings from
    crm.tupelosmb.com/api/public/listings?organizationId=<cuid>&take=&page=
    The org cuid is normally in the widget's organization-id attribute
    (<tupelo-marketplace organization-id="cm...">). Newer cuids start with
    'cm', not 'cl', so the bare 'cl…' scan is only a fallback.

    Every feed item has a stable `id`, and the widget opens its detail view
    with react-router's useSearchParams -> navigate("?listingId=<id>") on a
    browser history (marketplace/index.js: `n({listingId:e.id})`, then
    `n.get("listingId")` renders the detail component). So the per-listing
    URL is the index page's path + "?listingId=<id>" - the widget's own
    navigate replaces the whole query string, so the index's filter params
    (e.g. ?statuses=ACTIVE) are dropped, as they are here. Without this every
    listing's url was the index page and its id fell back to
    sha256(domain|title) (CLAUDE.md principle 4).

    Pagination is 1-based `page` (the widget sends take=10&page=N). The API
    ignores `skip`: the old skip loop re-read page 1 until skip >= totalCount,
    so a broker with 53 active + 141 sold got every active listing twice, and
    one with more than `take` active items lost everything past the first
    page. `statuses=ACTIVE` is the widget's own filter; the client-side status
    check stays in case the API ever ignores it, and a page that brings no
    new id ends the loop (principle 7: a repeated page is not more data).
    """
    m = (re.search(r'organization-?id\s*=\s*["\']?(c[a-z0-9]{20,})', html, re.I)
         or re.search(r'\b(cl[a-z0-9]{20,})\b', html))
    if not m:
        return None
    org_id = m.group(1)
    out, seen, page, take = [], set(), 1, 100
    while True:
        try:
            r = fetcher.session.get(
                "https://crm.tupelosmb.com/api/public/listings"
                f"?organizationId={org_id}&statuses=ACTIVE"
                f"&take={take}&page={page}",
                timeout=REQ_TIMEOUT)
            if r.status_code != 200:
                break
            data = r.json()
        except Exception:
            break
        items = data.get("listings", []) if isinstance(data, dict) else []
        total = data.get("totalCount", 0) if isinstance(data, dict) else 0
        new = 0
        for it in items:
            lid = it.get("id")
            if lid is not None:
                if lid in seen:
                    continue
                seen.add(lid)
            new += 1
            if str(it.get("status", "")).upper() not in ("ACTIVE", ""):
                continue
            loc = it.get("locationStringShort") or it.get("locationString") or ""
            out.append(_make_api_listing(
                url, broker_name,
                title=it.get("headline"),
                detail_url=_tupelo_detail_url(url, it.get("id")),
                asking_price=it.get("askingPrice"),
                cash_flow=it.get("cashFlow"),
                revenue=it.get("revenue"),
                state=_state_from_location(loc),
                city=(loc.split(",")[0].strip() if loc else None),
                description=it.get("headline")))
        if not items or not new or page * take >= total:
            break
        page += 1
    return out or None


# Base44 "Listing" entity feed (ninbb.com, rebuilt as a Base44 React SPA
# around 5 Oct 2026). The server returns an empty shell for every route; the
# browser loads ALL listings with one call,
#     GET /api/apps/<appId>/entities/Listing?sort=-created_date&limit=1000
# and filters client-side to published, non-training listings whose status is
# Active / Under LOI / Sale Pending, 12 cards per page ("1-12 of 179").
# The endpoint is public (no auth). It also returns fields the site never
# shows (HubSpot ids, internal notes, the asking price of listings marked
# "Undisclosed"), so only the public fields are requested and used.
BASE44_PUBLIC_STATUSES = ("Active", "Under LOI", "Sale Pending")
BASE44_LISTING_FIELDS = (
    "id,slug,listing_headline,listing_status,published,is_training,is_sample,"
    "asking_price,asking_price_public,revenue_public,cash_flow_public,"
    "hidden_fields,listing_region,listing_state,primary_industry,"
    "listing_summary_public,bd_filename")
# The site's default when a listing has no hidden_fields of its own.
BASE44_DEFAULT_HIDDEN = ("employees", "year_established")


def _base44_app_id(html):
    for pat in (r'appId\s*=\s*["\']([0-9a-f]{24})["\']',
                r'/api/apps/([0-9a-f]{24})/',
                r'media\.base44\.com/images/public/([0-9a-f]{24})/'):
        m = re.search(pat, html or "")
        if m:
            return m.group(1)
    return None


def _base44_public_listings(items):
    """The site's own public filter (businesses-for-sale page)."""
    return [it for it in items
            if isinstance(it, dict)
            and it.get("published") is True
            and not it.get("is_training")
            and not it.get("is_sample")
            and it.get("listing_status") in BASE44_PUBLIC_STATUSES]


def _base44_status(value):
    """('active', label-or-None) for a public Base44 listing_status."""
    try:
        from listing_status import classify_status
    except Exception:                 # module not present (job 3 not applied)
        classify_status = None
    text = (value or "").strip()
    if classify_status is not None:
        # Only BASE44_PUBLIC_STATUSES reach here, and the site lists all
        # three as for sale: status is always active, only the label varies.
        return "active", classify_status(text)[1]
    return "active", (text if text in ("Under LOI", "Sale Pending") else None)


def _base44_row(it, base_url, broker_name):
    """One public Base44 Listing -> a V6 listing dict.

    Identity (principle 4: never price, never title when ids exist):
      - listings carried over from the old Brilliant Directories site keep
        their bd_filename, which is the path of the URL every existing
        ninbb.com row was keyed on (https://www.ninbb.com/<path>). Hashing
        that keeps those rows' ids, and with them first_seen / DOM history.
      - listings created on the new site have no bd_filename and are keyed
        on their detail URL /listings/<slug>.
    The stored url is always the current detail page /listings/<slug>.
    """
    title = (it.get("listing_headline") or "").strip()
    slug = (it.get("slug") or "").strip()
    if not title or not slug:
        return None
    p = urlparse(base_url)
    host = _bare(p.netloc)
    detail_url = f"{p.scheme or 'https'}://{host}/listings/{slug}"
    hidden = it.get("hidden_fields")
    hidden = set(hidden) if isinstance(hidden, list) else set(BASE44_DEFAULT_HIDDEN)

    # The site shows "Undisclosed" unless asking_price_public and a price.
    price = None
    if ("asking_price" not in hidden and it.get("asking_price_public")
            and it.get("asking_price")):
        price = positive_or_none(it.get("asking_price"))
    revenue = None if "revenue" in hidden else it.get("revenue_public")
    cash_flow = None if "cash_flow" in hidden else it.get("cash_flow_public")
    region = None if "region" in hidden else it.get("listing_region")
    state = (it.get("listing_state") or "").strip().upper() or None
    if state not in US_STATES:
        state = _state_from_location(region)

    row = _make_api_listing(
        base_url, broker_name, title=title, detail_url=detail_url,
        asking_price=price, cash_flow=cash_flow, revenue=revenue,
        state=state, city=None,
        description=it.get("listing_summary_public") or title)
    bd = (it.get("bd_filename") or "").strip().strip("/")
    if bd:
        from urllib.parse import unquote
        legacy = f"https://www.{host}/{unquote(bd)}"
        row["id"] = stable_listing_id(title, legacy, base_url, p.netloc)
    # "Undisclosed" is the broker's choice, not a parse failure. Carried on
    # the row; the Supabase writer does not persist it (no column yet).
    row["price_not_disclosed"] = row["asking_price"] is None
    row["broker_status"] = it.get("listing_status")
    # Under LOI / Sale Pending are on the public list, so the row stays
    # active; the broker's words go in broker_status_text (decision 6, one
    # phrase set: scrapers/listing_status.py when importable).
    st, label = _base44_status(it.get("listing_status"))
    row["status"] = st
    if label:
        row["broker_status_text"] = label
    row["source_listing_id"] = it.get("id")
    return row


def _handler_base44(url, html, broker_name, fetcher):
    app_id = _base44_app_id(html)
    if not app_id:
        return None
    api = urljoin(url, f"/api/apps/{app_id}/entities/Listing")
    # Page on the unique id. The site's own sort (-created_date) has ties -
    # a bulk import on 17 Sep shares one created_date - and skip-paging over
    # a tied sort returned 5 rows twice and 5 never (174 of 179).
    items, skip, limit = [], 0, 500
    for _ in range(10):                       # 5,000 rows max
        r = fetcher.session.get(
            api, params={"sort": "id", "limit": limit,
                         "skip": skip, "fields": BASE44_LISTING_FIELDS},
            timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            print(f"   ⚠️  base44 feed HTTP {r.status_code}")
            return None
        page = r.json()
        if not isinstance(page, list):
            return None
        items.extend(page)
        if len(page) < limit:
            break
        skip += limit
    ids = [it.get("id") for it in items if isinstance(it, dict)]
    if len(set(ids)) != len(ids):
        # Unstable paging: some rows were served twice, so others were
        # skipped. A short set must not pass as the broker's full inventory.
        print(f"   ⚠️  base44 feed paging unstable ({len(ids)} rows, "
              f"{len(set(ids))} distinct) - refusing a partial set")
        return None
    # Not this site's schema (another Base44 app): no listing feed here.
    if not any(isinstance(it, dict) and "listing_headline" in it for it in items):
        return None
    out, seen = [], set()
    for it in _base44_public_listings(items):
        if it.get("id") in seen:
            continue
        seen.add(it.get("id"))
        row = _base44_row(it, url, broker_name)
        if row:
            out.append(row)
    return out or None


# Embedded listing index: <script type="application/json" id="lx-data">
# (iconic.co, an Astro site; real-links, job 3, 2026-11-12). The page renders
# 16 cards and a JS pager with no links ("Showing 1-16 of 479"); the browser
# filters and pages the WHOLE index client-side from this one JSON block, so
# V6 saw 16 cards and stopped (single_page). Each entry carries its detail
# href, title, price/revenue/profit and a status:
#   live       - for sale with an asking price ("Asking $249K")   -> emitted
#   exploring  - an owner "testing the market privately"; the card says
#                "Asking Not yet set" and the site's own filter separates
#                them. Not a listing for sale - not emitted (counted in the
#                log line), the same treatment as Coming Soon (decision 5).
# Identity: the detail href, exactly the URL the card's <a> carried, so ids
# keyed on the card links (stable_listing_id(url)) are unchanged.
LX_DATA_PUBLIC_STATUSES = ("live",)


def _handler_lx_data(url, html, broker_name, fetcher):
    soup = BeautifulSoup(html or "", "html.parser")
    tag = soup.find("script", id="lx-data")
    if tag is None:
        return None
    try:
        data = json.loads(tag.string or tag.get_text() or "{}")
    except ValueError:
        return None
    items = data.get("listings") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return None
    out, seen, skipped = [], set(), {}
    for it in items:
        if not isinstance(it, dict):
            continue
        status = str(it.get("status") or "").strip().lower()
        title = (it.get("title") or "").strip()
        href = (it.get("href") or "").strip()
        if not title or not href:
            continue
        if status not in LX_DATA_PUBLIC_STATUSES:
            skipped[status or "-"] = skipped.get(status or "-", 0) + 1
            continue
        detail_url = canonical_url(urljoin(url, href))
        if detail_url in seen:
            continue
        seen.add(detail_url)
        loc = (it.get("loc") or "").strip()
        state = (str(it.get("st") or "").strip().upper() or None)
        if state not in US_STATES:
            state = _state_from_location(loc)
        city = loc.split(",")[0].strip() if ("," in loc) else None
        row = _make_api_listing(
            url, broker_name, title=title, detail_url=detail_url,
            asking_price=it.get("price"), revenue=it.get("rev"),
            cash_flow=it.get("profit"), city=city, state=state,
            description=it.get("text") or title)
        row["source_listing_id"] = it.get("id")
        row["status"] = "active"
        # 11 of 57 live cards say "Asking Not disclosed": the broker's choice,
        # not a parse failure (same flag as the base44 handler / w14).
        row["price_not_disclosed"] = (row["asking_price"] is None and "disclosed"
                                      in str(it.get("priceText") or "").lower())
        out.append(row)
    print(f"   🗂  lx-data index: {len(items)} entries, {len(out)} live emitted"
          + (f"; not emitted: {', '.join(f'{k} {v}' for k, v in sorted(skipped.items()))}"
             if skipped else ""))
    return out or None


# Registry: substring found in page source -> handler(url, html, broker_name, fetcher)
API_HANDLERS = [
    ("tupelosmb.com", _handler_tupelosmb),
    ("base44", _handler_base44),
    ('id="lx-data"', _handler_lx_data),
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
# businessusa.biz — index table with a per-row broker Remarks column
# ============================================================
#
# The index is one paginated table ("Showing 602 of 602 businesses", 15 rows a
# page): Listing # | Type | County | State | Price | Annual | Remarks. Remarks is
# the broker's own status: "SOLD" (any case), "Under Contract", or a free note.
# The generic pattern path read this table as plain cards: every row went in
# as status='active' (no badge/class for looks_sold to see), the largest dollar
# figure on the row (Annual sales) was stored as asking_price, and a free-note
# remark sometimes became the title.
#
# Identity (principle 4): the Listing # is the broker's stable reference. The
# url is rebuilt as https://businessusa.biz/business/<Listing #> — the same
# string the generic path hashed (broker_sources' bare host; the site echoes
# whichever host it was requested on, so the www form would otherwise mint a
# second id for every listing). id = stable_listing_id(...) = sha256(url)[:16],
# exactly as before. Price and remark are never part of the id.
#
# Status (principle 14): SOLD is positive evidence from the broker, sent as an
# explicit status='sold'. Everything else is sent 'active'; "Under Contract" is
# carried verbatim in broker_status_text, a free note in broker_note.
# The phrase set is scrapers/listing_status.py when importable (job 3,
# decisions 5 + 6: one vocabulary for every writer, principle 15): the
# under-contract family (Pending, Under LOI, Accepted Offer, Off Market) is
# active + label, a Coming Soon / For Lease remark is not emitted (counted in
# the handler's log line). The local regexes below are the fallback.
try:
    from listing_status import lookup_status as _ls_lookup, DROP as _LS_DROP
except Exception:                    # module not present (job 3 not applied)
    _ls_lookup, _LS_DROP = None, "DROP"

BUSINESSUSA_HOST = "businessusa.biz"
_BUSINESSUSA_MAX_PAGES = 100
_BUSINESSUSA_SOLD_RE = re.compile(r"^\W*sold\W*$", re.I)
_BUSINESSUSA_UC_RE = re.compile(r"^\W*under[\s-]*contract\W*$", re.I)
_BUSINESSUSA_TOTAL_RE = re.compile(r"Showing\s+(\d[\d,]*)\s+of\s+(\d[\d,]*)", re.I)


def businessusa_remark_status(remark):
    """Map one Remarks cell to (status, broker_status_text, broker_note)."""
    text = re.sub(r"\s+", " ", (remark or "").replace("\xa0", " ")).strip()
    if not text:
        return "active", None, None
    if _ls_lookup is not None:
        hit = _ls_lookup(text, BUSINESSUSA_HOST)
        if hit and hit[1] is not None:       # sold / under-contract family / DROP / inactive
            return hit[0], hit[1], None
        return "active", None, text          # plain 'For Sale' or a free note
    if _BUSINESSUSA_SOLD_RE.match(text):
        return "sold", text, None
    if _BUSINESSUSA_UC_RE.match(text):
        return "active", text, None
    return "active", None, text


def _businessusa_state(*cells):
    """State column holds 'VA', 'Virginia', 'Washington DC', 'Northern VA';
    a few rows swap County and State. Try each cell, most specific first."""
    for raw in cells:
        t = (raw or "").strip()
        if not t:
            continue
        if re.search(r"\bD\.?C\.?\b|district of columbia", t, re.I):
            return "DC"
        if t.upper() in US_STATES:
            return t.upper()
        if t.lower() in STATE_NAME_TO_ABBREV:
            return STATE_NAME_TO_ABBREV[t.lower()]
        last = t.split()[-1].upper()
        if len(last) == 2 and last in US_STATES:
            return last
    return None


def _businessusa_money(text):
    digits = re.sub(r"[^\d.]", "", text or "")
    try:
        return positive_or_none(float(digits)) if digits else None
    except ValueError:
        return None


def parse_businessusa_rows(html, base_url, broker_name, dropped=None):
    """Parse one businessusa.biz index page into listing dicts. A row whose
    remark says Coming Soon / For Lease is not emitted; its remark is
    appended to `dropped` (if given) so the caller can log the count."""
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for table in soup.find_all("table"):
        heads = [th.get_text(" ", strip=True).lower() for th in table.find_all("th")]
        if not heads or not any(h.startswith("listing") for h in heads) \
                or "remarks" not in heads:
            continue
        col = {h: i for i, h in enumerate(heads)}
        i_num = next(i for h, i in col.items() if h.startswith("listing"))
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < len(heads):
                continue

            def cell(name):
                i = col.get(name)
                return tds[i].get_text(" ", strip=True) if i is not None else ""

            num = re.sub(r"\D", "", tds[i_num].get_text(" ", strip=True))
            if not num:
                continue
            biz_type, county, state = cell("type"), cell("county"), cell("state")
            price, annual = _businessusa_money(cell("price")), _businessusa_money(cell("annual"))
            remark = re.sub(r"\s+", " ", tds[col["remarks"]].get_text(" ", strip=True)
                            .replace("\xa0", " ")).strip()
            status, status_text, note = businessusa_remark_status(remark)
            if status == _LS_DROP:
                if dropped is not None:
                    dropped.append(status_text)
                continue

            detail_url = f"https://{BUSINESSUSA_HOST}/business/{num}"
            where = ", ".join(p for p in (f"{county} County" if county else "",
                                          state) if p)
            title = f"{biz_type or 'Business'}{' – ' + where if where else ''} (#{num})"
            desc = f"{biz_type or 'Business'} for sale" + (f" in {where}" if where else "") + "."
            if price:
                desc += f" Asking ${price:,.0f}."
            if annual:
                desc += f" Annual sales ${annual:,.0f}."
            if remark:
                desc += f" Remarks: {remark}"
            rec = _make_api_listing(
                base_url, broker_name, title=title, detail_url=detail_url,
                asking_price=price, revenue=annual,
                state=_businessusa_state(state, county),
                description=desc)
            rec["broker_domain"] = BUSINESSUSA_HOST
            rec["broker_listing_ref"] = num
            rec["status"] = status
            rec["status_source"] = "index_remarks"
            rec["broker_status_text"] = status_text
            rec["broker_note"] = note
            out.append(rec)
    return out


def _handler_businessusa(url, html, broker_name, fetcher):
    """Walk every index page (?page=N) and parse the Remarks table."""
    if _bare(urlparse(url).netloc) != BUSINESSUSA_HOST:
        return None
    p = urlparse(url)
    page_base = f"{p.scheme or 'https'}://{p.netloc}{p.path or '/business-for-sale'}"
    m = _BUSINESSUSA_TOTAL_RE.search(BeautifulSoup(html or "", "html.parser")
                                     .get_text(" ", strip=True))
    displayed = int(m.group(2).replace(",", "")) if m else None

    by_ref, dropped = {}, []
    page_html, page = html, 1
    while page <= _BUSINESSUSA_MAX_PAGES:
        rows = parse_businessusa_rows(page_html, url, broker_name, dropped)
        new = [r for r in rows if r["broker_listing_ref"] not in by_ref]
        for r in new:
            by_ref[r["broker_listing_ref"]] = r
        # dropped rows are still rows the site lists: they count toward its total
        if not new or (displayed and len(by_ref) + len(dropped) >= displayed):
            break
        page += 1
        try:
            r = fetcher.session.get(f"{page_base}?page={page}", timeout=REQ_TIMEOUT)
            if r.status_code != 200:
                print(f"   ⚠️  businessusa page {page}: HTTP {r.status_code}")
                break
            page_html = r.text
        except Exception as exc:
            print(f"   ⚠️  businessusa page {page}: {exc}")
            break

    out = list(by_ref.values())
    sold = sum(1 for r in out if r["status"] == "sold")
    print(f"   🗂  businessusa index: {len(out)} rows over {page} page(s), "
          f"site says {displayed}; sold={sold} active={len(out) - sold}"
          f"{f'; status DROP (not emitted): {len(dropped)}' if dropped else ''}")
    if displayed and len(out) + len(dropped) != displayed:
        print(f"   ⚠️  businessusa: parsed {len(out)} (+{len(dropped)} not emitted) "
              f"!= displayed {displayed}")
    return out or None


API_HANDLERS.append((BUSINESSUSA_HOST, _handler_businessusa))


# ============================================================
# Astro island props blob — corbettrestaurantgroup.com (2026-11-12)
# ============================================================
#
# /listings is an Astro page whose <astro-island component-url=".../
# ListingsContainer.*.js" props="..."> carries EVERY listing the site lists
# (178 on 2026-11-12, "Showing 1 to 30 of 178 listings") as JSON in Astro's
# serialised form ({"k": [0, value]}, arrays [1, [...]]). The cards are drawn
# client-side 30 at a time, so the generic path saw page 1 only (6 rows in the
# DB, URLs like /listings?page=4). The blob is the site's own data: read it as
# a feed, like the Base44 / Tupelo handlers.
#
# Each item has a uuid `id`, a `slug` (the card links /listings/<slug>, which
# returns 200), title, listing_price / cash_flow / sales, location, and a
# `status` of active | new_listing | price_reduced | under_agreement |
# off_market | sold. Status goes through the shared listing_status phrase set
# (decision 6): sold -> sold; under agreement / off market -> active with the
# broker's words; new / reduced / active -> active. A type of for_lease is a
# space for lease, not a business (decision 5): not emitted, counted.
CORBETT_HOST = "corbettrestaurantgroup.com"
_ASTRO_LISTINGS_COMPONENT = "ListingsContainer"


def _astro_decode(v):
    """Astro's island prop serialisation -> plain JSON values.
    [0, x] = value (objects recurse), [1, [...]] = array; other tags
    (dates, maps, ...) keep their payload."""
    if isinstance(v, list) and len(v) == 2 and isinstance(v[0], int):
        tag, x = v
        if tag == 0:
            return ({k: _astro_decode(y) for k, y in x.items()}
                    if isinstance(x, dict) else x)
        if tag == 1 and isinstance(x, list):
            return [_astro_decode(y) for y in x]
        return x
    if isinstance(v, dict):
        return {k: _astro_decode(y) for k, y in v.items()}
    return v


def astro_island_props(html, component):
    """Decoded props of the first <astro-island> whose component-url names
    `component`, or None."""
    soup = BeautifulSoup(html or "", "html.parser")
    for isl in soup.find_all("astro-island"):
        if component not in (isl.get("component-url") or ""):
            continue
        try:
            raw = json.loads(isl.get("props") or "{}")
        except ValueError:
            return None
        return {k: _astro_decode(v) for k, v in raw.items()}
    return None


def parse_corbett_props(html, base_url, broker_name, dropped=None):
    props = astro_island_props(html, _ASTRO_LISTINGS_COMPONENT) or {}
    items = props.get("allListings")
    if not isinstance(items, list):
        return None
    p = urlparse(base_url)
    root = f"{p.scheme or 'https'}://{p.netloc}"
    out, seen = [], set()
    for it in items:
        if not isinstance(it, dict) or it.get("deleted_at"):
            continue
        slug, title = (it.get("slug") or "").strip(), (it.get("title") or "").strip()
        if not slug or not title or slug in seen:
            continue
        seen.add(slug)
        if (it.get("type") or "").strip().lower() == "for_lease":
            if dropped is not None:
                dropped.append("For Lease (type)")
            continue
        raw_status = (it.get("status") or "").replace("_", " ").strip().title()
        hit = _ls_lookup(raw_status, CORBETT_HOST) if (_ls_lookup and raw_status) else None
        status, status_text = (hit if hit else ("active", None))
        if status == _LS_DROP:
            if dropped is not None:
                dropped.append(status_text or raw_status)
            continue
        loc = (it.get("location") or "").strip()
        city = it.get("city") or None
        state = (it.get("state") or "").strip().upper() or None
        if state not in US_STATES:
            state = _state_from_location(loc)
        desc = re.sub(r"\s+", " ", BeautifulSoup(it.get("content") or "", "html.parser")
                      .get_text(" ", strip=True))
        rec = _make_api_listing(
            base_url, broker_name, title=title,
            detail_url=f"{root}/listings/{slug}",
            asking_price=it.get("listing_price"), cash_flow=it.get("cash_flow"),
            revenue=it.get("sales"), city=city, state=state,
            description=(f"{loc}. " if loc else "") + desc)
        rec["broker_listing_ref"] = str(it.get("id") or "") or None
        rec["status"] = status
        rec["status_source"] = "index_feed"
        rec["broker_status_text"] = status_text
        out.append(rec)
    return out


def _handler_corbett(url, html, broker_name, fetcher):
    if _bare(urlparse(url).netloc) != CORBETT_HOST:
        return None
    dropped = []
    out = parse_corbett_props(html, url, broker_name, dropped)
    if out is None:
        print("   ⚠️  corbett: no ListingsContainer props on the page - generic path")
        return None
    sold = sum(1 for r in out if r["status"] == "sold")
    print(f"   🗂  corbett props blob: {len(out)} rows; sold={sold} "
          f"active={len(out) - sold}"
          + (f"; not emitted: {len(dropped)} ({', '.join(sorted(set(dropped)))})"
             if dropped else ""))
    return out or None


API_HANDLERS.append((CORBETT_HOST, _handler_corbett))


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
        # Domains whose server reset the HTTP/2 stream (curl 92) on the
        # default fingerprint and then served the fallback-family one —
        # later pages of the run go straight to RESET_FALLBACK_IMPERSONATE.
        self._reset_fallback_domains = set()
        self.stats = {"h2_reset_fallback": 0, "h2_reset_fallback_ok": 0}

    @staticmethod
    def _new_session():
        """A curl_cffi session impersonating IMPERSONATE (default chrome145,
        DL_IMPERSONATE overrides). chrome124 went stale ~6/30/2026 (Akamai
        403s); chrome131 ~11/2026 (laundromat-broker.com curl 92)."""
        if HAS_CURL_CFFI:
            return cffi_requests.Session(impersonate=IMPERSONATE)
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

    # Fingerprint for the one-shot retry after an HTTP/2 stream reset.
    # laundromat-broker.com (nginx) resets every request whose User-Agent says
    # Chrome < 137 — curl 92 over HTTP/2, curl 56 over HTTP/1.1, plain curl
    # too — so retrying the same fingerprint over HTTP/1.1 does not help; a
    # different one does (verified 2026-11-11). With the default now chrome145
    # the retry goes to the NEXT family (Safari 26), so it still has somewhere
    # to go when a server starts rejecting the current Chrome.
    RESET_FALLBACK_IMPERSONATE = _reset_fallback_for(IMPERSONATE)

    @staticmethod
    def _is_h2_stream_reset(exc):
        """curl error 92 (HTTP/2 stream reset by server, e.g. PROTOCOL_ERROR)."""
        if getattr(exc, "code", None) == 92:
            return True
        msg = str(exc)
        return "PROTOCOL_ERROR" in msg or "HTTP/2 stream" in msg

    def _get(self, url, use_proxy, timeout, tls_kw, impersonate=None):
        kw = dict(tls_kw)
        if impersonate:
            kw["impersonate"] = impersonate
        return self.session.get(
            url, headers=self._headers(), timeout=timeout,
            allow_redirects=True,
            proxies=self._proxy_dict() if use_proxy else None,
            **kw,
        )

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
        tls_kw = {"verify": False} if _bare(domain) in INSECURE_TLS_DOMAINS else {}
        # Per-domain pin (regression narrowing); None = session default.
        pin = IMPERSONATE_DOMAIN_OVERRIDES.get(_bare(domain)) if HAS_CURL_CFFI else None
        fb_target = _reset_fallback_for(pin) if pin else self.RESET_FALLBACK_IMPERSONATE
        fb_imp = (fb_target
                  if HAS_CURL_CFFI and domain in self._reset_fallback_domains else None)
        reset_retried = False
        resp = None
        for attempt in range(MAX_403_RETRIES + 1):
            resp = None
            try:
                resp = self._get(url, use_proxy, timeout, tls_kw,
                                 impersonate=fb_imp or pin)
            except Exception as e:
                # HTTP/2 stream reset: retry ONCE, same proxy setting, with the
                # next fingerprint family. If that also fails, fall through
                # with the ORIGINAL exception so the failure keeps its shape
                # (FETCH_ERROR) — the retry must never hide a real failure.
                if (HAS_CURL_CFFI and not reset_retried and fb_imp is None
                        and self._is_h2_stream_reset(e)):
                    reset_retried = True
                    self.stats["h2_reset_fallback"] += 1
                    print(f"   ↻ HTTP/2 PROTOCOL_ERROR on {domain}, retrying once "
                          f"as {fb_target}")
                    try:
                        resp = self._get(url, use_proxy, timeout, tls_kw,
                                         impersonate=fb_target)
                    except Exception as e2:
                        print(f"   ↻ fallback failed on {domain}: {str(e2)[:120]}")
                        resp = None
                    if resp is not None:
                        self.stats["h2_reset_fallback_ok"] += 1
                        self._reset_fallback_domains.add(domain)
                        fb_imp = fb_target
                if resp is None:
                    # Dead proxy IP / timeout — re-warm onto a fresh IP and retry.
                    if attempt < MAX_403_RETRIES:
                        time.sleep(BACKOFF_BASE * (2 ** attempt) + random.uniform(0, 1.5))
                        self._rewarm(url, use_proxy)
                        continue
                    raise e

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
            # Keep scrolling to the bottom until the page stops growing:
            # four fixed wheel scrolls stopped restaurantrealty.com's
            # infinite-scroll widget (12 cards per fetch) at 55-114 of 124
            # listings, and nothing said so. Never fewer rounds than the old
            # fixed four; a cap reached while still growing is logged.
            prev_h, stable = None, 0
            self.last_scroll_growth = 0
            for i in range(SCROLL_MAX_ROUNDS):
                page.mouse.wheel(0, 4000)
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                page.wait_for_timeout(1200)
                try:
                    h = page.evaluate("document.body.scrollHeight")
                except Exception:
                    h = None
                if h is not None and h == prev_h:
                    stable += 1
                    if stable >= 2 and i >= 3:
                        break
                    # A slow (proxied) XHR may not have landed yet.
                    try:
                        page.wait_for_load_state("networkidle", timeout=3000)
                    except Exception:
                        pass
                else:
                    if prev_h is not None and h is not None:
                        self.last_scroll_growth += 1
                    stable = 0
                prev_h = h
            else:
                print(f"   ⚠️  infinite scroll still growing after "
                      f"{SCROLL_MAX_ROUNDS} rounds - page may be truncated")
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

    # Link text that means "next page" once whitespace (incl. &nbsp;) is
    # collapsed and case folded. "››" / "»" alone are Drupal / WP pagers.
    _NEXT_TEXTS = {"next", "next »", "next ›", "next >", "next page", "next »»",
                   "→", ">", "›", "»", "››", ">>", "next →"}

    @staticmethod
    def _norm_text(s):
        return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip().lower()

    @classmethod
    def find_next_page(cls, soup, current_url, exclude=None):
        """`exclude`: URLs that cannot be the next page - the detail URLs of
        this page's own cards. A card's "›" / ">" arrow link matched the
        bare-arrow text rule and sent the crawl to a listing's detail page as
        "page 2" (crossroadsbusiness.com, listings.kingsleybrokers.com,
        2026-11-12); such a match is skipped and the search goes on."""
        exclude = {canonical_url(u) for u in (exclude or ()) if u}

        def ok(href):
            return canonical_url(urljoin(current_url, href)) not in exclude

        for sel in ["a.next", "a.next-page", "a[rel='next']", "li.next a",
                    "a.pagination-next", "a[aria-label='Next']",
                    "li.pager__item--next a", "a.page-numbers.next",
                    ".pagination a.active + a"]:
            try:
                el = soup.select_one(sel)
                if (el and el.get("href") and not el["href"].startswith(("#", "javascript"))
                        and ok(el["href"])):
                    return urljoin(current_url, el["href"])
            except Exception:
                pass

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith(("#", "javascript")) or not ok(href):
                continue
            label = cls._norm_text(a.get("aria-label") or a.get("title"))
            if label in ("next", "next page", "go to next page"):
                return urljoin(current_url, href)
            if cls._norm_text(a.get_text(" ", strip=True)) in cls._NEXT_TEXTS:
                return urljoin(current_url, href)
        return None

    @classmethod
    def find_numbered_page(cls, soup, current_url, next_num):
        """The page's own link to page `next_num`, for pagers with numbered
        links and no "next" (Toolset Views ?wpv_view_count=..&wpv_paged=N,
        2026-11-12: v-aidgroup.com). Principle 9: the pager says what its
        URLs are, so read them instead of guessing ?page=N.

        An <a> counts when its text (or data-page) is exactly the number, it
        stays on the same host, and the number appears in its URL as a query
        value or a whole path segment - so a "2" in a menu or a phone number
        never qualifies. None when there is no such link."""
        want = str(next_num)
        host = _bare(urlparse(current_url).netloc)
        cur = urlparse(current_url)
        for a in soup.find_all("a", href=True):
            href = (a.get("href") or "").strip()
            if not href or href.startswith(("#", "javascript", "mailto:", "tel:")):
                continue
            txt = cls._norm_text(a.get_text(" ", strip=True))
            if txt != want and (a.get("data-page") or "").strip() != want:
                continue
            full = urljoin(current_url, href)
            pu = urlparse(full)
            if _bare(pu.netloc) != host:
                continue
            if (pu.path, pu.query) == (cur.path, cur.query):
                continue
            in_query = any(want in vals for vals in parse_qs(pu.query).values())
            in_path = want in [seg for seg in pu.path.split("/") if seg]
            if in_query or in_path or cls._in_pager(a):
                return full
        return None

    _PAGER_HINT = re.compile(r"paginat|pager|page-numbers", re.I)

    @classmethod
    def _in_pager(cls, a, depth=4):
        """The link sits inside the page's own pager: an ancestor (within
        `depth` levels) whose class or aria-label says pagination. Covers
        offset pagers whose URL carries the offset, not the page number
        (krbrokers.com: "2" -> ?limitstart=10)."""
        el = a
        for _ in range(depth):
            el = el.parent
            if el is None or el.name in ("body", "html", "[document]"):
                return False
            hint = " ".join(el.get("class") or []) + " " + (el.get("aria-label") or "")
            if cls._PAGER_HINT.search(hint):
                return True
        return False

    # Per-broker override, from broker_sources.pagination_mode /
    # pagination_param (or --pagination MODE[:PARAM] on a --broker test):
    #   param   ?<param>=N, 1-based (default param 'page')
    #   param0  ?<param>=N-1, 0-based (Drupal pagers: ?page=1 is page 2)
    #   offset  ?<param>=(N-1)*page_size (default param 'offset')
    #   path    /page/N/
    #   none    page 1 only (site renders every listing at once)
    _OVERRIDE_DEFAULTS = {"param": "page", "param0": "page", "offset": "offset",
                          "path": None}

    @classmethod
    def parse_override(cls, mode, param=None):
        """Template tuple, the string 'none', or None (no override)."""
        mode = (str(mode).strip().lower() if mode is not None else "")
        if mode in ("", "nan", "auto"):
            return None
        if mode == "none":
            return "none"
        if mode not in cls._OVERRIDE_DEFAULTS:
            print(f"   ⚠️  unknown pagination_mode {mode!r} - ignoring")
            return None
        p = (str(param).strip() if param is not None and str(param).strip()
             not in ("", "nan") else cls._OVERRIDE_DEFAULTS[mode])
        return (mode, p)

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
        ?p=N / ?pg=N / ?wpv_paged=N, ?offset=N*size / ?start=N*size, /page/N/.

        0-based ?page=N-1 goes first: on a 1-based site ?page=1 serves page 1
        again and is rejected by the caller's first-page check, so the cost is
        one fetch; on a Drupal site it is the only candidate that lands on
        page 2 instead of skipping to page 3."""
        out = [(cls._with_param(base_url, "page", page_num - 1), ("param0", "page"))]
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
        if kind == "param0":
            return cls._with_param(base_url, param, page_num - 1)
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

# One rule, shared with run_specialized.py, for "the broker explicitly withholds
# the price" -> listings_direct.price_not_disclosed / broker_price_text.
from price_disclosure import (
    DisclosureColumns, disclosure_fields, find_labelled_withheld_price,
    flagged_without_phrase, group_by_keys, is_missing_disclosure_column,
    strip_disclosure,
)

# The broker's verbatim status label (decision 6) -> broker_status_text, with
# broker_status_seen_at. Same optional-column mechanism as the disclosure
# fields (optional_columns.py), shared with run_specialized.py (principle 15).
from optional_columns import missing_group, new_status_label_columns, status_label_fields

# price_not_disclosed / broker_price_text may not exist yet (w14 migration);
# broker_status_text / broker_status_seen_at may not (j3 migration).
# Module-level, not set in __init__, so a writer built without __init__ (tests
# with a fake client) still works; remembered per process so we log once.
_DISCLOSURE = DisclosureColumns()
_STATUS_LABEL = new_status_label_columns()


def _optional_groups():
    """Looked up at call time (tests rebind the module globals)."""
    return (_DISCLOSURE, _STATUS_LABEL)


class SupabaseWriter:

    def __init__(self):
        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_SERVICE_KEY")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set")
        if not HAS_SUPABASE:
            raise RuntimeError("pip3 install supabase")
        self.client = create_client(url, key)

    def _upsert_batch(self, batch: list[dict]) -> None:
        """One PostgREST upsert. If it fails ONLY because an optional column
        group (disclosure, status label) doesn't exist yet, strip that group,
        note it, and retry — at most once per group. The response is read, the
        drop is counted (principle 17)."""
        groups = _optional_groups()
        for g in groups:
            g.strip_if_missing(batch)
        while True:
            try:
                self.client.table("listings_direct").upsert(
                    batch, on_conflict="id", ignore_duplicates=False
                ).execute()
                return
            except Exception as exc:
                g = missing_group(exc, groups)
                if g is None:
                    raise
                g.note_missing(print, g.strip(batch))

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
            # Sent only with evidence: a price (-> false) or the broker's
            # explicit phrase (-> true + verbatim). Neither -> not sent, so an
            # existing row keeps its value and a new row gets the default.
            disclosure = disclosure_fields(l)
            if flagged_without_phrase(l):
                _DISCLOSURE.flag_no_phrase += 1
            # Same rule for the broker's status label: sent only when the row
            # carries one, so a row without a label never nulls a stored one
            # (principle 14).
            label = status_label_fields(l, now)
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
                **(disclosure or {}),
                **(label or {}),
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

        # PostgREST needs uniform keys per POST: rows with and without the
        # optional fields (disclosure, status label) are written as separate
        # batches.
        batches = [g[i:i + 100] for g in group_by_keys(rows)
                   for i in range(0, len(g), 100)]

        written = 0
        for batch in batches:
            try:
                self._upsert_batch(batch)
                written += len(batch)
            except Exception as exc:
                # One bad row must not cost the other 99. Retry singly so the
                # rest land and the offender is named.
                print(f"   ⚠️  batch upsert failed ({exc}); retrying rows individually")
                for row in batch:
                    try:
                        self._upsert_batch([row])
                        written += 1
                    except Exception as row_exc:
                        print(f"      row {row.get('id')} "
                              f"({(row.get('title') or '')[:50]}): {row_exc}")
            time.sleep(0.1)
        for g in _optional_groups():
            if g.summary():
                print(f"   ⓘ  {g.summary()}")
        if _DISCLOSURE.flag_no_phrase:
            print(f"   ⓘ  {_DISCLOSURE.flag_no_phrase} row(s) so far flagged "
                  f"price_not_disclosed by the scraper with no broker phrase - not written")
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

        fp = fingerprint_banner()
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
            # one entry per broker whose cached pattern extracted 0 cards
            # (STALE_CACHE) or that fell back to the www/bare twin's entry;
            # lands in summary.json so a dead cache entry is never silent
            "pattern_cache_events": [],
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
            b = {"name": name, "url": url, "domain": domain}
            # Optional per-broker pagination override (export_brokers_csv.py
            # carries broker_sources.pagination_mode / pagination_param).
            for col in ("pagination_mode", "pagination_param"):
                if col in df.columns and pd.notna(row.get(col)) and str(row[col]).strip():
                    b[col] = str(row[col]).strip()
            brokers.append(b)

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
        "routesforsale.net", "eatz-associates.com",
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

                # WEEKLY BACKOFF (2026-11-10). 27% of a week's V6 runs went to
                # brokers that never once succeeded, retried every night by the
                # fill-the-budget step above. v6_backoff_domains() names those
                # tried in the last 6 days with 3+ failures, no ok run in 14
                # days and nothing published; they wait for next week instead.
                # Brokers with live listings on the site are never skipped.
                backoff = self._backoff_domains()
                if backoff:
                    before = len(ordered)
                    ordered = [b for b in ordered if _bare(b["domain"]) not in backoff]
                    print(f"⏸️  Weekly backoff — skipping {before - len(ordered)} "
                          f"repeatedly failing brokers tonight")
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

    def _backoff_domains(self) -> set:
        """Bare domains v6_backoff_domains() says to skip tonight.

        Read-only. Off with V6_BACKOFF=0. Any error (function not deployed
        yet, network) returns an empty set so the run crawls as before.
        """
        if os.environ.get("V6_BACKOFF", "1") == "0" or not self.supabase_writer:
            return set()
        try:
            rows = (self.supabase_writer.client
                    .rpc("v6_backoff_domains", {}).execute().data) or []
            return {(r.get("bare_domain") or "").lower() for r in rows if r.get("bare_domain")}
        except Exception as e:
            print(f"⚠️  v6_backoff_domains unavailable ({type(e).__name__}); no backoff tonight")
            return set()

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
        # A --broker test run (Supabase disabled) must not write crawl_run
        # either. start_run() only checks for a service key, which is in .env
        # and the shell, so until 2026-11-11 every "no-write" --broker test
        # opened and closed a production crawl_run row (principle 16).
        # supabase_writer is None exactly when listings are not being written.
        log_runs = self.supabase_writer is not None
        run_id = start_run("v6", broker.get("domain")) if log_runs else None
        failures_before = len(self.failures)
        # Displayed count (2026-11-11): _scrape_broker_inner feeds every index
        # page it fetches into this tracker; see scrapers/displayed_count.py.
        self._displayed = DisplayedCountTracker()
        try:
            listings = self._scrape_broker_inner(broker)
        except Exception as e:                       # inner shouldn't raise, but
            finish_run(run_id, "failed", error=str(e))
            raise

        # seen = one more plausibility signal for a weak stated count (j3-polish)
        dc = self._displayed.result(seen=len(listings)) or {}
        dc_kw = {"displayed_count": dc.get("count"),
                 "displayed_source": dc.get("source"),
                 "displayed_note": "; ".join(x for x in (
                     dc.get("text") and f"matched: {dc['text']}", dc.get("note")) if x) or None}
        if dc:
            print(f"   📏 broker displays {dc['count']} ({dc['source']}"
                  + (f": '{dc['text']}'" if dc.get("text") else "")
                  + f") vs {len(listings)} seen -> {verdict_label(len(listings), dc['count'])}"
                  + (f"\n      [{dc['note'][:300]}]" if dc.get("note") else ""))
        else:
            print(f"   📏 no displayed count found -> {verdict_label(len(listings), None)}")

        if len(self.failures) > failures_before:
            ftype = self.failures[-1].get("type", "UNKNOWN")
            finish_run(run_id, "failed", error=ftype, **dc_kw)
        elif listings:
            finish_run(run_id, "ok", len(listings), len(listings), **dc_kw)
        else:
            # 'empty' is not an allowed status (crawl_run_status_check:
            # running/ok/failed/partial/backfill). The PATCH was rejected and
            # the row stayed 'running' forever. Zero listings is not a
            # coverage window, so it closes as failed.
            finish_run(run_id, "failed", 0, 0, error="EMPTY", **dc_kw)
        return listings

    def _dc_page(self, page_html: str, count: bool = True) -> None:
        """Feed one index page to the displayed-count tracker. Never raises."""
        t = getattr(self, "_displayed", None)
        if t is not None:
            t.add_page(page_html, count=count)

    def _scrape_broker_inner(self, broker: dict) -> list[dict]:
        name      = broker["name"]
        url       = broker["url"]
        # Pattern cache + proxy are keyed on the canonical host, so the bare
        # and www spellings of one site share one learned pattern
        # (calhouncompanies.com's bare key held a stale div.block selector
        # that matched one card per page).
        domain    = canonical_host(broker["domain"])
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
            self._dc_page(html, count=False)  # stated count only; loop counts cards

            # ── API handler (JS sites with a JSON feed) ───────────────────
            # If this is a known JS platform, pull listings straight from its
            # JSON endpoint and skip DOM pattern detection entirely.
            api_listings = extract_via_api(url, html, name, self.fetcher)
            if api_listings:
                print(f"   🔌 API handler → {len(api_listings)} listings")
                p  = sum(1 for l in api_listings if l.get("asking_price"))
                st = sum(1 for l in api_listings if l.get("state"))
                print(f"   💰 price={p}  state={st}/{len(api_listings)}")
                syn = assign_synthetic_urls(api_listings, url)
                if syn["synthetic"]:
                    print(f"   #️⃣  synthetic URLs: {syn['synthetic']} "
                          f"(slug collisions left {syn['unresolved_collisions']})")
                self.stats["brokers_success"] += 1
                self.stats["detail_pages_fetched"] += 0
                return api_listings

            # ── Pattern detection ─────────────────────────────────────────
            forced_card = card_selector_override(domain)
            cached = ({"container_selector": forced_card} if forced_card
                      else self.pattern_cache.get(domain))
            # A cached pattern is a shortcut past detection, so a dead one
            # (the site was rebuilt) used to fail as NO_PATTERN every night
            # with detection and the Playwright re-detect never running
            # (ninbb.com, momentumprojects.com, districtbusinessbrokers.com).
            # The nightly job never commits pattern_cache.json, so this has to
            # heal inside the run: a cached selector that extracts 0 cards
            # from page 1 is dropped for this run and detection runs as if
            # nothing were cached.
            stale_sel = None
            if (cached and not forced_card
                    and not pattern_card_count(html, url, cached["container_selector"],
                                               name, stop_at=1)):
                stale_sel = cached["container_selector"]
                print(f"   ♻️  STALE_CACHE: cached pattern {stale_sel} extracts 0 cards "
                      f"from this page - ignoring it, re-detecting")
                self.stats["pattern_cache_events"].append(
                    {"broker": name, "domain": domain, "event": "stale_cache",
                     "selector": stale_sel})
                cached = None
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
                    except Exception as e:
                        print(f"   ⚠️  Playwright re-detect failed: "
                              f"{type(e).__name__}: {str(e)[:120]}")

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

                # Last resort before NO_PATTERN: an entry learned under the
                # www / bare twin of this host (patriotbusinessadvisors.com's
                # working selector sits under www.; the nightly crawls the
                # bare host). Only used if it extracts cards from this page.
                alt_used = False
                if not pattern:
                    alt = self.pattern_cache.other_spelling(domain)
                    if alt and alt.get("container_selector") != stale_sel:
                        for cand_html in (html, rendered_html):
                            if cand_html and pattern_card_count(
                                    cand_html, url, alt["container_selector"], name, stop_at=1):
                                pattern, html, alt_used = alt, cand_html, True
                                print(f"   📦 Cached pattern (www/bare twin): "
                                      f"{alt['container_selector']}")
                                self.stats["pattern_cache_events"].append(
                                    {"broker": name, "domain": domain, "event": "twin_spelling",
                                     "selector": alt["container_selector"]})
                                break

                if pattern and alt_used:
                    self.pattern_cache.store(domain, pattern)
                elif pattern:
                    # Re-detecting after a dead cache entry: this broker's own
                    # page only. Another broker's generic selector is the
                    # weakest evidence there is (proformapartners.com healed
                    # to div[class*="elementor"] and wrote webinar links).
                    predicted = (None if stale_sel
                                 else self.pattern_cache.predict(html, url))
                    if predicted_beats_detected(predicted, pattern, html, url, name):
                        pattern = predicted
                        print(f"   🧠 ML predicted: {pattern['container_selector']}")
                    else:
                        print(f"   🔎 Detected: {pattern['container_selector']} "
                              f"({pattern['count']} elements)")
                    exposed = pattern.pop("_exposed_by_yield_guard", False)
                    bad = (healed_pattern_problem(html, url, pattern["container_selector"], name)
                           if (stale_sel or exposed) else None)
                    if bad:
                        # A broker whose cached pattern died, or whose detected
                        # pattern only surfaces because the yield guard dropped
                        # the prediction that hid it, has no history for the
                        # new guess; it has to look like a listing grid before
                        # it writes anything (businessadvisors.us re-detected 1
                        # "Business Valuation Calculator" card,
                        # ccbusinessbrokers.com 7 cards on one NDA link,
                        # laundrybuyers.com two "Listings" nav cards).
                        why = (f"cached {stale_sel} is dead" if stale_sel
                               else "ML prediction rejected")
                        print(f"   ⚠️  NO_PATTERN ({why}; re-detected "
                              f"{pattern['container_selector'][:80]} rejected: {bad})")
                        if stale_sel:
                            self.stats["pattern_cache_events"][-1]["rejected"] = \
                                f"{pattern['container_selector']}: {bad}"
                        self.failures.append({"broker": name, "url": url,
                                              "error": f"{why}; detected pattern rejected: {bad}",
                                              "type": "NO_PATTERN",
                                              **({"stale_cache": stale_sel} if stale_sel else {}),
                                              "html_size": len(html)})
                        self.stats["brokers_failed"] += 1
                        self.stats["failure_types"]["NO_PATTERN"] += 1
                        return []
                    if stale_sel:
                        self.pattern_cache.replace(domain, pattern)
                        print(f"   ♻️  STALE_CACHE healed: {stale_sel} → "
                              f"{pattern['container_selector']}")
                        self.stats["pattern_cache_events"][-1]["healed_to"] = \
                            pattern["container_selector"]
                    else:
                        self.pattern_cache.store(domain, pattern)
                    self.stats["patterns_learned"] += 1
                else:
                    print(f"   ⚠️  NO_PATTERN"
                          + (f" (cached {stale_sel} is dead, re-detect found nothing)"
                             if stale_sel else ""))
                    self.failures.append({"broker": name, "url": url,
                                          "error": "No listing pattern detected",
                                          "type": "NO_PATTERN",
                                          **({"stale_cache": stale_sel} if stale_sel else {}),
                                          "html_size": len(html)})
                    self.stats["brokers_failed"] += 1
                    self.stats["failure_types"]["NO_PATTERN"] += 1
                    return []

            # Striped tables: read both row classes, not just the one cached.
            _sel = pattern["container_selector"]
            _wide = widen_striped_rows(html, _sel, url)
            if _wide != _sel:
                print(f"   🦓 striped rows: {_sel} → {_wide}")
                pattern = dict(pattern, container_selector=_wide)

            # ── Phase 1: extract cards from all list pages ────────────────
            # Follows explicit next-links; when a site has none (numbered / JS
            # pagination), falls back to constructing ?page=N / /page/N/ /
            # ?offset= URLs, locks onto whichever template yields new cards, and
            # stops when a page adds nothing new or repeats page 1.
            unique_cards: list[dict] = []
            ListingExtractor.status_drops = []   # Coming Soon / For Lease cards, this broker
            seen_ids: set[str] = set()
            first_page_ids: set[str] = set()
            page_size = 0
            page_num = 1
            max_pages = 50
            current_url = url
            pending_html = html          # page 1 already fetched
            template = None              # locked pagination template once found
            stop_reason = "max_pages"    # printed with the card count
            # Per-broker override (broker_sources.pagination_mode/_param).
            # A forced template is used for every page and skips next-link
            # following and template discovery.
            override = PaginationHandler.parse_override(
                broker.get("pagination_mode"), broker.get("pagination_param"))
            forced = override if isinstance(override, tuple) else None
            if forced:
                template = forced
                print(f"   🔧 pagination override: {forced[0]}"
                      + (f" ({forced[1]})" if forced[1] else ""))

            wrapper_log = WrapperSkipLog()   # printed once, after the page loop

            def _cards_from(html_text, page_url):
                soup = BeautifulSoup(html_text, "html.parser")
                try:
                    els = soup.select(pattern["container_selector"])
                except Exception:
                    els = []
                els, n_wrappers = drop_wrapper_matches(
                    els, lambda e: ListingExtractor.from_card(e, page_url, name) is not None)
                wrapper_log.add(n_wrappers)
                out = []
                for el in els:
                    c = ListingExtractor.from_card(el, page_url, name)
                    if c:
                        out.append(c)
                return soup, out

            def _card_key(c):
                d = c.get("_detail_url")
                if d and c.get("url_is_listing_specific", True) and d != url:
                    return ("u", d)
                return ("t", (c.get("title") or "")[:80])

            MAX_CARDS_PER_BROKER = 2500
            visited = set()         # index URLs already fetched (pager loop guard)
            via_pager = False       # this page came from the site's own pager link
            stalls, stalled = 0, None

            while page_num <= max_pages:
                if over_budget():
                    print(f"   ⏱️  {BROKER_TIME_BUDGET}s budget reached on page {page_num} "
                          f"- keeping {len(unique_cards)} cards")
                    stop_reason = "time_budget"
                    break
                if len(unique_cards) >= MAX_CARDS_PER_BROKER:
                    print(f"   ⚠️  hit {MAX_CARDS_PER_BROKER}-card ceiling for this "
                          f"broker - stopping (suspect pagination loop)")
                    stop_reason = "card_ceiling"
                    break
                if pending_html is not None:
                    cur_html, pending_html = pending_html, None
                else:
                    time.sleep(random.uniform(1, 3))
                    try:
                        cur_html, _ = self.fetcher.fetch(current_url, use_proxy=use_proxy)
                    except Exception as e:
                        stop_reason = f"fetch_error p{page_num}: {type(e).__name__}"
                        break

                visited.add(canonical_url(current_url))
                soup, page_cards = _cards_from(cur_html, current_url)
                if not page_cards:
                    stop_reason = f"no_cards p{page_num}"
                    break
                page_ids = {c["id"] for c in page_cards}

                # Content fingerprint of the page, independent of how ids are
                # built. The id-set check below cannot fire when ids embed the
                # page URL - quietlight.com returned the same 227 cards on all
                # 50 pages and every one counted as new, producing 11,440 rows
                # for ~300 real listings. Titles are what actually repeat.
                # A card's content key is its own detail URL when it has one,
                # else its title: on a site whose cards all carry the same
                # CTA text ("Full Listing") every page had the same title
                # set, so page 2 always "repeated" page 1 (therouteexchange.com,
                # 2026-11-12); a detail URL is what actually repeats.
                page_sig = hash(frozenset(_card_key(c) for c in page_cards))
                if page_num == 1:
                    first_page_ids = page_ids
                    first_page_sig = page_sig
                    page_size = len(page_cards)
                    seen_sigs = {page_sig}
                else:
                    halt, halt_msg = None, None
                    if page_sig in seen_sigs:
                        halt = f"repeat_content p{page_num}"
                        halt_msg = f"page {page_num} repeats earlier content"
                    seen_sigs.add(page_sig)

                    if not halt and (not (page_ids - seen_ids) or page_ids == first_page_ids):
                        halt = f"no_new_ids p{page_num}"  # nothing new / looped to page 1

                    # A site that ignores the page parameter serves page 1
                    # forever. Titles repeating is the tell.
                    prev_titles = {_card_key(c) for c in unique_cards}
                    page_titles = {_card_key(c) for c in page_cards}
                    if (not halt and page_titles
                            and len(page_titles - prev_titles) / len(page_titles) < 0.2):
                        halt = f"repeat_titles p{page_num}"
                        halt_msg = (f"page {page_num} is "
                                    f"{100 - int(100*len(page_titles - prev_titles)/len(page_titles))}% "
                                    f"repeat titles")
                    if halt:
                        # A page the site's OWN pager linked to (not a guessed
                        # URL) that adds nothing is not a loop when it is not
                        # page 1 again: unionbaygroup.com lists ~32 identical
                        # "Investment Opportunity" cards (one URL) across pages
                        # 1-3, and its other 30 listings are on pages 4-6.
                        # Allow PAGER_STALL_MAX such pages in a row, then stop.
                        if (via_pager and page_ids != first_page_ids
                                and stalls < PAGER_STALL_MAX):
                            stalls += 1
                            print(f"   …  page {page_num} adds nothing ({halt}) - "
                                  f"pager offers more, continuing ({stalls}/{PAGER_STALL_MAX})")
                            stalled = halt
                        else:
                            if halt_msg:
                                print(f"   ↩︎  {halt_msg} - stopping")
                            stop_reason = halt
                            break
                    else:
                        stalls, stalled = 0, None

                new = [c for c in page_cards if c["id"] not in seen_ids]
                # A later page whose new cards are ALL sold is a sold archive
                # (therouteexchange.com pages its "Sold Routes" list only:
                # pages 2-10 = ~720 sold routes, 0 for sale). Stop rather than
                # walk it; page 1's sold cards are kept as before.
                if (page_num > 1 and new
                        and all(c.get("status") == "sold" for c in new)):
                    print(f"   🏁 page {page_num}: all {len(new)} new cards are sold "
                          f"- sold archive, stopping")
                    stop_reason = f"sold_only p{page_num}"
                    break
                unique_cards.extend(new)
                seen_ids |= page_ids
                # Displayed count: only pages accepted as new content (a page
                # that repeats page 1 would double the card count).
                self._dc_page(cur_html)
                if page_num > 1:
                    print(f"   📄 Page {page_num}: {len(new)} new cards")

                # ── decide next page ──────────────────────────────────────
                if override == "none":
                    stop_reason = "override_none"
                    break
                card_urls = {c.get("_detail_url") for c in page_cards}
                nxt = None if forced else PaginationHandler.find_next_page(
                    soup, current_url, exclude=card_urls)
                if not nxt and not forced:
                    nxt = PaginationHandler.find_numbered_page(
                        soup, current_url, page_num + 1)
                if nxt and canonical_url(nxt) in visited:
                    stop_reason = stalled or f"pager_loop p{page_num}"
                    break
                if nxt:
                    current_url, template, via_pager = nxt, None, True
                    page_num += 1
                    continue
                if stalled:                 # a stalled page with no further pager link
                    stop_reason = stalled
                    break
                via_pager = False
                if template:
                    current_url = PaginationHandler.build_from_template(
                        url, template, page_num + 1, page_size)
                    page_num += 1
                    continue
                # No explicit next-link and no locked template yet — discover one.
                # Unless page 1 was an infinite scroll that kept growing past
                # the old four fixed scrolls: it is already the whole list,
                # and rendering ~9 guessed ?page= URLs the same way ate most of
                # the broker budget (restaurantrealty.com, 2026-11-11).
                if (page_num == 1 and method == "playwright"
                        and getattr(self.fetcher, "last_scroll_growth", 0) > 4):
                    stop_reason = "infinite_scroll"
                    break
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
                    stop_reason = ("single_page" if page_num == 1
                                   else f"no_next p{page_num}")
                    break

            if not unique_cards:
                print(f"   ⚠️  Pattern matched but no listings extracted")
                self.failures.append({"broker": name, "url": url,
                                      "error": "Pattern matched, extraction failed",
                                      "type": "NO_PATTERN",
                                      "pattern": pattern["container_selector"]})
                self.stats["brokers_failed"] += 1
                return []

            print(f"   📋 {len(unique_cards)} cards (reached page {page_num}, "
                  f"stopped: {stop_reason}"
                  + (f", template {template[0]}:{template[1]}" if template else "") + ")")
            if wrapper_log.summary():
                print(wrapper_log.summary())
            sf_drops = _STATUS_FIELD_DROPS.pop(_bare(urlparse(url).netloc), None)
            if sf_drops:
                print("   ⓘ  status field: " + ", ".join(
                    f"{len(v)} '{k}'" for k, v in sorted(sf_drops.items()))
                      + " not emitted (Coming Soon / For Lease)")
            # Decision 5, title/badge markers: Coming Soon / For Lease cards
            # are not emitted - say so.
            _drops = {t: lab for t, lab in ListingExtractor.status_drops}
            if _drops:
                _by = {}
                for lab in _drops.values():
                    _by[lab or "?"] = _by.get(lab or "?", 0) + 1
                print(f"   🚫 status DROP: {len(_drops)} cards not emitted ("
                      + ", ".join(f"{k}: {v}" for k, v in sorted(_by.items())) + ")")
            sf_labels = Counter(f"{l.get('status')}:{l['broker_status_text']}"
                                for l in unique_cards if l.get("broker_status_text"))
            if sf_labels:
                print("   🏷️  broker status labels: " + ", ".join(
                    f"{k} {n}" for k, n in sf_labels.most_common()))

            # ── Phase 2: fetch detail pages ───────────────────────────────
            with_detail = [
                c for c in unique_cards
                if c.get("_detail_url") and c["_detail_url"] != url
            ]

            # Listings already enriched on an earlier run: carry the stored
            # detail fields forward instead of fetching the page again.
            stored = self._stored_listings([c["id"] for c in with_detail])
            fin_map = _fin_map(urlparse(url).netloc)
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
                        restatus_from_title(c)     # status was decided on the card title
                    for f in ("asking_price", "cash_flow", "revenue", "state"):
                        if not c.get(f) and prev.get(f):
                            c[f] = prev[f]
                    if fin_map:
                        for f in ("asking_price", "cash_flow", "revenue"):
                            if prev.get(f):
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
                    restatus_from_title(listing)   # the detail <h1> may carry "(SOLD)"
                    if fin_map:
                        found = _apply_fin_map(fin_map, detail_html)
                        for f, v in found.items():
                            listing[f] = v
                        if found.get("asking_price"):
                            listing["_fin_from_map"] = True
                    listing["url"] = detail_url
                    enriched += 1
                    self.stats["detail_pages_fetched"] += 1
                    if not had_state and listing.get("state"):
                        states_gained += 1
                except Exception:
                    listing.pop("_detail_url", None)

            # Mapped domain: a card whose detail page wasn't read this run keeps
            # its stored financials rather than the card's largest-$ guess.
            if fin_map:
                for c in with_detail:
                    prev = stored.get(c["id"])
                    if prev and not c.get("_fin_from_map"):
                        for f in ("asking_price", "cash_flow", "revenue"):
                            if prev.get(f):
                                c[f] = prev[f]

            # Strip remaining internal fields
            for l in unique_cards:
                l.pop("_detail_url", None)
                l.pop("_fin_from_map", None)
                l.pop("_status_from_field", None)

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

            # SITE-PAGE GATE (2026-10-05): drop cards that are the broker's own
            # site pages (services, valuation, contact, blog articles, social
            # links), re-checked here on the FINAL url/title after detail
            # enrichment; then, if this broker has a listing URL pattern
            # (/listing/<slug> ...), drop single-segment pages that don't fit it.
            # ~900 such rows across 323 domains were quarantined by hand on
            # 2026-10-05 (quarantine_log) — this keeps them from coming back.
            before = len(unique_cards)
            unique_cards = [l for l in unique_cards
                            if not is_site_page(l.get("title"), l.get("url") or "")]
            unique_cards, off_pattern = listing_url_pattern_filter(unique_cards, url)
            site_dropped = before - len(unique_cards)
            if site_dropped:
                print(f"   🧹 site-page gate: dropped {site_dropped} non-listing page(s)"
                      + (f" ({len(off_pattern)} off the listing URL pattern)" if off_pattern else ""))
                self.stats["site_pages_dropped"] = self.stats.get("site_pages_dropped", 0) + site_dropped
            if not unique_cards:
                print("   🚫 nothing left after site-page gate — not writing")
                return []

            # SAME-LISTING DEDUPE (2026-10-05): a nested container match
            # (div.row inside div.row) yields the same listing 2-3 times under
            # one id; the outer copy can pick up the page's price-filter values
            # (intemedior: $20M "Price Unlimited" slider) as its price. Keep one
            # card per id, preferring the price most copies agree on.
            by_id: dict[str, list[dict]] = {}
            for l in unique_cards:
                by_id.setdefault(l["id"], []).append(l)
            if len(by_id) < len(unique_cards):
                deduped = []
                for group in by_id.values():
                    prices = [g.get("asking_price") for g in group if g.get("asking_price")]
                    best = group[0]
                    if prices:
                        mode = max(set(prices), key=prices.count)
                        best = next(g for g in group if g.get("asking_price") == mode)
                    deduped.append(best)
                print(f"   🧹 dedupe: {len(unique_cards)} cards -> {len(deduped)} listings")
                unique_cards = deduped

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

            # SYNTHETIC URLS (2026-11-11): cards with no detail link get
            # <index>#<slug>. Done last so every gate above saw the plain
            # index URL as before; the id is not changed here.
            syn = assign_synthetic_urls(unique_cards, url)
            if syn["synthetic"]:
                print(f"   #️⃣  synthetic URLs: {syn['synthetic']} "
                      f"(slug collisions left {syn['unresolved_collisions']})")

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
        ev = s.get("pattern_cache_events") or []
        if ev:
            stale = [e for e in ev if e["event"] == "stale_cache"]
            dead = [e["domain"] for e in stale if not e.get("healed_to")]
            print(f"  ♻️  stale cache entries: {len(stale)} "
                  f"({len(stale) - len(dead)} healed by re-detect"
                  + (f"; still dead: {', '.join(dead[:10])}" if dead else "") + ")"
                  + f"; www/bare twin entries used: "
                  f"{sum(1 for e in ev if e['event'] == 'twin_spelling')}")
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
    parser.add_argument("--pagination",  metavar="MODE[:PARAM]",
                        help="--broker test only: force pagination, e.g. "
                             "param0:page, param:paged, path, offset:start, none")
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

    # A one-broker run must never overwrite the daily crawl's snapshot files
    # (data/snapshots/<date>/ is committed by CI; a 1-broker test replaced three
    # 800-broker snapshots on 2026-11-09).
    output_dir = args.output
    if args.broker and output_dir == "data/snapshots":
        output_dir = "data/snapshots_test"

    scraper = DealLedgerScraper(
        output_dir=output_dir,
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
            if args.pagination:
                mode, _, param = args.pagination.partition(":")
                brokers[0]["pagination_mode"] = mode
                if param:
                    brokers[0]["pagination_param"] = param
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
                      "domain": b["domain"],
                      "pagination_mode": b.get("pagination_mode", ""),
                      "pagination_param": b.get("pagination_param", "")}
                     for b in brokers],
                    columns=["broker_name", "listing_url", "domain",
                             "pagination_mode", "pagination_param"],
                ).to_csv(args.plan_out, index=False)
                print(f"🗺️  plan: wrote {len(brokers)} ordered brokers to {args.plan_out}")
                return

        scraper.run(brokers)
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    main()
