#!/usr/bin/env python3
"""
junk_filter.py — single source of truth for "is this a junk listing?"

Python port of the DB is_listing_junk(title, url) filter. Kept dependency-free
(stdlib only) so BOTH the scraper's extraction path (dealledger_scraper_v6.py)
and the regression suite (regression_check.py) import the SAME logic — a title
that would be extracted is exactly a title the regression suite considers clean.
"""

import re
from urllib.parse import unquote, urlparse

# ── Title extraction guard ────────────────────────────────────────────────────
# One rule, shared by every scraper: if an extracted title is junk/blank, reject
# it and fall through to the next strategy; if all fail, derive from the URL
# slug. ~24% of listings were capturing a CTA/status/category string instead of
# the business name (Sunbelt's register modal, Link's blank, companysellers'
# "MORE DETAILS", "Pending", "Sold", "Request Quote", …).
JUNK_TITLES = {
    "more details", "add to favorites", "view details", "request quote",
    "asking price", "for sale", "sold", "pending", "under contract",
    "learn more", "read more", "view listing", "see details", "click here",
    "contact us", "get started", "details", "view", "more info",
    "view more", "inquire", "request info", "save", "share", "favorites",
}
JUNK_PREFIXES = ("you need to register", "sign up to", "log in to",
                 "create an account", "please register", "register to")


def is_junk_title(t, firm_name=None):
    """True if `t` is a CTA / status / category string rather than a business
    name — i.e. reject it and fall through to the next title strategy."""
    if not t or len(t.strip()) < 6:
        return True
    tl = t.strip().lower()
    if tl in JUNK_TITLES:
        return True
    if any(tl.startswith(p) for p in JUNK_PREFIXES):
        return True
    # Category slug, e.g. "Food/Liquor-Liquor Store". Requires the '/' so a real
    # hyphenated title ("Well-Established Cafe") is NOT rejected.
    if "/" in tl and len(tl) < 40 and re.match(r'^[a-z/ ]+-[a-z/ ]+$', tl):
        return True
    # The broker's own firm name used as a listing title (lbaweb et al).
    if firm_name and tl == firm_name.strip().lower():
        return True
    return False


def title_from_slug(url):
    """Derive a title from a listing URL's last path segment.
    /businesses-for-sale/SD00077/Women%27s-Fashion-Boutique-North-County
        -> "Women's Fashion Boutique North County"
    Returns None when the segment is empty, numeric, or too short."""
    if not url:
        return None
    try:
        path = urlparse(url).path.rstrip("/")
    except Exception:
        return None
    seg = path.split("/")[-1] if path else ""
    seg = unquote(seg)
    seg = re.sub(r"\.\w+$", "", seg)               # strip .html/.php
    slug = re.sub(r"[-_+]+", " ", seg)
    slug = re.sub(r"\s+", " ", slug).strip()
    if len(slug) < 6 or slug.replace(" ", "").isdigit():
        return None
    # Capitalize the first letter only — .title() would mangle apostrophes
    # ("Women's" -> "Women'S"). Keep existing all-caps tokens (HVAC, LLC).
    return " ".join(w if w.isupper() else (w[:1].upper() + w[1:])
                    for w in slug.split())[:500]


# Link/heading text that is a call-to-action / nav furniture, not a title.
_JUNK_TITLE_EXACT = {
    # nav / UI furniture
    "more details", "add to favorites", "business listings", "newsletter sign up",
    "create account", "recent posts", "medical spa", "real estate", "environmental svcs",
    "view all", "load more", "sign in", "log in", "learn more", "read more", "click here",
    "view listing", "view details", "see details", "favorites", "next", "previous",
    "our listings", "all listings", "featured listings", "search", "filter", "home",
    "contact us", "about us", "get started", "subscribe", "menu",
    # status words / error & challenge pages
    "sold", "pending", "under contract", "coming soon", "new listing", "new", "featured",
    "checking your browser", "403 - forbidden", "403 forbidden", "404", "not found",
    "access denied", "page not found", "error",
}
_JUNK_URL_SUBSTR = ("javascript:", "mailto:", "maps.app.goo.gl", "/privacy",
                    "/terms", "/author/", "addtofavorites", "/newsletter")


def is_listing_junk(title, url=""):
    """True if (title, url) is scraper junk, not a real business listing."""
    t = (title or "").strip()
    tl = t.lower()
    u = (url or "").lower()
    if len(t) < 6:                                             # 1 empty/short
        return True
    if tl in _JUNK_TITLE_EXACT:                                # 2/3 nav + status
        return True
    if tl.startswith("checking your browser") or re.match(r'403.*forbidden', tl):
        return True
    # 4 financial fragments (grabbed a price/metric, not a name). Match a
    # label followed by ':' or '$' ("Price: $500k"), or a SHORT bare label —
    # but NOT a real title that merely opens with the word
    # ("Price Reduced! Restaurant for Sale in Scottsdale").
    _fin = r'(net|gross|asking price|revenue|cash flow|sde|ebitda|price)'
    if re.match(r'^' + _fin + r'\s*[:$]', tl):
        return True
    if re.match(r'^' + _fin + r'\b', tl) and len(t) < 25:
        return True
    if re.search(r'gross (revenue|sales)', tl) or re.search(r'net op(erating)? inc', tl):
        return True
    if re.match(r'^\$?[\d,]+(\.\d+)?\s*$', t):                 # title is just a number
        return True
    if re.match(r'^\$[\d,]+.{0,4}(for sale|gross|revenue)', tl):
        return True
    # 5 CTA / marketing copy
    if (tl.startswith("join the") or tl.startswith("sell your business")
            or "exit planning circle" in tl
            or tl.startswith("it's the easiest thing")
            or "the easiest thing in the world" in tl):
        return True
    # 6 broker firm name as title (short + firm keyword)
    if len(t) < 45 and re.search(
            r'(business advisors|business brokers|commercial real estate brokerage|'
            r'the business selling experts)$', tl):
        return True
    # 7 privacy / terms / legal
    if (tl.startswith("privacy policy") or tl.startswith("terms ")
            or "terms of service" in tl or tl == "google maps"):
        return True
    # 8 bad destination URLs
    if u and any(s in u for s in _JUNK_URL_SUBSTR):
        return True
    # 9 CRE / land (not a business)
    if re.search(r'(commercial real estate for lease|residential land|land for sale|'
                 r'for lease in)', tl) and 'business' not in tl:
        return True
    return False


# Back-compat alias — dealledger_scraper_v6 historically called this is_junk_listing.
is_junk_listing = is_listing_junk


# Sold / under-contract STATUS markers in a title. Kept in sync with the SQL
# is_sold_or_pending(t). Used by the scrapers to set status='sold' (NOT to drop
# the listing — a sold deal is a real listing, just not active). Catches badge-
# style markers (all-caps SOLD, UNDER CONTRACT, "(SOLD & SETTLED)", "just
# closed", "recently sold") while sparing descriptive uses ("Sold With Real
# Estate", "Sold As-Is").
_SOLD_SPARE = re.compile(r'\bsold\s+(?:with|as[\s-]?is)\b', re.IGNORECASE)
_SOLD_MARKERS = (
    r'\bSOLD\b',                        # all-caps badge
    r'\bunder[\s-]*contract\b',
    r'\bsale[\s-]*pending\b',
    r'\bjust[\s-]*closed\b',
    r'\brecently[\s-]*sold\b',
    r'\(\s*sold[^)]*\)',               # (SOLD), (SOLD & SETTLED)
)


# Title-suffix status marker: "Company Name – Sold", "Diner: Under Contract"
# (synergybb has no status taxonomy — status lives in the title suffix).
_SOLD_SUFFIX = re.compile(
    r'[-–—:|]\s*(?:sold|under[\s-]*contract|sale[\s-]*pending|pending)\s*$', re.IGNORECASE)


def is_sold_or_pending(title):
    """True if the title marks the deal SOLD / UNDER CONTRACT (a status signal,
    not a reason to discard the listing)."""
    t = (title or "").strip()
    if not t or _SOLD_SPARE.search(t):
        return False
    if _SOLD_SUFFIX.search(t):                   # "… – Sold" title suffix
        return True
    if re.search(_SOLD_MARKERS[0], t):           # SOLD must be ALL-CAPS mid-title
        return True
    return any(re.search(p, t, re.IGNORECASE) for p in _SOLD_MARKERS[1:])
