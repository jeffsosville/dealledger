#!/usr/bin/env python3
"""
junk_filter.py — single source of truth for "is this a junk listing?"

Python port of the DB is_listing_junk(title, url) filter. Kept dependency-free
(stdlib only) so BOTH the scraper's extraction path (dealledger_scraper_v6.py)
and the regression suite (regression_check.py) import the SAME logic — a title
that would be extracted is exactly a title the regression suite considers clean.
"""

import re

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
    # 4 financial fragments (grabbed a price/metric, not a name)
    if re.match(r'^(net|gross|asking price|revenue|cash flow|sde|ebitda|price)\b', tl):
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
