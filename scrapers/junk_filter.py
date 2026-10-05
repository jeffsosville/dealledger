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
    # Bodner/execbb category column: "General Services-Laundromat",
    # "Food/Liquor-Liquor Lic. C", "General Retail-Convenience". The hyphen must
    # sit tight between letters so a real title ("General Store - Profitable")
    # is spared.
    if re.match(r'^(?:general|food|retail)[ /][a-z ]*[a-z]-[a-z]', tl):
        return True
    # The broker's own firm name used as a listing title (lbaweb et al).
    if firm_name and tl == firm_name.strip().lower():
        return True
    return False


# A last path segment that is a script filename or a generic route is NOT a
# title. Without these guards execbb.com/Buyer/sub/listingdetail.asp?listingid=X
# yielded "Listingdetail" for 370 listings — worse than the original.
_SCRIPT_EXT_RE = re.compile(r'\.(?:asp|aspx|php|html?|jsp|cgi|cfm)$', re.IGNORECASE)
_RESERVED_SEGMENTS = {
    "index", "detail", "details", "listingdetail", "results", "result",
    "default", "search", "listing", "listings", "page", "view", "item",
    "property", "properties", "business", "businesses", "home",
}


def title_from_slug(url):
    """Derive a title from a listing URL's last path segment.
    /businesses-for-sale/SD00077/Women%27s-Fashion-Boutique-North-County
        -> "Women's Fashion Boutique North County"

    Returns None rather than inventing a title when the segment is a script
    filename (listingdetail.asp), a generic route (index/detail/search),
    numeric, or too short to be a real slug. Callers do
    `title_from_slug(url) or title`, so None correctly keeps the original."""
    if not url:
        return None
    try:
        path = urlparse(url).path.rstrip("/")
    except Exception:
        return None
    seg = unquote(path.split("/")[-1] if path else "")
    if not seg:
        return None
    if _SCRIPT_EXT_RE.search(seg):                 # listingdetail.asp -> not a title
        return None
    seg = re.sub(r"\.\w+$", "", seg)               # strip any other extension
    if seg.strip().lower() in _RESERVED_SEGMENTS:
        return None
    if seg.replace("-", "").replace("_", "").isdigit():
        return None
    # A real slug is hyphen/underscore separated; a short bare word isn't a title.
    if "-" not in seg and "_" not in seg and len(seg) < 10:
        return None
    slug = re.sub(r"[-_+]+", " ", seg)
    slug = re.sub(r"\s+", " ", slug).strip()
    # Trailing listing number (Sunbelt: ...-revenue-57447) is not part of the
    # business name.
    slug = re.sub(r"\s+\d{4,6}$", "", slug).strip()
    if len(slug) < 6 or slug.replace(" ", "").isdigit():
        return None
    if slug.lower() in _RESERVED_SEGMENTS:
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

# ── Dead-listing detection ────────────────────────────────────────────────────
# Three verified signals. Note status codes LIE — Transworld and Sunbelt both
# serve 200 on dead listings, so the <title> is the only definitive check.
_DEAD_URL_RE = re.compile(
    r'\?post_type=[^&]*&p=\d+'      # VR: WordPress raw fallback; the post is gone
    r'|/deleted-business/'          # Sunbelt: dead listings redirect here
    r'|/listing-not-found',
    re.IGNORECASE)
_DEAD_TITLE_RE = re.compile(
    r'page not found|404 not found|listing no longer available|'
    r'listing not found|no longer available',
    re.IGNORECASE)


def is_dead_listing(url, html=None):
    """True if a listing URL/page is a dead link.

    Signals (all verified in the wild):
      - VR          : ?post_type=listing&p=<id>  (WP raw fallback = post deleted)
      - Sunbelt     : redirects to /omaha-ne/deleted-business/ ("Listing No
                      Longer Available")
      - Transworld  : client-rendered 404 — HTTP 200, so only the <title> tells

    `html` is optional: the URL check alone catches VR/Sunbelt without a fetch.
    """
    if url and _DEAD_URL_RE.search(url):
        return True
    if html:
        m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
        if m and _DEAD_TITLE_RE.search(m.group(1)):
            return True
        if re.search(r'listing no longer available', html, re.IGNORECASE):
            return True
    return False


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
    # 8 bad destination URLs (incl. known dead-listing URL markers, e.g. VR's
    # ?post_type=listing&p=<id> WordPress fallback — the post no longer exists)
    if u and any(s in u for s in _JUNK_URL_SUBSTR):
        return True
    if u and _DEAD_URL_RE.search(u):
        return True
    # 9 CRE / land (not a business)
    if re.search(r'(commercial real estate for lease|residential land|land for sale|'
                 r'for lease in)', tl) and 'business' not in tl:
        return True
    return False


# ── Site-page gate (2026-10-05) ───────────────────────────────────────────────
# The generic crawler was saving a broker's OWN site pages as listings: the
# services page, "schedule a call", valuation landing pages, LinkedIn links,
# Cloudflare email-protection links, blog posts syndicated across broker
# franchises ("Recognizing the Warning Signs in Your Business"), and filter-
# widget fragments ("$80,000,000 $1,030,000 $1,520,000"). ~900 rows across 323
# domains were quarantined on 2026-10-05 (quarantine_log). These are the
# signatures, checked on the destination URL and the title.
_SOCIAL_HOST_RE = re.compile(
    r'(^|\.)(linkedin|facebook|twitter|x|instagram|youtube|tiktok|pinterest)\.com$')
# Last path segment that is a site page, never a listing.
_SITE_PAGE_SLUG_RE = re.compile(
    r'^(services?|business-services|about(-us)?|about-\d+|contact(-us|-form)?|team|our-team|'
    r'faqs?|testimonials|schedule-a-call|book-a-call|buyer-registration|'
    r'seller-registration|sell-your-business|sell-a-business|sell-my-business|'
    r'business-valuations?|valuations?|'
    r'business-valuation-service|mergers-acquisitions|business-brokerage|'
    r'franchise-sales|franchise-resales|franchise-consulting|commercial-real-estate|'
    r'business-broker-[a-z-]+|business-brokers-in-[a-z-]+|privacy-policy|careers|'
    r'resources|blog|news|press|insights|accessibility-statement|sitemap|nda|'
    r'how-we-sell|how-it-works|new-listing|buyside-broker-services)$')
# Path sections that hold articles / taxonomy pages, not listings. Deliberately
# NOT here: /blog/ (resolutionep.com publishes listings as Squarespace blog
# posts) and plain /category/ (azbusinessbrokers.com cards resolve to
# /search-listings/category/<X> while carrying real listing titles).
# Likewise "buy-a-business" is not a site-page slug: capitalbusinessadvisor.com
# renders its listings inline on /buy-a-business/, so the cards carry that URL.
_CONTENT_SECTION_RE = re.compile(
    r'/(insights|news|articles|industries-served|business-category)/')
# Boilerplate article / marketing titles.
_SITE_PAGE_TITLE_RE = re.compile(
    r'(©|\bcopyright\b|all rights reserved|^businesses for sale with|'
    r'^search (businesses|listings)|^business financing|^lender pre-qualified businesses|'
    r'^contact\b|^sell my business|what buyers really want|'
    r'does your asking price|what makes a business attractive|'
    r'^recognizing the warning signs|^the differences between|'
    r'free 15-minute call|^looking for a\s?\w|^do you want to sell|'
    r'^book a confidential|^useful links$|^quick links$|transaction terms$|'
    r'^why use (a|our)\b|^i want to (buy|sell)\b|^how it works$|'
    r'back to listings|^greater than or equal$|^lower than or equal$|'
    r'^\$[\d,]+ \$[\d,]+)', re.IGNORECASE)


def is_site_page(title, url=""):
    """True if (title, url) is one of the broker's own site pages (services,
    contact, valuation, blog article, social profile, homepage) rather than a
    business-for-sale listing."""
    t = (title or "").strip()
    if t and _SITE_PAGE_TITLE_RE.search(t):
        return True
    if not url:
        return False
    try:
        pu = urlparse(url.strip())
    except Exception:
        return False
    host = (pu.netloc or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    if _SOCIAL_HOST_RE.search(host):
        return True
    path = pu.path or ""
    if "/cdn-cgi/" in path:
        return True
    segs = [s for s in path.split("/") if s]
    if not segs and not pu.query:                    # the bare homepage
        return True
    if segs and _SITE_PAGE_SLUG_RE.match(segs[-1].lower()):
        return True
    if _CONTENT_SECTION_RE.search(path.lower() + "/"):
        return True
    return False


# A detail URL that LOOKS like a listing: the section names brokers use for
# per-listing pages, followed by a slug or id.
_LISTING_URL_RE = re.compile(
    r'/(listings?|business-listings?|businesses?-for-sale|business-for-sale|'
    r'for-sale|property|properties|detail|details|business|businesses|biz|'
    r'opportunit(y|ies)|offerings?|active_sellers|current-listings|'
    r'available-businesses|portfolio-items|deals?|engagements?|'
    r'current-engagements|restaurant-for-sale|atm-route-for-sale|'
    r'business-opportunity|business_listing|directory)/[^/?#]+', re.IGNORECASE)


# A single path segment that is itself a listings index page — inline cards
# legitimately carry it as their URL.
_INDEX_SEGMENT_RE = re.compile(
    r'(listing|for-sale|businesses|find-a-business|buy-a-business|'
    r'index\.(php|html?|aspx?)|search|opportunit|offering|inventory|portfolio)',
    re.IGNORECASE)


def listing_url_pattern_filter(cards, listings_url=""):
    """Set-level gate for one broker. If at least two cards carry a
    listing-shaped detail URL (/listing/<slug>, /business-for-sale/<id>, ...),
    the broker HAS a listing URL pattern — so any other card that points to a
    single-segment page on the same site (/what-buyers-really-want/,
    /our-team/) is a site page that leaked in, and is dropped.

    Cards with no per-listing URL (url == the listings page, or
    url_is_listing_specific False) are kept: some brokers render every
    listing inline on one page. Returns (kept, dropped)."""
    def _url(c):
        return (c.get("url") or c.get("_detail_url") or "").strip()
    lp = sum(1 for c in cards if _LISTING_URL_RE.search(_url(c)))
    if lp < 2:
        return list(cards), []
    base = (listings_url or "").rstrip("/").lower()
    # A URL carried by 2+ cards is the page the listings render inline on
    # (quietlight.com/listings, companysellers.com/businesses-for-sale) — keep.
    seen = {}
    for c in cards:
        k = _url(c).rstrip("/").lower()
        seen[k] = seen.get(k, 0) + 1
    kept, dropped = [], []
    for c in cards:
        u = _url(c)
        if (not u or u.rstrip("/").lower() == base or _LISTING_URL_RE.search(u)
                or seen.get(u.rstrip("/").lower(), 0) > 1):
            kept.append(c)
            continue
        try:
            pu = urlparse(u)
            segs = [s for s in (pu.path or "").split("/") if s]
        except Exception:
            kept.append(c)
            continue
        if len(segs) == 1 and not pu.query and not _INDEX_SEGMENT_RE.search(segs[0]):
            dropped.append(c)
        else:
            kept.append(c)
    return kept, dropped


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
