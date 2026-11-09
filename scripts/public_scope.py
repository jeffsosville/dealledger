"""public_scope.py — what may appear in DealLedger's public files.

Shared by scripts/export_public_listings.py (which applies the rules) and
scripts/verify_public_export.py (which checks the written files against them
before anything is committed). Keep the two in step by importing from here.
"""
import hashlib
import re
from urllib.parse import urlsplit

# Hosts that are never a public listing source, whatever broker row points at
# them: marketplaces and aggregators (broker-direct only), digital/ecommerce
# platforms (out of scope since 2026-11-08), real-estate portals, and broker
# MLS / association feeds (discovery seeds only). Matched on the URL's host
# (exact or subdomain), so a broker whose own domain merely contains one of
# these words is not caught.
DENY_HOSTS = {
    # marketplaces / aggregators
    "bizbuysell.com", "bizquest.com", "businessesforsale.com", "dealstream.com",
    "businessbroker.net", "bizben.com", "businessmart.com", "globalbx.com",
    "interexo.com", "loopnet.com", "crexi.com", "buildout.com",
    # digital / ecommerce (out of scope)
    "flippa.com", "empireflippers.com", "quietlight.com", "websiteclosers.com",
    "acquire.com", "feinternational.com", "websiteproperties.com",
    # real estate portals
    "realtor.com", "zillow.com", "redfin.com", "trulia.com",
    # broker MLS / associations (seeds, never a source)
    "bizmls.com", "commercialmls.com", "bbf.org",
}

# The four public listing-age levels, as listing_age_v2 / mv_listings_page_v2
# name them (sql/2026-11-07_listing_age_v2.sql). Level 1 (broker-stated date)
# and level 3 (archive capture) are not published yet.
AGE_BASES = ("window", "observed", "in_db")
WINDOW_MAX_DAYS = 45
RECORD_START = "2026-02-18"   # earliest dated sighting in listing_sighting

# Columns that must never appear in a public file: broker-written text
# (copyright, kept out since 2026-11-08), contact details, marketplace fields,
# and any days-on-market figure (DOM hold).
FORBIDDEN_COLUMNS = {
    "description", "contact_name", "contact_phone", "profile_views",
    "bbs_listing_id", "days_on_market", "dom", "dom_days", "first_seen_seed",
    "quality_score", "quality_tier", "match_confidence",
}

# Words DealLedger never asserts outside broker-quoted fields.
SOLD_WORDS = re.compile(r"\b(sold|time to sell|closed|sell-through)\b", re.I)


def host_of(url):
    try:
        h = (urlsplit(url if "://" in url else "https://" + url).hostname or "").lower()
    except ValueError:
        return ""
    return h[4:] if h.startswith("www.") else h


def bare_domain(domain):
    d = (domain or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


def host_denied(host, blocked=()):
    """True if host is (a subdomain of) a denied host or a blocked broker domain."""
    if not host:
        return True
    for d in list(DENY_HOSTS) + [bare_domain(b) for b in blocked if b]:
        if host == d or host.endswith("." + d):
            return True
    return False


def listing_number(row_id):
    """The public listing number for a listings_direct row: the same formula the
    database uses for /listing/{n} pages and listing_age_v2
    (900000000 + first 28 bits of md5(row id))."""
    return 900000000 + int(hashlib.md5(str(row_id).encode()).hexdigest()[:7], 16)
