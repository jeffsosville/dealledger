#!/usr/bin/env python3
"""
run_specialized.py

Runs all 8 specialized broker scrapers and upserts results
to DealLedger Supabase → listings_direct table.

CHANGES (2026-04-27):
- Now writes to listings_direct (the unified broker-direct table)
  instead of the legacy listings_broker table.
- Broker names are resolved from account IDs at write time, so rows
  land with "Transworld Business Advisors" instead of "28148".
- Adds url_is_listing_specific flag to mark broker-index URLs that
  need re-scraping later (some sites lack per-listing detail pages).

Usage:
    python3 scrapers/run_specialized.py
    python3 scrapers/run_specialized.py --brokers transworld,sunbelt,fcbb
    python3 scrapers/run_specialized.py --dry-run
"""

import os, sys, re, json, time, hashlib, argparse, logging
from datetime import datetime, timezone
import re
from urllib.parse import urlparse
import requests as http_requests

# Add parent dir to path so we can import specialized_scrapers
sys.path.insert(0, os.path.dirname(__file__))
import csv as _csv
from crawl_run_log import (start_run, finish_run, expire_stale_runs,
                           record_domain_runs, domain_counts_excluding)
from displayed_count import parse_stated_count, verdict_label
from junk_filter import is_junk_title, title_from_slug
from specialized_scrapers import (
    MurphyScraper, HedgestoneScraper, TransworldScraper,
    SunbeltScraper, VRScraper, FCBBScraper,
    LinkBusinessScraper, LarryBodnerScraper,
    WeSellRestaurantsScraper, VestedScraper, RoutesForSaleScraper,
    WPRestScraper, FacetWPScraper
)
from ere_property_scraper import EREPropertyScraper

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

def _domain_urls(domain: str) -> tuple[set, set]:
    """(all known URLs, URLs that already have revenue) for one domain.
    Empty sets when there is no service key (dry runs)."""
    if not SUPABASE_SERVICE_KEY:
        return set(), set()
    h = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}
    known, have = set(), set()
    off = 0
    while True:
        r = http_requests.get(f"{SUPABASE_URL}/rest/v1/listings_direct", headers=h, timeout=60,
                              params={"select": "url,revenue", "broker_domain": f"eq.{domain}",
                                      "order": "id", "offset": str(off), "limit": "1000"})
        r.raise_for_status()
        rows = r.json()
        for row in rows:
            known.add(row["url"])
            if row.get("revenue"):
                have.add(row["url"])
        if len(rows) < 1000:
            return known, have
        off += 1000


def _transworld():
    s = TransworldScraper()
    listings = s.scrape("28148", max_pages=450, workers=8)
    try:
        known, have = _domain_urls("tworld.com")
        s.add_revenue(listings, have, known,
                      max_pages=int(os.environ.get("TWORLD_DETAIL_MAX", "400")))
    except Exception as e:   # revenue is extra; never fail the crawl over it
        log.warning(f"[transworld] revenue pass skipped: {e}")
    return listings


# ── Broker registry ───────────────────────────────────────────────────────────
# Each entry has:
#   account      → numeric account ID (matches broker_master / past data)
#   display_name → real broker name written to listings_direct.broker_name
#   fn           → callable that runs the scraper and returns List[Dict]
#   domain       → bare domain for crawl_run.broker_domain. Needed up front:
#                  a crawl that returns nothing (or raises) has no listing URL
#                  to derive it from, and those were exactly the rows left
#                  'running' with broker_domain NULL (96 of them by 09-24).
BROKERS = {
    "transworld": {
        "domain": "tworld.com",
        "account": "28148",
        "display_name": "Transworld Business Advisors",
        "fn": lambda: _transworld(),
    },
    "sunbelt": {
        "domain": "sunbeltnetwork.com",
        "account": "1001",
        "display_name": "Sunbelt Business Brokers",
        "fn": lambda: SunbeltScraper().scrape("1001", max_pages=130),
    },
    "fcbb": {
        "domain": "fcbb.com",
        "account": "1002",
        "display_name": "First Choice Business Brokers (FCBB)",
        "fn": lambda: FCBBScraper().scrape("1002", max_pages=79),
    },
    "murphy": {
        "domain": "murphybusiness.com",
        "account": "1003",
        "display_name": "Murphy Business",
        "fn": lambda: MurphyScraper.scrape("1003", max_pages=50),
    },
    "vr": {
        "domain": "vrbusinessbrokers.com",
        "account": "1004",
        "display_name": "VR Business Brokers",
        "fn": lambda: VRScraper().scrape("1004", max_pages=40),
    },
    "hedgestone": {
        "domain": "hedgestone.com",
        "account": "28149",
        "display_name": "Hedgestone Business Advisors",
        "fn": lambda: HedgestoneScraper().scrape("28149", max_pages=40),
    },
    "link": {
        "domain": "linkbusiness.com",
        "account": "1005",
        "display_name": "Link Business",
        "fn": lambda: LinkBusinessScraper().scrape("1005", max_pages=60),
    },
    "bodner": {
        "domain": "bodnergroup.com",
        "account": "1006",
        "display_name": "Executive Business Brokers (Larry Bodner)",
        "fn": lambda: LarryBodnerScraper().scrape("1006"),
    },
    "wesell": {
        "domain": "wesellrestaurants.com",
        "account": "2900",
        "display_name": "We Sell Restaurants",
        "fn": lambda: WeSellRestaurantsScraper().scrape("2900", max_pages=40),
    },
    "vested": {
        "domain": "vestedbb.com",
        "account": "1593",
        "display_name": "Vested Business Brokers",
        "fn": lambda: VestedScraper().scrape("1593", max_pages=250),
    },
    "routesforsale": {
        "domain": "routesforsale.net",
        "account": "13461",
        "display_name": "Routes For Sale",
        "fn": lambda: RoutesForSaleScraper().scrape("13461"),
    },
    # Eatz moved off WP-REST (Oct 6): its REST feed is the whole archive, ~93%
    # sold, with no status field. The FacetWP for-sale view is the real inventory.
    "eatz": {
        "domain": "eatz-associates.com",
        "account": "3477",
        "display_name": "Eatz & Associates",
        "fn": lambda: FacetWPScraper("eatz-associates.com", "listings",
                                     # under-contract is still listed (Oct 12: 11 such listings had
                                     # been marked sold because this list left them out).
                                     {"status": ["for-sale", "new-listing", "under-contract"]},
                                     link_match="/listings/").scrape("3477"),
    },
    # Reputed Brokerage (2026-11-11): G5 Beyot theme + Essential Real Estate
    # plugin. The `property` post type is not exposed in WP REST, so this reads
    # the server-filtered property-status archives: for-sale -> active,
    # not-available -> inactive, for-lease -> not emitted; a for-sale card the
    # broker labels Sold/Expired -> sold, Lands/Apartment types not emitted
    # (owner decision 7, 2026-11-12). Replaces the V6
    # generic crawl, which also wrote the search form's Min/Max Price dropdowns
    # as listings (see SPECIALIZED_DOMAINS in dealledger_scraper_v6.py).
    "reputedbrokerage": {
        "domain": "reputedbrokerage.com",
        "account": "reputedbrokerage",
        "display_name": "Reputed Brokerage",
        "fn": lambda: EREPropertyScraper("www.reputedbrokerage.com").scrape("reputedbrokerage"),
    },
}


def _register_wp_rest_brokers():
    """Register the WordPress-REST brokers from data/wp_rest_brokers.csv —
    one generic WPRestScraper, ~54 brokers as (domain, rest_base, account)
    rows. Keeps the big registry version-controlled and out of the code."""
    path = os.path.join(os.path.dirname(__file__), "..", "data", "wp_rest_brokers.csv")
    if not os.path.exists(path):
        return
    with open(path) as f:
        for row in _csv.DictReader(f):
            domain = (row.get("domain") or "").strip()
            rest = (row.get("rest_base") or "").strip()
            if not domain or not rest:
                continue
            acct = (row.get("account") or "").strip()
            slug = re.sub(r"[^a-z0-9]+", "_", domain.replace("www.", "").lower()).strip("_")
            key = "wp_" + slug
            account = acct or key
            display = (row.get("display_name") or "").strip() or domain
            # Live "for sale" page. With it, REST posts that are not on the
            # page go out as status='inactive' instead of active (a WP post
            # type keeps every listing ever published — vikingmergers: 411
            # posts, 62 live). Blank = REST status fields only.
            index_url = (row.get("index_url") or "").strip() or None
            # 'drop' only for a broker whose off-index rows are handled by hand
            # (truforte: quarantined); blank = 'inactive'.
            off_index = (row.get("off_index") or "").strip() or "inactive"
            # yes = follow each active post's detail page for price / revenue /
            # cash flow (synergybb: REST and its FacetWP grid carry none).
            fetch_fin = (row.get("fetch_financials") or "").strip().lower() in ("yes", "true", "1")
            BROKERS[key] = {
                "domain": domain.replace("www.", "").lower(),
                "account": account,
                "display_name": display,
                "wp_rest": True,
                "fn": (lambda d=domain, rb=rest, a=account, iu=index_url, oi=off_index, ff=fetch_fin:
                       WPRestScraper(d, rb, fetch_financials=ff, index_url=iu,
                                     off_index=oi).scrape(a, max_pages=60)),
            }


_register_wp_rest_brokers()

# Build reverse lookup: account_id → display_name
ACCOUNT_TO_BROKER_NAME = {b["account"]: b["display_name"] for b in BROKERS.values()}
# Defensive: handle float-as-string from CSV imports (e.g. "28148.0")
ACCOUNT_TO_BROKER_NAME.update({
    f"{int(acct)}.0": name
    for acct, name in list(ACCOUNT_TO_BROKER_NAME.items())
    if acct.isdigit()
})


# ── Helpers ───────────────────────────────────────────────────────────────────
# Pagination markers. These appear ANYWHERE in the path or query, not only at
# the end, which is how they slipped past the original suffix-only check.
#
# Aug 2026: aria.net produced 343 rows for 3 real listings because the crawler
# walked /listings/page/2/, /page/3/ ... and each one carried the same detail
# slug appended. Every row came back flagged as a specific listing, so the
# vertical views trusted them and one amusement park rendered 42 times on
# VendingExits.
_PAGINATION_PATTERNS = (
    re.compile(r"/(?:page|pg|p)/\d+", re.I),      # /page/10/  /pg/3  /p/7
    re.compile(r"[?&](?:page|pg|start|offset)=\d+", re.I),
    re.compile(r"/\d+/\d+/?$"),                    # trailing /2/10
)

# Index path segments. Checked mid-path as well as at the end - a detail slug
# appended to an index path does not make it a detail page.
_INDEX_SEGMENTS = (
    "listings", "businesses-for-sale", "business-for-sale",
    "business-listings", "business-directory", "restaurants-for-sale",
    "routes-for-sale", "search", "results", "browse", "archive",
)


def detect_index_page_url(url: str) -> bool:
    """
    Return False if the URL looks like a broker's listings INDEX or a paginated
    page rather than an individual listing detail page.

    True means "safe to treat as a listing". The vertical marketplace views
    filter on this, so a false positive here puts junk on a live site.
    """
    if not url:
        return False

    lower = url.lower()
    stripped = lower.rstrip("/")

    junk_schemes = ("javascript:", "mailto:", "tel:")
    if any(scheme in lower for scheme in junk_schemes):
        return False

    # Pagination anywhere in the URL disqualifies it.
    if any(pat.search(lower) for pat in _PAGINATION_PATTERNS):
        return False

    # A doubled slash mid-path is a crawler join bug, not a real URL.
    path = lower.split("://", 1)[-1]
    if "//" in path:
        return False

    junk_suffixes = tuple(f"/{seg}" for seg in _INDEX_SEGMENTS) + ("/routelist.aspx",)
    if any(stripped.endswith(suf) for suf in junk_suffixes):
        return False

    return True


def derive_broker_domain(url: str) -> str | None:
    """Extract the bare domain (no www) from a listing URL."""
    if not url:
        return None
    try:
        netloc = urlparse(url).netloc or ""
        return netloc.replace("www.", "") or None
    except Exception:
        return None


def resolve_broker_name(broker_account: str, fallback_display_name: str | None = None) -> str:
    """
    Map a numeric account ID to a real broker name.
    Falls back to the explicit display_name from the BROKERS registry,
    then to the raw account string, then to "Unknown".
    """
    raw = str(broker_account or "").strip()
    if raw in ACCOUNT_TO_BROKER_NAME:
        return ACCOUNT_TO_BROKER_NAME[raw]
    if fallback_display_name:
        return fallback_display_name
    return raw or "Unknown"


# NOTE: the old local derive_title_from_url() was removed — it duplicated
# title_from_slug() WITHOUT the script-filename/reserved-route guards and would
# happily turn listingdetail.asp into "Listingdetail". junk_filter.title_from_slug
# is the single source of truth.
derive_title_from_url = title_from_slug


def listing_key(url: str, broker_domain: str | None = None) -> str:
    """
    Stable per-broker identity for a listing URL. The upsert conflict key
    (row `id`) is derived from THIS, not the raw URL — so cosmetic variance
    (execbb's trailing ID letters, vested's changing slug, http/https/www/
    trailing-slash flips) maps the same listing to the same row instead of
    inserting a duplicate.

    Per-broker, because each site encodes identity differently:
      execbb  -> numeric listingid only   (…listingid=44836130SXS4 == …44836130)
      vested  -> numeric listing-id only   (slug is volatile; the id is stable)
      default -> the raw URL, verbatim     (a different URL — e.g. a VR/bizbiz
                 slug append — is a genuine relist and must stay a distinct row)

    Returning the raw URL for the default case is deliberate: it keeps the id
    of every non-execbb/vested row IDENTICAL to the old md5(url) scheme, so the
    key change is surgical — only execbb and vested rows re-key, nothing else,
    and no full-table migration is needed. Index/placeholder URLs (e.g.
    'javascript:void(0)', broker '/listings/' pages) therefore behave exactly
    as before and are never merged by this function.
    """
    u = (url or "").strip()
    if not u:
        return ""
    host = (broker_domain or urlparse(u).netloc or "").replace("www.", "").lower()

    if "execbb.com" in host or "execbb.com" in u.lower():
        m = re.search(r"listingid=(\d+)", u, re.I)
        if m:
            return f"execbb:{m.group(1)}"

    if "vestedbb.com" in host or "vestedbb.com" in u.lower():
        m = re.search(r"listing-id-(\d+)", u, re.I)
        if m:
            return f"vestedbb:{m.group(1)}"

    return u


# ── Supabase upsert ───────────────────────────────────────────────────────────
# One rule, shared with dealledger_scraper_v6.py, for "the broker explicitly
# withholds the price" -> listings_direct.price_not_disclosed / broker_price_text.
from price_disclosure import (
    DisclosureColumns, disclosure_fields, flagged_without_phrase,
    group_by_keys, is_missing_disclosure_column, strip_disclosure,
)

# price_not_disclosed / broker_price_text may not exist yet (w14 migration);
# remembered for the whole process so the fallback is logged once.
_DISCLOSURE = DisclosureColumns()

# The broker's verbatim status label (decision 6) -> broker_status_text, with
# broker_status_seen_at. Same optional-column mechanism and the same field
# builder as dealledger_scraper_v6.SupabaseWriter (principle 15); the columns
# may not exist yet (j3 migration).
from optional_columns import missing_group, new_status_label_columns, status_label_fields
_STATUS_LABEL = new_status_label_columns()


def _optional_groups():
    """Looked up at call time (tests rebind the module globals)."""
    return (_DISCLOSURE, _STATUS_LABEL)


def domain_breakdown(listings: list[dict]) -> dict:
    """Pure. {broker_domain: n} the way upsert_listings would key the rows
    (URL required, deduped by listing_key). Rows later dropped for a missing
    title or a refused write are still counted - dry-run estimate only."""
    seen, out = set(), {}
    for l in listings or []:
        url = l.get("listing_url") or l.get("url") or ""
        if not url:
            continue
        d = derive_broker_domain(url)
        k = listing_key(url, d)
        if k in seen:
            continue
        seen.add(k)
        out[d] = out.get(d, 0) + 1
    return out


def upsert_listings(listings: list[dict], display_name: str | None = None,
                    domain_counts: dict | None = None) -> int:
    """
    Upsert specialized-scraper results to listings_direct.

    listings:     list of dicts as returned by specialized_scrapers.format_listing()
    display_name: explicit broker name from the BROKERS registry; used as a
                  fallback when account-ID resolution fails.
    domain_counts: optional dict, filled with {broker_domain: rows written}
                  for rows the server ACCEPTED (crawl_run per-domain rows).
    """
    if not listings:
        return 0

    # A 'DROP' row (Coming Soon / For Lease, scrapers/listing_status.py) is
    # never written: the scrapers filter them and log the count; this catches
    # one that slipped through, loudly, instead of sending status='DROP'
    # through the explicit-status path below (2026-11-12).
    slipped = sum(1 for l in listings if (l.get("status") or "") == "DROP")
    if slipped:
        log.warning(f"{slipped} row(s) with status DROP reached the writer - not written")
        listings = [l for l in listings if (l.get("status") or "") != "DROP"]
        if not listings:
            return 0

    # Deduplicate by STABLE per-broker key within this batch before upserting
    seen_keys = set()
    deduped = []
    for l in listings:
        url = l.get("listing_url") or l.get("url") or ""
        if not url:
            deduped.append(l)
            continue
        k = listing_key(url, derive_broker_domain(url))
        if k not in seen_keys:
            seen_keys.add(k)
            deduped.append(l)
    listings = deduped

    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    # Normalize to listings_direct schema (the unified table)
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    flag_no_phrase = 0
    for l in listings:
        url = l.get("listing_url") or l.get("url", "")
        if not url:
            continue
        # Sent only with evidence: a price (-> false) or the broker's explicit
        # phrase (-> true + verbatim). Neither -> not sent, so an existing row
        # keeps its value (principle 14) and a new row gets the default (false).
        disclosure = disclosure_fields(l)
        if flagged_without_phrase(l):
            flag_no_phrase += 1
        # Same rule for the broker's status label: sent only when the row
        # carries one, so a row without a label never nulls a stored one.
        label = status_label_fields(l, now)

        broker_domain = derive_broker_domain(url)
        key = listing_key(url, broker_domain)
        uid = f"spec:{hashlib.md5(key.encode()).hexdigest()[:16]}"
        broker_name = resolve_broker_name(l.get("broker_account", ""), display_name)
        url_is_listing_specific = detect_index_page_url(url)

        # ONE RULE: if the scraper's title is blank OR junk (a CTA/status/
        # category string rather than a business name), fall back to the URL
        # slug. Covers every specialized scraper in one place — Sunbelt's
        # register-modal string (971), Link's blanks (429), Bodner's category
        # column (~200), plus Transworld/Hedgestone's missing headings.
        title = (l.get("title") or "").strip()[:500]
        if is_junk_title(title, firm_name=broker_name):
            title = title_from_slug(url) or derive_title_from_url(url) or title or None

        rows.append({
            "id":            uid,
            "title":         title,
            "url":           url,
            "broker_name":   broker_name,
            "broker_domain": broker_domain,
            "city":          l.get("city") or l.get("location_city"),
            "state":         l.get("state") or l.get("location_state"),
            "asking_price":  int(l["price"]) if l.get("price") else None,
            "cash_flow":     int(l["cash_flow"]) if l.get("cash_flow") else None,
            "revenue":       int(l["revenue"]) if l.get("revenue") else None,
            "description":   (l.get("description") or "")[:2000] or None,
            # The scraper's industry/business type. format_listing() has always
            # carried it as business_type, but it was never written, so every
            # specialized feed landed with no category (tworld 1%, execbb 0%).
            "category":      (l.get("business_type") or "").strip()[:200] or None,
            "contact_name":  l.get("contact_name"),
            "contact_phone": l.get("contact_phone"),
            "location_raw":  l.get("location"),
            "status":        l.get("status") or "active",
            # Only a non-'active' status is evidence (a sold/pending badge the
            # scraper saw). format_listing() defaults status to 'active', so
            # treating that as evidence flipped every sold/inactive row the
            # scraper still saw back to active (112 Vested rows by Oct 11).
            "_status_explicit": (l.get("status") or "active") != "active",  # stripped before write; see below
            "source":        "broker_direct",
            "quality_tier":  "Unverified",
            "url_is_listing_specific": url_is_listing_specific,
            # Broker-reported publication date (e.g. WordPress `date`). Stored as
            # reported; never treated as the proven original listing date. Null
            # values don't overwrite (listings_direct_keep_filled trigger).
            "broker_published_on": l.get("broker_published_on"),
            "broker_modified_on":  l.get("broker_modified_on"),
            "broker_date_source":  l.get("broker_date_source"),
            "broker_date_first_recorded": now[:10] if l.get("broker_published_on") else None,
            "first_seen":    now,
            "last_seen":     now,
            "created_at":    now,
            "updated_at":    now,
            **(disclosure or {}),
            **(label or {}),
        })
    if flag_no_phrase:
        log.warning(f"{flag_no_phrase} row(s) flagged price_not_disclosed by the scraper "
                    f"with no broker phrase (broker_price_text/price_text) - flag not written")

    # Drop rows still missing a NOT NULL title (no scraper title, no usable
    # slug) so they can't reject an entire batch.
    dropped_null = sum(1 for r in rows if not r.get("title"))
    if dropped_null:
        log.warning(f"Skipping {dropped_null} row(s) with no derivable title")
    rows = [r for r in rows if r.get("title")]

    endpoint = f"{SUPABASE_URL}/rest/v1/listings_direct"

    def write_chunk(chunk: list[dict]) -> int:
        """Post a chunk; on rejection, split-and-retry down to single rows so
        one bad record can't take out its 99 good neighbours."""
        if not chunk:
            return 0
        groups = _optional_groups()
        for g in groups:
            g.strip_if_missing(chunk)
        r = http_requests.post(endpoint, headers=headers, json=chunk, timeout=60)
        if r.status_code in (200, 201):
            if domain_counts is not None:
                for row in chunk:
                    d = row.get("broker_domain")
                    domain_counts[d] = domain_counts.get(d, 0) + 1
            return len(chunk)
        # An optional column group (w14 disclosure, j3 status label) isn't
        # there yet: strip it and retry this chunk, instead of splitting it
        # down to single rows that all fail. Once per group per process.
        g = missing_group(r.text, groups)
        if g is not None:
            g.note_missing(log.warning, g.strip(chunk))
            return write_chunk(chunk)
        if len(chunk) == 1:
            log.error(f"Dropping bad row id={chunk[0].get('id')}: "
                      f"{r.status_code} {r.text[:150]}")
            return 0
        mid = len(chunk) // 2
        time.sleep(0.05)
        return write_chunk(chunk[:mid]) + write_chunk(chunk[mid:])

    # first_seen guard: existing rows must keep their original first_seen /
    # created_at (that anchor drives days-on-market). merge-duplicates would
    # otherwise overwrite them on every run, so figure out which ids already
    # exist and split the write: new rows carry first_seen/created_at, updates
    # drop them. (PostgREST also requires uniform keys per POST, so two batches.)
    existing = set()
    all_ids = [r["id"] for r in rows]
    for i in range(0, len(all_ids), 200):
        idl = ",".join(f'"{x}"' for x in all_ids[i:i + 200])
        try:
            rr = http_requests.get(endpoint, headers=headers,
                                   params={"id": f"in.({idl})", "select": "id"},
                                   timeout=30)
            if rr.ok:
                existing.update(row["id"] for row in rr.json())
        except Exception as e:
            log.warning(f"first_seen guard: existence check failed: {e}")

    new_rows = [r for r in rows if r["id"] not in existing]
    upd_rows = [r for r in rows if r["id"] in existing]
    for r in new_rows:
        r.pop("_status_explicit", None)

    # status guard: an existing row keeps whatever status it has (active,
    # superseded, a manual correction) unless THIS scraper run has positive
    # evidence otherwise (e.g. a detected sold/pending badge). Without this,
    # every successful re-scrape silently flipped a superseded row back to
    # active — restoring listing_key()'s id stability made this worse, not
    # better: once the id matched the original row again, upsert found it
    # and stamped over the supersession every run. No row's manual state
    # survives a scrape unless this guard is here.
    #
    # Split further (not just new vs. existing): PostgREST requires uniform
    # keys within one POST batch, and only SOME existing rows will carry
    # positive status evidence, so "status" can't be a key on some objects
    # in a batch and absent on others. One pass, each row lands in exactly
    # one of the two lists.
    upd_rows_status, upd_rows_nostatus = [], []
    for r in upd_rows:
        r.pop("first_seen", None)
        r.pop("created_at", None)
        if r.pop("_status_explicit"):
            upd_rows_status.append(r)
        else:
            r.pop("status", None)
            upd_rows_nostatus.append(r)

    upserted = 0
    for batch in (new_rows, upd_rows_status, upd_rows_nostatus):
        # Uniform keys per POST: rows with and without the optional fields
        # (disclosure, status label) are posted separately.
        for group in group_by_keys(batch):
            for i in range(0, len(group), 500):
                upserted += write_chunk(group[i:i + 500])
                time.sleep(0.1)

    for g in _optional_groups():
        if g.summary():
            log.warning(g.summary())
    return upserted


# ── Broker-displayed count (2026-11-11) ──────────────────────────────────────
# vikingmergers.com's REST post type holds 411 posts; its page says "Showing 62
# items". Every run closed 'ok'. For a broker whose public index page is known,
# fetch it once, read the count the broker states, and hand it to finish_run(),
# which closes the run 'partial' when we are outside 90-110% of it.
# Index page: wp_rest_brokers.csv `index_url` if that column exists, else the
# domain's row in data/brokers_clean.csv (regenerated from broker_sources at
# job start); other specialized brokers only via DISPLAYED_INDEX_URLS below. Stated counts only: one page
# cannot be card-counted when the index paginates.
DISPLAYED_INDEX_URLS = {
    # FacetWP status facet carries "For Sale (56)" / "Under Contract (16)".
    "eatz": "https://eatz-associates.com/listings/",
}


def _index_url_for(name: str, broker_meta: dict) -> str | None:
    if name in DISPLAYED_INDEX_URLS:
        return DISPLAYED_INDEX_URLS[name]
    # Scope: the WP-REST brokers (whose REST feed is known to over-count).
    # The hand-written scrapers (Transworld, Sunbelt, VR...) get an entry in
    # DISPLAYED_INDEX_URLS once someone has checked their page's count.
    if not name.startswith("wp_"):
        return None
    dom = (broker_meta.get("domain") or "").lower().removeprefix("www.")
    if not dom:
        return None
    here = os.path.dirname(__file__)
    for fname, col in (("wp_rest_brokers.csv", "index_url"), ("brokers_clean.csv", None)):
        path = os.path.join(here, "..", "data", fname)
        try:
            with open(path, newline="") as f:
                for row in _csv.DictReader(f):
                    if col:
                        d = (row.get("domain") or "").lower().removeprefix("www.")
                        u = (row.get(col) or "").strip()
                    else:
                        u = (row.get("listing_url") or "").strip()
                        d = (urlparse(u).netloc or "").lower().removeprefix("www.")
                    if d == dom and u:
                        return u
        except Exception:
            continue
    return None


def _displayed_count_for(name: str, broker_meta: dict, listings: list[dict]) -> dict:
    """finish_run() kwargs for the broker's displayed count. Never raises;
    {} when no index page is known or it states no count."""
    try:
        url = _index_url_for(name, broker_meta)
        if not url:
            return {}
        r = http_requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"})
        if r.status_code != 200:
            log.info(f"[{name}] displayed count: index HTTP {r.status_code} ({url})")
            return {}
        dc = parse_stated_count(r.text, seen=len(listings))   # seen: plausibility signal
        # Compare against what we would publish as for sale, not every REST
        # post: sold / inactive rows are not on the broker's for-sale page.
        active = sum(1 for l in listings
                     if (l.get("status") or "active") == "active")
        if not dc:
            log.info(f"[{name}] displayed count: none stated on {url}")
            return {"compared_count": active}
        log.info(f"[{name}] broker displays {dc['count']} (stated: '{dc['text']}'"
                 f"{'; ' + dc['note'] if dc.get('note') else ''}) vs {active} active seen "
                 f"of {len(listings)} -> {verdict_label(active, dc['count'])}")
        return {"displayed_count": dc["count"], "displayed_source": "stated",
                "displayed_note": f"matched: {dc['text']}; {dc.get('note') or ''}; index {url}"[:500],
                "compared_count": active}
    except Exception as e:                                  # never raise
        log.warning(f"[{name}] displayed count failed: {e}")
        return {}


def cleanup_stale_wesell_rows() -> int:
    """
    Remove stale We Sell Restaurants rows that predate the dedicated scraper.

    The old generic scrape left index-page rows under
    broker_domain='wesellrestaurants.com' (e.g. the account-2900 row stuck
    at a single '/restaurant-for-sale-near-me/...' URL since 2026-03-27).
    The new scraper writes per-listing detail URLs with 'spec:'-prefixed ids,
    so those legacy rows don't get superseded by the upsert. Delete any
    wesellrestaurants.com row whose URL is NOT a per-listing detail page.
    """
    if not SUPABASE_SERVICE_KEY:
        return 0
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    # A valid per-listing detail URL looks like
    # /restaurant-for-sale/<slug>/<id>. Anything else (index pages, the
    # legacy /restaurant-for-sale-near-me/... row) is stale. Paginate the
    # fetch so we don't miss rows beyond Supabase's default 1000-row window.
    def _is_detail_url(u: str) -> bool:
        u = (u or "").rstrip("/")
        if "/restaurant-for-sale-near-me/" in u:
            return False
        return bool(re.search(r"/restaurant-for-sale/[^/]+/\d+$", u))

    stale_ids = []
    offset, page_size = 0, 1000
    while True:
        try:
            r = http_requests.get(
                f"{SUPABASE_URL}/rest/v1/listings_direct",
                headers={**headers, "Range-Unit": "items",
                         "Range": f"{offset}-{offset + page_size - 1}"},
                params={"broker_domain": "eq.wesellrestaurants.com", "select": "id,url"},
                timeout=30,
            )
            r.raise_for_status()
            rows = r.json()
        except Exception as e:
            log.error(f"[wesell cleanup] fetch failed: {e}")
            break
        if not rows:
            break
        stale_ids.extend(row["id"] for row in rows if not _is_detail_url(row.get("url", "")))
        if len(rows) < page_size:
            break
        offset += page_size

    if not stale_ids:
        log.info("[wesell cleanup] no stale index-page rows found")
        return 0

    deleted = 0
    for i in range(0, len(stale_ids), 100):
        chunk = stale_ids[i:i + 100]
        id_list = ",".join(f'"{sid}"' for sid in chunk)
        dr = http_requests.delete(
            f"{SUPABASE_URL}/rest/v1/listings_direct",
            headers=headers,
            params={"id": f"in.({id_list})"},
            timeout=30,
        )
        if dr.status_code in (200, 204):
            deleted += len(chunk)
        else:
            log.error(f"[wesell cleanup] delete error {dr.status_code}: {dr.text[:200]}")
    log.info(f"[wesell cleanup] removed {deleted} stale index-page row(s)")
    return deleted


EMPTY_RETRY_WAIT = int(os.environ.get("EMPTY_RETRY_WAIT", "90"))


# ── broker_block ──────────────────────────────────────────────────────────────
# Triage rule (owner, 2026-11-11): a "Sign up to view" / VIP-access wall on financials = gated platform, not broker-direct -> broker_block (unbroker.com, flow.unbroker.com).
# V6 reads broker_block at startup (dealledger_scraper_v6.py RUNTIME_BLOCKED);
# this runner never did, so a domain blocked in the DB kept being crawled from
# data/wp_rest_brokers.csv (trufortebusinessgroup.com, 2026-11-11: blocked,
# still registered). Same bug class as V6's --broker path skipping the domain
# filter: a block that only some callers check is not a block.
def load_broker_block() -> set | None:
    """Bare domains on broker_block, or None if the table could not be read
    (no key, HTTP error). None is never treated as 'nothing is blocked'."""
    if not SUPABASE_SERVICE_KEY:
        return None
    h = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}
    try:
        r = http_requests.get(f"{SUPABASE_URL}/rest/v1/broker_block", headers=h, timeout=30,
                              params={"select": "broker_domain", "limit": "10000"})
        r.raise_for_status()
        rows = r.json()
    except Exception as e:
        log.error(f"broker_block: could not read table: {e}")
        return None
    out = set()
    for row in rows or []:
        d = (row.get("broker_domain") or "").strip().lower()
        if d:
            out.add(d[4:] if d.startswith("www.") else d)
    return out


def is_block_listed(domain: str | None, blocked: set) -> bool:
    d = (domain or "").strip().lower()
    d = d[4:] if d.startswith("www.") else d
    return bool(d) and any(d == b or d.endswith("." + b) for b in blocked)


# ── Main ──────────────────────────────────────────────────────────────────────
def run(broker_filter: list[str] | None, dry_run: bool):
    if not SUPABASE_SERVICE_KEY and not dry_run:
        log.error("Set SUPABASE_SERVICE_KEY env var")
        sys.exit(1)

    if not dry_run:
        n = expire_stale_runs()
        if n:
            log.info(f"crawl_run: marked {n} row(s) running > 6h as failed/timeout")

    to_run = broker_filter if broker_filter else list(BROKERS.keys())
    log.info(f"Running {len(to_run)} specialized scrapers: {', '.join(to_run)}")

    blocked = load_broker_block()
    if blocked is not None:
        log.info(f"broker_block: {len(blocked)} domains")
    elif dry_run:
        log.warning("broker_block NOT read (dry run / no key) — block list not "
                    "enforced for this dry run; nothing is written")
    else:
        log.error("broker_block NOT read — every CSV-registered (WP-REST) broker is "
                  "skipped this run and the job exits non-zero")
    unverified = []

    grand_total = 0
    results = {}
    domain_row_failures = []   # (broker, domain) whose crawl_run row was refused

    for name in to_run:
        if name not in BROKERS:
            log.warning(f"Unknown broker: {name}")
            continue

        broker_meta = BROKERS[name]
        if blocked is not None and is_block_listed(broker_meta.get("domain"), blocked):
            log.warning(f"[{name}] {broker_meta.get('domain')} is on broker_block — skipped")
            results[name] = "blocked"
            continue
        if blocked is None and not dry_run and broker_meta.get("wp_rest"):
            log.error(f"[{name}] skipped: broker_block unreadable, cannot prove "
                      f"{broker_meta.get('domain')} is not blocked")
            results[name] = "unverified"
            unverified.append(name)
            continue
        log.info(f"\n{'='*60}\nScraping: {name.upper()} ({broker_meta['display_name']})\n{'='*60}")

        # crawl_run (2026-09-24): every row this opens is closed, ok or
        # failed, with broker_domain set.
        #   - opened BEFORE the scrape, with the registry domain, so a crash
        #     or kill leaves a 'running' row the 6h sweep turns into
        #     failed/timeout rather than no row at all;
        #   - 0 listings closes as failed/EMPTY. The old code sent 'empty',
        #     which crawl_run_status_check rejects - the PATCH 400'd silently
        #     and the row stayed 'running' forever;
        #   - the finally: catches KeyboardInterrupt/SystemExit too, which
        #     `except Exception` did not.
        #   - dry runs open no row at all ('dry_run' is not a valid status
        #     either).
        run_id = None if dry_run else start_run("specialized", broker_meta.get("domain"))
        outcome = ("failed", 0, 0, "interrupted")
        dc_kw = {}
        try:
            listings = broker_meta["fn"]()
            # 2026-11-04: Transworld, Sunbelt, routesforsale and Vested come
            # back EMPTY on one of the two daily runs and fine on the other.
            # Retry an empty result once after a pause before closing it as
            # failed/EMPTY. Never retried for dry runs.
            if not listings and not dry_run:
                log.warning(f"[{name}] 0 listings; retrying once in {EMPTY_RETRY_WAIT}s")
                time.sleep(EMPTY_RETRY_WAIT)
                listings = broker_meta["fn"]()
            log.info(f"[{name}] Got {len(listings)} listings")
            results[name] = len(listings)
            if listings:
                dc_kw = _displayed_count_for(name, broker_meta, listings)

            if dry_run:
                if listings:
                    sample = listings[0]
                    log.info(
                        f"  Sample: {sample.get('title','?')[:80]} | "
                        f"${sample.get('price','?')} | "
                        f"{sample.get('city','?')}, {sample.get('state','?')}"
                    )
                    log.info(f"  Would write as broker_name='{broker_meta['display_name']}'")
                    extra = domain_counts_excluding(domain_breakdown(listings),
                                                    broker_meta.get("domain"))
                    log.info(f"  Would log crawl_run: {broker_meta.get('domain')} "
                             f"(run total) + {len(extra)} domain row(s)"
                             + "".join(f"\n    {d:<40} {n:>6}" for d, n in
                                       sorted(extra.items(), key=lambda x: -x[1])))
                continue

            if not listings:
                outcome = ("failed", 0, 0, "EMPTY: scraper returned 0 listings")
                continue

            written_by_domain = {}
            upserted = upsert_listings(listings, display_name=broker_meta["display_name"],
                                       domain_counts=written_by_domain)
            log.info(f"[{name}] Upserted {upserted} rows")
            grand_total += upserted

            # 'ok' is load-bearing: v_dom_direct filters status='ok' to find the
            # last successful crawl before a listing appeared.
            outcome = ("ok", len(listings), upserted, None)

            # The registry row above carries the whole run. Listings that land
            # on another host (Bodner -> execbb.com, VR -> ~30 office domains)
            # get one closed row per host with that host's count, because
            # v_dom_direct / absence_candidates join crawl_run on broker_domain.
            # The registry domain itself never gets a second row.
            ids, failed = record_domain_runs("specialized", broker_meta.get("domain"),
                                             written_by_domain)
            if ids:
                per = domain_counts_excluding(written_by_domain, broker_meta.get("domain"))
                log.info(f"[{name}] crawl_run domain rows: "
                         + ", ".join(f"{d}={per[d]}" for d in sorted(ids)))
            for d in failed:
                domain_row_failures.append((name, d))

            # Post-upsert cleanup of legacy index-page rows for We Sell Restaurants
            if name == "wesell":
                cleanup_stale_wesell_rows()

        except Exception as e:
            log.error(f"[{name}] Failed: {e}")
            results[name] = 0
            outcome = ("failed", 0, 0, f"{type(e).__name__}: {e}")
        finally:
            status, fetched, seen, err = outcome
            finish_run(run_id, status, urls_fetched=fetched,
                       listings_seen=seen, error=err, **dc_kw)

    log.info(f"\n{'='*60}")
    log.info(f"DONE — {grand_total} total listings upserted to listings_direct")
    for name, count in results.items():
        log.info(f"  {name:<15} {count:>5}")
    if domain_row_failures:
        log.error(f"crawl_run: {len(domain_row_failures)} per-domain row(s) NOT written "
                  f"(those domains have no 'ok' crawl for today): "
                  + ", ".join(f"{b}:{d}" for b, d in domain_row_failures))
    if unverified:
        log.error(f"{len(unverified)} WP-REST broker(s) skipped because broker_block "
                  f"could not be read: {', '.join(unverified)}")
        sys.exit(4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", help="Comma-separated broker names (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    broker_filter = [b.strip() for b in args.brokers.split(",")] if args.brokers else None
    run(broker_filter, args.dry_run)
