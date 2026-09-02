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
from junk_filter import is_junk_title, title_from_slug
from specialized_scrapers import (
    MurphyScraper, HedgestoneScraper, TransworldScraper,
    SunbeltScraper, VRScraper, FCBBScraper,
    LinkBusinessScraper, LarryBodnerScraper,
    WeSellRestaurantsScraper, VestedScraper, RoutesForSaleScraper,
    WPRestScraper
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# ── Broker registry ───────────────────────────────────────────────────────────
# Each entry has:
#   account      → numeric account ID (matches broker_master / past data)
#   display_name → real broker name written to listings_direct.broker_name
#   fn           → callable that runs the scraper and returns List[Dict]
BROKERS = {
    "transworld": {
        "account": "28148",
        "display_name": "Transworld Business Advisors",
        "fn": lambda: TransworldScraper().scrape("28148", max_pages=450, workers=8),
    },
    "sunbelt": {
        "account": "1001",
        "display_name": "Sunbelt Business Brokers",
        "fn": lambda: SunbeltScraper().scrape("1001", max_pages=130),
    },
    "fcbb": {
        "account": "1002",
        "display_name": "First Choice Business Brokers (FCBB)",
        "fn": lambda: FCBBScraper().scrape("1002", max_pages=79),
    },
    "murphy": {
        "account": "1003",
        "display_name": "Murphy Business",
        "fn": lambda: MurphyScraper.scrape("1003", max_pages=50),
    },
    "vr": {
        "account": "1004",
        "display_name": "VR Business Brokers",
        "fn": lambda: VRScraper().scrape("1004", max_pages=40),
    },
    "hedgestone": {
        "account": "28149",
        "display_name": "Hedgestone Business Advisors",
        "fn": lambda: HedgestoneScraper().scrape("28149", max_pages=40),
    },
    "link": {
        "account": "1005",
        "display_name": "Link Business",
        "fn": lambda: LinkBusinessScraper().scrape("1005", max_pages=60),
    },
    "bodner": {
        "account": "1006",
        "display_name": "Executive Business Brokers (Larry Bodner)",
        "fn": lambda: LarryBodnerScraper().scrape("1006"),
    },
    "wesell": {
        "account": "2900",
        "display_name": "We Sell Restaurants",
        "fn": lambda: WeSellRestaurantsScraper().scrape("2900", max_pages=40),
    },
    "vested": {
        "account": "1593",
        "display_name": "Vested Business Brokers",
        "fn": lambda: VestedScraper().scrape("1593", max_pages=130),
    },
    "routesforsale": {
        "account": "13461",
        "display_name": "Routes For Sale",
        "fn": lambda: RoutesForSaleScraper().scrape("13461"),
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
            BROKERS[key] = {
                "account": account,
                "display_name": display,
                "fn": (lambda d=domain, rb=rest, a=account:
                       WPRestScraper(d, rb).scrape(a, max_pages=60)),
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
def upsert_listings(listings: list[dict], display_name: str | None = None) -> int:
    """
    Upsert specialized-scraper results to listings_direct.

    listings:     list of dicts as returned by specialized_scrapers.format_listing()
    display_name: explicit broker name from the BROKERS registry; used as a
                  fallback when account-ID resolution fails.
    """
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
    for l in listings:
        url = l.get("listing_url") or l.get("url", "")
        if not url:
            continue

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
            "contact_name":  l.get("contact_name"),
            "contact_phone": l.get("contact_phone"),
            "location_raw":  l.get("location"),
            "status":        l.get("status") or "active",
            "source":        "broker_direct",
            "quality_tier":  "Unverified",
            "url_is_listing_specific": url_is_listing_specific,
            "first_seen":    now,
            "last_seen":     now,
            "created_at":    now,
            "updated_at":    now,
        })

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
        r = http_requests.post(endpoint, headers=headers, json=chunk, timeout=60)
        if r.status_code in (200, 201):
            return len(chunk)
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
    for r in upd_rows:
        r.pop("first_seen", None)
        r.pop("created_at", None)

    upserted = 0
    for batch in (new_rows, upd_rows):
        for i in range(0, len(batch), 500):
            upserted += write_chunk(batch[i:i + 500])
            time.sleep(0.1)

    return upserted


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


# ── Main ──────────────────────────────────────────────────────────────────────
def run(broker_filter: list[str] | None, dry_run: bool):
    if not SUPABASE_SERVICE_KEY and not dry_run:
        log.error("Set SUPABASE_SERVICE_KEY env var")
        sys.exit(1)

    to_run = broker_filter if broker_filter else list(BROKERS.keys())
    log.info(f"Running {len(to_run)} specialized scrapers: {', '.join(to_run)}")

    grand_total = 0
    results = {}

    for name in to_run:
        if name not in BROKERS:
            log.warning(f"Unknown broker: {name}")
            continue

        broker_meta = BROKERS[name]
        log.info(f"\n{'='*60}\nScraping: {name.upper()} ({broker_meta['display_name']})\n{'='*60}")
        try:
            listings = broker_meta["fn"]()
            log.info(f"[{name}] Got {len(listings)} listings")
            results[name] = len(listings)

            if dry_run:
                if listings:
                    sample = listings[0]
                    log.info(
                        f"  Sample: {sample.get('title','?')[:80]} | "
                        f"${sample.get('price','?')} | "
                        f"{sample.get('city','?')}, {sample.get('state','?')}"
                    )
                    log.info(f"  Would write as broker_name='{broker_meta['display_name']}'")
                continue

            upserted = upsert_listings(listings, display_name=broker_meta["display_name"])
            log.info(f"[{name}] Upserted {upserted} rows")
            grand_total += upserted

            # Post-upsert cleanup of legacy index-page rows for We Sell Restaurants
            if name == "wesell":
                cleanup_stale_wesell_rows()

        except Exception as e:
            log.error(f"[{name}] Failed: {e}")
            results[name] = 0

    log.info(f"\n{'='*60}")
    log.info(f"DONE — {grand_total} total listings upserted to listings_direct")
    for name, count in results.items():
        log.info(f"  {name:<15} {count:>5}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", help="Comma-separated broker names (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    broker_filter = [b.strip() for b in args.brokers.split(",")] if args.brokers else None
    run(broker_filter, args.dry_run)
