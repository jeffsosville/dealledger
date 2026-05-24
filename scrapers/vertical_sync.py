#!/usr/bin/env python3
"""
vertical_sync.py

Pulls quality-scored listings from DealLedger Supabase and syncs them
into the appropriate vertical Supabase instance (CleaningExits, VendingExits, etc.)

Runs AFTER quality_scorer.py in the pipeline.

Usage:
    python3 scrapers/vertical_sync.py --vertical cleaning
    python3 scrapers/vertical_sync.py --vertical vending
    python3 scrapers/vertical_sync.py --vertical all
    python3 scrapers/vertical_sync.py --vertical cleaning --min-score 40 --dry-run
"""

import os, re, sys, time, argparse, logging
from datetime import datetime, timezone
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── DealLedger (master) ───────────────────────────────────────────────────────
DL_URL = os.environ.get("SUPABASE_URL",  "https://kqckuedsyyosmccushyd.supabase.co")
DL_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# ── Vertical configs ──────────────────────────────────────────────────────────
VERTICALS = {
    "cleaning": {
        "supabase_url": os.environ.get("CLEANINGEXITS_SUPABASE_URL", "https://ctvrauiiskucinibnfaj.supabase.co"),
        "supabase_key": os.environ.get("CLEANINGEXITS_SERVICE_KEY", ""),
        "table":        "cleaning_listings_merge",
        "keywords":     ["cleaning", "laundromat", "laundry", "dry clean", "janitorial",
                         "maid", "carpet clean", "window clean", "pressure wash",
                         "landscap", "lawn", "pool service", "pest control", "junk removal"],
        "exclude":      ["real estate", "realty", "property"],
    },
    "vending": {
        "supabase_url": os.environ.get("VENDINGEXITS_SUPABASE_URL") or os.environ.get("CLEANINGEXITS_SUPABASE_URL") or "https://ctvrauiiskucinibnfaj.supabase.co",
        "supabase_key": os.environ.get("VENDINGEXITS_SERVICE_KEY", os.environ.get("CLEANINGEXITS_SERVICE_KEY", "")),
        "table":        "vending_listings_merge",
        "keywords":     ["vending", "vend machine", "coin-op", "amusement"],
        "exclude":      ["real estate"],
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────
def sb_headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

def dl_fetch_scored(min_score: int = 0) -> list[dict]:
    """Pull all active listings from DealLedger that have been scored."""
    log.info(f"Fetching scored listings from DealLedger (min_score={min_score})...")
    rows, offset = [], 0
    params_base = {
        "select": "*",
        "is_active": "eq.true",
        "quality_tier": "not.is.null",
        "order": "quality_score.desc",
    }
    if min_score > 0:
        params_base["quality_score"] = f"gte.{min_score}"

    while True:
        p = {**params_base, "limit": 1000, "offset": offset}
        r = requests.get(
            f"{DL_URL}/rest/v1/listings",
            headers=sb_headers(DL_KEY),
            params=p,
            timeout=30,
        )
        if r.status_code != 200:
            log.error(f"DealLedger fetch error: {r.status_code} {r.text[:200]}")
            break
        batch = r.json()
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
        time.sleep(0.1)

    # Also pull from listings_broker (direct broker listings)
    log.info("Also fetching from listings_broker...")
    offset = 0
    while True:
        p = {"select": "*", "is_active": "eq.true", "limit": 1000, "offset": offset}
        r = requests.get(
            f"{DL_URL}/rest/v1/listings_broker",
            headers=sb_headers(DL_KEY),
            params=p,
            timeout=30,
        )
        if r.status_code == 404:
            log.info("listings_broker table not yet created, skipping")
            break
        if r.status_code != 200:
            break
        batch = r.json()
        # Tag as direct
        for row in batch:
            row["trust_tier"] = "direct"
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
        time.sleep(0.1)

    log.info(f"Total scored listings from DealLedger: {len(rows)}")
    return rows


def matches_vertical(row: dict, config: dict) -> bool:
    title = (row.get("header") or row.get("title") or "").lower()
    if not title:
        return False
    if any(ex in title for ex in config["exclude"]):
        return False
    return any(kw in title for kw in config["keywords"])


def transform_for_vertical(row: dict, vertical: str) -> dict:
    """Map DealLedger fields to vertical table schema."""
    now = datetime.now(timezone.utc).isoformat()

    # DOM: use days_on_market if available, else estimate from bbs_listing_number
    dom = row.get("days_on_market")
    if dom is None:
        ln = row.get("bbs_listing_number")
        if ln:
            from datetime import date
            ANCHOR_DATE = date(2025, 5, 14)
            ANCHOR_NUM  = 2_367_857
            RATE        = 373.8
            days = (int(ln) - ANCHOR_NUM) / RATE
            anchor_ord = ANCHOR_DATE.toordinal()
            est_date = date.fromordinal(int(anchor_ord + days))
            dom = (date.today() - est_date).days

    # --- vending: map to vending_listings_merge real columns ---
    if vertical == "vending":
        _title = row.get("header") or row.get("title", "")
        _url   = row.get("url") or row.get("listing_url", "")
        _loc   = ", ".join([x for x in [row.get("city"), row.get("state")] if x])
        _active = bool(row.get("is_active", True))
        return {
            "listing_id":    row.get("id") or _url[-40:],
            "title":         _title,
            "listing_url":   _url,
            "location":      _loc,
            "city":          row.get("city", ""),
            "state":         row.get("state", ""),
            "price":         row.get("price"),
            "cash_flow":     row.get("cash_flow"),
            "revenue":       str(row.get("revenue", "") or ""),
            "description":   "",
            "broker_account": row.get("broker_account", ""),
            "contact_name":  row.get("contact_name", ""),
            "contact_phone": row.get("contact_phone", ""),
            "status":        "active" if _active else "removed",
            "is_active":     _active,
            "relist_count":  row.get("relist_count") or 0,
            "first_seen":    row.get("first_seen"),
            "last_seen":     row.get("last_seen"),
            "quality_score": row.get("quality_score"),
            "quality_tier":  row.get("quality_tier") or "Unverified",
            "scraped_at":    row.get("first_seen") or now,
            "synced_at":     now,
        }

    return {
        # Core identity
        "id":              row.get("id") or row.get("listing_url", "")[-40:],
        "source":          "dealledger",
        "source_id":       str(row.get("id", "")),
        "url":             row.get("listing_url") or row.get("url", ""),
        "direct_broker_url": row.get("direct_broker_url") or row.get("broker_url"),

        # Listing content
        "header":          row.get("header") or row.get("title", ""),
        "price":           row.get("price"),
        "cash_flow":       row.get("cash_flow"),
        "revenue":         str(row.get("revenue", "") or ""),
        "notes":           row.get("notes") or row.get("description", ""),
        "location":        row.get("location") or row.get("location_raw", ""),
        "city":            row.get("city") or row.get("location_city", ""),
        "state":           row.get("state") or row.get("location_state", ""),

        # Broker / contact
        "broker_account":  row.get("broker_name", ""),
        "contact_name":    row.get("contact_name", ""),
        "contact_phone":   row.get("contact_phone", ""),

        # Quality signals
        "quality_score":   row.get("quality_score"),
        "quality_tier":    row.get("quality_tier") or row.get("trust_tier", "Unverified"),
        "days_on_market":  dom,
        "listing_views":   row.get("listing_views") or row.get("bbs_views"),
        "bbs_account_id":  row.get("bbs_account_id"),
        "is_verified":     row.get("quality_tier") == "Verified",

        # Computed
        "calculated_multiple": (
            round(float(row["price"]) / float(row["cash_flow"]), 2)
            if row.get("price") and row.get("cash_flow") and float(row.get("cash_flow", 0)) > 0
            else None
        ),

        # Timestamps
        "scraped_at":     row.get("scraped_at") or now,
    }



def drop_spam_clones(rows, min_cluster=4):
    """Drop templated mass-posted listings: same price AND same title
    skeleton (city collapsed) across >= min_cluster listings."""
    import re as _re
    from collections import defaultdict as _dd
    def _norm(t):
        t = (t or "").lower()
        t = _re.sub(r"throughout .*$", "throughout <city>", t)
        t = _re.sub(r"[^a-z0-9 ]", "", t).strip()
        return t
    buckets = _dd(list)
    for i, row in enumerate(rows):
        buckets[(row.get("price"), _norm(row.get("header") or row.get("title")))].append(i)
    drop = set()
    for key, idxs in buckets.items():
        if len(idxs) >= min_cluster and key[0] is not None:
            drop.update(idxs)
    kept = [r for i, r in enumerate(rows) if i not in drop]
    return kept, len(drop)



def live_columns(base_url, key, table):
    """Return the set of columns PostgREST currently exposes for `table`."""
    import requests as _rq
    try:
        r=_rq.get(f"{base_url}/rest/v1/",
            headers={"apikey":key,"Authorization":f"Bearer {key}"}, timeout=30)
        props=r.json().get("definitions",{}).get(table,{}).get("properties",{})
        return set(props.keys())
    except Exception as _e:
        return set()


def sync_vertical(vertical: str, config: dict, min_score: int, dry_run: bool):
    if not config.get("supabase_url") or not config.get("supabase_key"):
        log.warning(f"Skipping {vertical} — missing Supabase URL or key")
        return

    all_listings = dl_fetch_scored(min_score)

    # Filter to vertical
    vertical_listings = [r for r in all_listings if matches_vertical(r, config)]
    log.info(f"[{vertical}] {len(vertical_listings)} listings match vertical keywords")

    vertical_listings, n_spam = drop_spam_clones(vertical_listings)
    if n_spam:
        log.info(f"[{vertical}] Dropped {n_spam} templated spam clones")

    if not vertical_listings:
        log.info(f"[{vertical}] Nothing to sync")
        return

    # Transform
    transformed = [transform_for_vertical(r, vertical) for r in vertical_listings]

    # Stats
    tiers = {}
    for r in transformed:
        t = r.get("quality_tier", "Unknown")
        tiers[t] = tiers.get(t, 0) + 1
    log.info(f"[{vertical}] Tier breakdown: {tiers}")

    if dry_run:
        log.info(f"[{vertical}] Dry run — not writing to Supabase")
        for r in transformed[:3]:
            log.info(f"  Sample: {r['header'][:60]} | ${r.get('price','?')} | {r['quality_tier']}")
        return

    # Upsert
    _cols = live_columns(config["supabase_url"], config["supabase_key"], config["table"])
    if _cols:
        before = len(transformed[0]) if transformed else 0
        transformed = [{k:v for k,v in r.items() if k in _cols} for r in transformed]
        log.info(f"[{vertical}] Filtered rows to {len(_cols)} live table columns "
                 f"(dropped {before-len(transformed[0]) if transformed else 0} unknown fields)")
    log.info(f"[{vertical}] Upserting {len(transformed)} rows to {config['table']}...")
    vurl, vkey = config["supabase_url"], config["supabase_key"]
    for i in range(0, len(transformed), 500):
        batch = transformed[i:i+500]
        r = requests.post(
            f"{vurl}/rest/v1/{config['table']}",
            headers=sb_headers(vkey),
            json=batch,
            timeout=60,
        )
        if r.status_code not in (200, 201):
            log.error(f"Upsert error {r.status_code}: {r.text[:200]}")
        else:
            log.info(f"  Synced rows {i}–{i+len(batch)-1}")
        time.sleep(0.1)

    log.info(f"[{vertical}] Sync complete.")


def run(args):
    if not DL_KEY:
        log.error("Set SUPABASE_SERVICE_KEY env var")
        sys.exit(1)

    verticals_to_run = (
        list(VERTICALS.keys()) if args.vertical == "all"
        else [args.vertical]
    )

    for v in verticals_to_run:
        if v not in VERTICALS:
            log.error(f"Unknown vertical: {v}. Available: {list(VERTICALS.keys())}")
            continue
        log.info(f"\n{'='*60}\nSyncing vertical: {v.upper()}\n{'='*60}")
        sync_vertical(v, VERTICALS[v], args.min_score, args.dry_run)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--vertical",  default="cleaning", help="Vertical to sync (or 'all')")
    parser.add_argument("--min-score", type=int, default=0, help="Minimum quality score to sync")
    parser.add_argument("--dry-run",   action="store_true")
    args = parser.parse_args()
    run(args)
