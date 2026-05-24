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
        # Vending lives in the SAME Supabase project as cleaning
        # (ctvrauiiskucinibnfaj). Share its URL/key so vending no longer
        # silently skips for missing env. Same fallbacks as cleaning.
        "supabase_url": os.environ.get("VENDINGEXITS_SUPABASE_URL",
                                       os.environ.get("CLEANINGEXITS_SUPABASE_URL",
                                                      "https://ctvrauiiskucinibnfaj.supabase.co")),
        "supabase_key": os.environ.get("VENDINGEXITS_SERVICE_KEY",
                                       os.environ.get("CLEANINGEXITS_SERVICE_KEY", "")),
        "table":        "vending_listings_merge",
        # NOTE: 'ATM' removed — ATM is its own vertical (ATMExits). Leaving it
        # here pulled ATM-business listings into VendingExits.
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
        "select": "id,header,price,cash_flow,revenue,location,state,city,"
                  "broker_name,broker_url,listing_url,direct_broker_url,"
                  "contact_name,contact_phone,notes,scraped_at,first_seen,"
                  "bbs_listing_number,listing_views,days_on_market,"
                  "quality_score,quality_tier,quality_rules_fired,"
                  "bbs_account_id,is_active",
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
    """Map DealLedger fields to vertical table schema.

    Two verticals, two target schemas:
      - cleaning_listings_merge: header / url / notes / is_verified
      - vending_listings_merge:  title / listing_url / description / is_active
        (the columns the VendingExits site code actually reads)
    """
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

    title    = row.get("header") or row.get("title", "")
    url      = row.get("listing_url") or row.get("url", "")
    is_active = bool(row.get("is_active", True))

    common = {
        "price":           row.get("price"),
        "cash_flow":       row.get("cash_flow"),
        "revenue":         str(row.get("revenue", "") or ""),
        "location":        row.get("location") or row.get("location_raw", ""),
        "city":            row.get("city") or row.get("location_city", ""),
        "state":           row.get("state") or row.get("location_state", ""),
        "broker_account":  row.get("broker_name", ""),
        "contact_name":    row.get("contact_name", ""),
        "contact_phone":   row.get("contact_phone", ""),
        "quality_score":   row.get("quality_score"),
        "quality_tier":    row.get("quality_tier") or row.get("trust_tier", "Unverified"),
        "scraped_at":      row.get("scraped_at") or now,
        "synced_at":       now,
    }

    if vertical == "vending":
        # Match vending_listings_merge / the VendingExits site code.
        return {
            **common,
            "listing_id":  row.get("id") or url[-40:],
            "title":       title,
            "listing_url": url,
            "description": row.get("notes") or row.get("description", ""),
            "status":      "active" if is_active else "removed",
            "is_active":   is_active,
            "relist_count": row.get("relist_count") or 0,
            "first_seen":  row.get("first_seen"),
            "last_seen":   row.get("last_seen"),
        }

    # cleaning (unchanged original schema)
    return {
        **common,
        "id":              row.get("id") or url[-40:],
        "source":          "dealledger",
        "source_id":       str(row.get("id", "")),
        "url":             url,
        "direct_broker_url": row.get("direct_broker_url") or row.get("broker_url"),
        "header":          title,
        "notes":           row.get("notes") or row.get("description", ""),
        "days_on_market":  dom,
        "listing_views":   row.get("listing_views") or row.get("bbs_views"),
        "bbs_account_id":  row.get("bbs_account_id"),
        "is_verified":     row.get("quality_tier") == "Verified",
        "calculated_multiple": (
            round(float(row["price"]) / float(row["cash_flow"]), 2)
            if row.get("price") and row.get("cash_flow") and float(row.get("cash_flow", 0)) > 0
            else None
        ),
    }


def sync_vertical(vertical: str, config: dict, min_score: int, dry_run: bool):
    if not config.get("supabase_url") or not config.get("supabase_key"):
        log.warning(f"Skipping {vertical} — missing Supabase URL or key")
        return

    all_listings = dl_fetch_scored(min_score)

    # Filter to vertical
    vertical_listings = [r for r in all_listings if matches_vertical(r, config)]
    log.info(f"[{vertical}] {len(vertical_listings)} listings match vertical keywords")

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
            disp = r.get("header") or r.get("title", "")
            log.info(f"  Sample: {disp[:60]} | ${r.get('price','?')} | {r['quality_tier']}")
        return

    # Upsert
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
