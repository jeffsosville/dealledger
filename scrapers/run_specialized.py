#!/usr/bin/env python3
"""
run_specialized.py

Runs all 8 specialized broker scrapers and upserts results
to DealLedger Supabase → listings_broker table.

Usage:
    python3 scrapers/run_specialized.py
    python3 scrapers/run_specialized.py --brokers transworld,sunbelt,fcbb
    python3 scrapers/run_specialized.py --dry-run
"""

import os, sys, json, time, hashlib, argparse, logging
from datetime import datetime, timezone
import requests as http_requests

# Add parent dir to path so we can import specialized_scrapers
sys.path.insert(0, os.path.dirname(__file__))
from specialized_scrapers import (
    MurphyScraper, HedgestoneScraper, TransworldScraper,
    SunbeltScraper, VRScraper, FCBBScraper,
    LinkBusinessScraper, LarryBodnerScraper
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

# ── Broker registry ───────────────────────────────────────────────────────────
BROKERS = {
    "transworld": {
        "account": "28148",
        "fn": lambda: TransworldScraper().scrape("28148", max_pages=150, workers=8),
    },
    "sunbelt": {
        "account": "1001",
        "fn": lambda: SunbeltScraper().scrape("1001", max_pages=130),
    },
    "fcbb": {
        "account": "1002",
        "fn": lambda: FCBBScraper().scrape("1002", max_pages=79),
    },
    "murphy": {
        "account": "1003",
        "fn": lambda: MurphyScraper.scrape("1003", max_pages=50),
    },
    "vr": {
        "account": "1004",
        "fn": lambda: VRScraper().scrape("1004", max_pages=15),
    },
    "hedgestone": {
        "account": "28149",
        "fn": lambda: HedgestoneScraper().scrape("28149", max_pages=15),
    },
    "link": {
        "account": "1005",
        "fn": lambda: LinkBusinessScraper().scrape("1005", max_pages=20),
    },
    "bodner": {
        "account": "1006",
        "fn": lambda: LarryBodnerScraper().scrape("1006"),
    },
}

# ── Supabase upsert ───────────────────────────────────────────────────────────
def upsert_listings(listings: list[dict]) -> int:
    if not listings:
        return 0

    # Deduplicate by URL before upserting
    seen_urls = set()
    deduped = []
    for l in listings:
        url = l.get("listing_url") or ""
        if url and url not in seen_urls:
            seen_urls.add(url)
            deduped.append(l)
        elif not url:
            deduped.append(l)
    listings = deduped

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }

    # Normalize to listings_broker schema
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for l in listings:
        url = l.get("listing_url") or l.get("url", "")
        if not url:
            continue
        uid = f"spec:{hashlib.md5(url.encode()).hexdigest()[:16]}"
        rows.append({
            "id":           uid,
            "broker_name":  l.get("broker_account", ""),
            "listing_url":  url,
            "title":        l.get("title"),
            "price":        int(l["price"]) if l.get("price") else None,
            "cash_flow":    int(l["cash_flow"]) if l.get("cash_flow") else None,
            "revenue":      int(l["revenue"]) if l.get("revenue") else None,
            "location_raw": l.get("location"),
            "location_city": l.get("city"),
            "location_state": l.get("state"),
            "description":  l.get("description"),
            "source":       "broker_direct",
            "trust_tier":   "direct",
            "is_active":    True,
            "scraped_at":   now,
            "last_seen":    now,
        })

    upserted = 0
    for i in range(0, len(rows), 500):
        batch = rows[i:i+500]
        r = http_requests.post(
            f"{SUPABASE_URL}/rest/v1/listings_broker",
            headers=headers,
            json=batch,
            timeout=60,
        )
        if r.status_code in (200, 201):
            upserted += len(batch)
        else:
            log.error(f"Upsert error {r.status_code}: {r.text[:200]}")
        time.sleep(0.1)

    return upserted


# ── Main ──────────────────────────────────────────────────────────────────────
def run(broker_filter: list[str] | None, dry_run: bool):
    if not SUPABASE_KEY and not dry_run:
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

        log.info(f"\n{'='*60}\nScraping: {name.upper()}\n{'='*60}")
        try:
            listings = BROKERS[name]["fn"]()
            log.info(f"[{name}] Got {len(listings)} listings")
            results[name] = len(listings)

            if dry_run:
                if listings:
                    log.info(f"  Sample: {listings[0].get('title','?')} | ${listings[0].get('price','?')}")
                continue

            upserted = upsert_listings(listings)
            log.info(f"[{name}] Upserted {upserted} rows")
            grand_total += upserted

        except Exception as e:
            log.error(f"[{name}] Failed: {e}")
            results[name] = 0

    log.info(f"\n{'='*60}")
    log.info(f"DONE — {grand_total} total listings upserted")
    for name, count in results.items():
        log.info(f"  {name:<15} {count:>5}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--brokers", help="Comma-separated broker names (default: all)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    broker_filter = [b.strip() for b in args.brokers.split(",")] if args.brokers else None
    run(broker_filter, args.dry_run)
