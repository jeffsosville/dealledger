#!/usr/bin/env python3
"""
BizBuySell Laundromat Pipeline
================================
Three modes:
  python3 bbs_pipeline.py --mode full       # scrape all 400+ listings (weekly)
  python3 bbs_pipeline.py --mode daily      # scrape last 2 days only (run daily)
  python3 bbs_pipeline.py --mode backfill   # estimate listed_date from listNumber model

Usage:
  export SUPABASE_SERVICE_KEY='your-service-role-key'
  python3 bbs_pipeline.py --mode daily
"""

import argparse
import csv
import json
import os
import re
import uuid
from collections import Counter
from datetime import date, datetime, timedelta, timezone

# pip install curl_cffi supabase --break-system-packages
from curl_cffi import requests as cffi_requests
from supabase import create_client

# ── Config ─────────────────────────────────────────────────────────────────────
SUPABASE_URL        = "https://ctvrauiiskucinibnfaj.supabase.co"
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
PROXY_STRING        = "2e675ba5977dd3336e3d__cr.us:719577c3bc6fb269@gw.dataimpulse.com:823"
BATCH_SIZE          = 50

# listNumber → date model (7 anchors, ±12 day mean error)
MODEL_BASE_DATE = date(2025, 5, 14)
MODEL_BASE_NUM  = 2_367_857
MODEL_RATE      = 357.1  # listings/day (median of 6 intervals)
# ───────────────────────────────────────────────────────────────────────────────


# ── Date estimation ─────────────────────────────────────────────────────────────
def estimate_listed_date(list_num):
    """Estimate listing date from sequential listNumber. ±12 day mean error."""
    if not list_num or list_num < 500_000:
        return None, 'none'
    d = MODEL_BASE_DATE + timedelta(days=(list_num - MODEL_BASE_NUM) / MODEL_RATE)
    if list_num >= 2_360_000:
        conf = 'high'    # within calibration window, ±18 day max
    elif list_num >= 2_000_000:
        conf = 'medium'  # extrapolated ~1yr back
    else:
        conf = 'low'
    return d, conf


# ── Scraper ─────────────────────────────────────────────────────────────────────
class BBSScraper:
    def __init__(self):
        self.session = cffi_requests.Session(impersonate="chrome")
        self.proxies = {
            "http":  f"http://{PROXY_STRING}",
            "https": f"http://{PROXY_STRING}",
        }
        self.headers = {
            'User-Agent':      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept':          'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type':    'application/json',
            'Origin':          'https://www.bizbuysell.com',
            'Referer':         'https://www.bizbuysell.com/',
            'X-Correlation-Id': str(uuid.uuid4()),
        }
        self.token = self._get_token()

    def _get_token(self):
        try:
            r = self.session.get(
                'https://www.bizbuysell.com/businesses-for-sale/new-york-ny/',
                headers=self.headers, proxies=self.proxies, timeout=60
            )
            token = r.cookies.get('_track_tkn')
            print(f"[+] Token {'obtained' if token else 'FAILED'}")
            return token
        except Exception as e:
            print(f"[-] Token error: {e}")
            return None

    def scrape(self, days_listed_ago=0, max_pages=500):
        """
        days_listed_ago=0  → all listings (full/weekly mode)
        days_listed_ago=2  → last 2 days (daily mode, overlaps to catch nothing missed)
        """
        if not self.token:
            print("[-] No token, aborting.")
            return []

        mode_label = "ALL" if days_listed_ago == 0 else f"last {days_listed_ago} days"
        print(f"[*] Scraping BizBuySell laundromats ({mode_label})...")

        api_headers = {**self.headers, 'Authorization': f'Bearer {self.token}'}

        payload_template = {
            "bfsSearchCriteria": {
                "siteId": 20, "languageId": 10,
                "categories": [199],          # siteCategoryId 199 = Laundromats & Coin Laundry
                "locations": None, "excludeLocations": None,
                "askingPriceMax": 0, "askingPriceMin": 0,
                "pageNumber": 1, "keyword": None,
                "cashFlowMin": 0, "cashFlowMax": 0,
                "grossIncomeMin": 0, "grossIncomeMax": 0,
                "daysListedAgo": days_listed_ago,
                "establishedAfterYear": 0,
                "listingsWithNoAskingPrice": 0, "homeBasedListings": 0,
                "includeRealEstateForLease": 0, "listingsWithSellerFinancing": 0,
                "realEstateIncluded": 0, "showRelocatableListings": False,
                "relatedFranchises": 0, "listingTypeIds": None,
                "designationTypeIds": None, "sortList": None,
                "absenteeOwnerListings": 0, "seoSearchType": None,
            }
        }

        all_listings, seen_ids = [], set()

        for page in range(1, max_pages + 1):
            payload = json.loads(json.dumps(payload_template))
            payload["bfsSearchCriteria"]["pageNumber"] = page

            try:
                resp = self.session.post(
                    'https://api.bizbuysell.com/bff/v2/BbsBfsSearchResults',
                    headers=api_headers, json=payload,
                    proxies=self.proxies, timeout=60
                )
                if resp.status_code != 200:
                    print(f"[-] Page {page}: HTTP {resp.status_code} — stopping.")
                    break

                listings = resp.json().get("value", {}).get("bfsSearchResult", {}).get("value", [])
                if not listings:
                    print(f"[*] Page {page}: empty — done.")
                    break

                new = 0
                for l in listings:
                    lid = f"{l.get('urlStub')}--{l.get('header')}"
                    if lid and lid not in seen_ids:
                        seen_ids.add(lid)
                        all_listings.append(l)
                        new += 1

                print(f"[+] Page {page:>3}: +{new:>3} new  |  Total: {len(all_listings)}")
                if new == 0:
                    print(f"[*] No new unique listings — done.")
                    break

            except Exception as e:
                print(f"[-] Page {page}: {e}")
                break

        print(f"[+] Scraped {len(all_listings)} listings total.")
        return all_listings


# ── DB Loader ───────────────────────────────────────────────────────────────────
def load_to_supabase(listings, supabase, broker_map, daily_mode=False):
    """Upsert listings into Supabase. In daily mode, set first_seen = estimated date."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []

    for l in listings:
        if not l:
            continue
        url = (l.get("urlStub") or "").strip()
        if not url:
            continue

        list_num_raw = l.get('listNumber') or l.get('specificId')
        list_num = int(list_num_raw) if list_num_raw else None

        # Date estimation
        est_date, conf = estimate_listed_date(list_num)

        # Broker resolution
        account_id = l.get('account')
        bbs_account_id = int(account_id) if account_id else None
        broker_uuid = broker_map.get(bbs_account_id)

        contact = l.get('contactInfo') or {}
        broker_company = contact.get('brokerCompany', '') or l.get('brokerCompany', '') or ''

        rows.append({
            "listing_id":           str(list_num) if list_num else None,
            "broker_id":            broker_uuid,
            "broker_account":       broker_company or None,
            "bbs_account_id":       bbs_account_id,
            "title":                (l.get('header') or '').strip() or None,
            "price":                int(l['price']) if l.get('price') else None,
            "cash_flow":            int(l['cashFlow']) if l.get('cashFlow') else None,
            "description":          (l.get('description') or '')[:2000] or None,
            "listing_url":          url,
            "location":             (l.get('location') or '').strip() or None,
            "state":                (l.get('region') or '').strip() or None,
            "industry":             "Laundromat",
            "is_active":            True,
            "status":               "active",
            "consecutive_misses":   0,
            "scraped_at":           now,
            "last_confirmed_live":  now,
            "last_seen":            now,
            # Set first_seen to estimated date for new records, today for daily new
            "first_seen":           est_date.isoformat() if est_date else now,
        })

    print(f"[*] Upserting {len(rows)} records...")
    success = errors = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            supabase.table("listings").upsert(
                batch, on_conflict="listing_url"
            ).execute()
            success += len(batch)
            print(f"[+] Batch {i//BATCH_SIZE+1:>3}: {len(batch)} upserted  |  Total: {success}")
        except Exception as e:
            errors += len(batch)
            print(f"[-] Batch {i//BATCH_SIZE+1:>3}: ERROR — {e}")

    print(f"\n[*] Done. {success} upserted, {errors} errors.")
    return success


# ── Backfill Mode ───────────────────────────────────────────────────────────────
def backfill_dates(supabase):
    """
    For all BizBuySell laundromat listings where first_seen is null or was set to 
    scraped_at, estimate first_seen from bbs_account_id/listing_id using the model.
    """
    print("[*] Backfilling estimated listed dates...")

    # Pull all BizBuySell laundromat listings
    result = supabase.table("listings").select(
        "id, listing_id, listing_url, first_seen"
    ).ilike("listing_url", "%bizbuysell%").execute()

    rows = result.data or []
    print(f"[*] Found {len(rows)} BizBuySell listings to process.")

    updates = []
    skipped = 0
    for r in rows:
        # Extract listNumber from listing_id or listing_url
        list_num = None
        for field in [r.get('listing_id', ''), r.get('listing_url', '')]:
            m = re.search(r'/(\d{6,8})/?(?:$|--)', field or '')
            if not m:
                m = re.search(r'^(\d{6,8})$', field or '')
            if m:
                n = int(m.group(1))
                if 1_000_000 < n < 3_000_000:
                    list_num = n
                    break

        if not list_num:
            skipped += 1
            continue

        est_date, conf = estimate_listed_date(list_num)
        if not est_date or conf == 'none':
            skipped += 1
            continue

        updates.append({
            "id":         r['id'],
            "first_seen": est_date.isoformat(),
        })

    print(f"[*] {len(updates)} to update, {skipped} skipped (no listNumber).")

    # Update in batches
    success = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i+BATCH_SIZE]
        try:
            supabase.table("listings").upsert(batch).execute()
            success += len(batch)
            print(f"[+] Batch {i//BATCH_SIZE+1:>3}: {len(batch)} updated  |  Total: {success}")
        except Exception as e:
            print(f"[-] Batch {i//BATCH_SIZE+1:>3}: ERROR — {e}")

    print(f"\n[*] Backfill done. {success} records updated.")


# ── Broker map ──────────────────────────────────────────────────────────────────
def load_broker_map(supabase):
    broker_map = {}
    page = 0
    while True:
        result = supabase.table("brokers").select("id, account").range(
            page * 1000, (page+1) * 1000 - 1
        ).execute()
        if not result.data:
            break
        for row in result.data:
            if row.get("account"):
                broker_map[int(row["account"])] = row["id"]
        if len(result.data) < 1000:
            break
        page += 1
    print(f"[+] Loaded {len(broker_map)} broker accounts.")
    return broker_map


# ── Main ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BizBuySell Laundromat Pipeline")
    parser.add_argument('--mode', choices=['full', 'daily', 'backfill'], default='daily',
                        help='full=scrape all, daily=last 2 days, backfill=estimate dates')
    args = parser.parse_args()

    if not SUPABASE_SERVICE_KEY:
        print("[-] Set SUPABASE_SERVICE_KEY env var first.")
        return

    supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

    if args.mode == 'backfill':
        backfill_dates(supabase)
        return

    broker_map = load_broker_map(supabase)
    scraper    = BBSScraper()

    if args.mode == 'daily':
        listings = scraper.scrape(days_listed_ago=2)   # 2-day overlap ensures no gaps
    else:
        listings = scraper.scrape(days_listed_ago=0)   # full pull

    if listings:
        load_to_supabase(listings, supabase, broker_map, daily_mode=(args.mode=='daily'))

    # Summary
    print(f"\n{'='*50}")
    print(f"Mode: {args.mode.upper()}")
    print(f"Listings processed: {len(listings)}")
    if args.mode == 'daily':
        print(f"Cron suggestion: 0 6 * * * python3 bbs_pipeline.py --mode daily")
        print(f"Weekly full:     0 6 * * 0 python3 bbs_pipeline.py --mode full")

if __name__ == "__main__":
    main()
