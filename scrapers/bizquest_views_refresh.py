#!/usr/bin/env python3
"""
BizQuest Views Refresh
======================
Pulls all active BizBuySell listings from DealLedger Supabase,
fetches current profileViews from the BizQuest detail API,
and writes listing_views back to Supabase.

Runs weekly via GitHub Actions (Sunday 6am ET).

Usage:
  export SUPABASE_URL='...'
  export SUPABASE_SERVICE_KEY='...'
  python3 bizquest_views_refresh.py

Requirements:
  pip install curl_cffi supabase colorama
"""

import os
import sys
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date

from curl_cffi import requests
from supabase import create_client

try:
    from colorama import Fore, Style, init
    init(autoreset=True)
except ImportError:
    class Fore:
        GREEN = CYAN = YELLOW = RED = WHITE = ''
    class Style:
        RESET_ALL = ''

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL         = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
PROXY                = "2e675ba5977dd3336e3d__cr.us:719577c3bc6fb269@gw.dataimpulse.com:823"
WORKERS              = 5
BATCH_SIZE           = 100
FETCH_LIMIT          = 1000  # Supabase page size


# ── BizQuest API client ───────────────────────────────────────────────────────
class BizQuestViews:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        self.proxies = {
            "http":  f"http://{PROXY}",
            "https": f"http://{PROXY}",
        }
        self.headers = {
            'User-Agent':        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Accept':            'application/json, text/plain, */*',
            'Accept-Language':   'en-US,en;q=0.9',
            'Accept-Encoding':   'gzip, deflate, br',
            'Content-Type':      'application/json',
            'Origin':            'https://www.bizquest.com',
            'Referer':           'https://www.bizquest.com/',
            'X-Correlation-Id':  str(uuid.uuid4()),
        }
        self.token = self._get_token()

    def _get_token(self):
        print(f"{Fore.CYAN}[*] Getting BizQuest auth token...")
        try:
            r = self.session.get(
                'https://www.bizquest.com/businesses-for-sale/',
                headers=self.headers,
                proxies=self.proxies,
                timeout=60
            )
            token = r.cookies.get('_track_tkn')
            print(f"{Fore.GREEN}[+] Token {'obtained' if token else 'FAILED'}")
            return token
        except Exception as e:
            print(f"{Fore.RED}[-] Token error: {e}")
            return None

    def get_views(self, url_stub):
        """Fetch profileViews for a single listing. Returns int or None."""
        if not self.token or not url_stub:
            return None

        api_headers = {**self.headers, 'Authorization': f'Bearer {self.token}'}

        # Strip domain — seoName needs just the path
        seo_name = url_stub
        if 'bizquest.com' in url_stub:
            seo_name = url_stub.split('bizquest.com')[1]

        payload = {
            "bfsSearchCriteria": {
                "siteId": 10,
                "languageId": 10,
                "queryString": "",
                "seoName": seo_name
            },
            "bfsSearchResultsCounts": 0,
            "cmsFilteredData": 0,
            "industriesFlat": 0,
            "industriesHierarchy": 0,
            "languageTypeId": 10,
            "rightRailBrokers": 0,
            "statesRegions": 0
        }

        try:
            r = self.session.post(
                'https://api.bizbuysell.com/bff/v1/BqBfsListingDetail',
                headers=api_headers,
                json=payload,
                proxies=self.proxies,
                timeout=30
            )
            if r.status_code == 200:
                data = r.json()
                if data.get("status") == 1:
                    detail = data.get("value", {}).get("listingDetail") or {}
                    return detail.get("profileViews")
        except Exception:
            pass
        return None


# ── Supabase helpers ──────────────────────────────────────────────────────────
def fetch_active_listings(sb):
    """Pull all active BizBuySell listings with a URL from Supabase."""
    print(f"{Fore.CYAN}[*] Fetching active listings from Supabase...")
    all_rows = []
    offset = 0
    while True:
        res = sb.table('listings')\
            .select('listing_number, url')\
            .eq('is_active', True)\
            .eq('source', 'bizbuysell')\
            .not_.is_('url', 'null')\
            .limit(FETCH_LIMIT)\
            .offset(offset)\
            .execute()
        rows = res.data or []
        all_rows.extend(rows)
        print(f"  Fetched {len(all_rows):,}...", end='\r')
        if len(rows) < FETCH_LIMIT:
            break
        offset += FETCH_LIMIT
    print(f"{Fore.GREEN}[+] {len(all_rows):,} active listings loaded{' '*20}")
    return all_rows


def write_views_batch(sb, updates):
    """Write a batch of {listing_number, listing_views} back to Supabase."""
    ok = err = 0
    for i in range(0, len(updates), BATCH_SIZE):
        batch = updates[i:i+BATCH_SIZE]
        for row in batch:
            try:
                sb.table('listings')\
                    .update({'listing_views': row['views']})\
                    .eq('listing_number', row['listing_number'])\
                    .execute()
                ok += 1
            except Exception as e:
                err += 1
    return ok, err


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    if not SUPABASE_SERVICE_KEY:
        print(f"{Fore.RED}[-] Set SUPABASE_SERVICE_KEY env var first.")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    print(f"{Fore.GREEN}[+] Connected to DealLedger Supabase")

    bq = BizQuestViews()
    if not bq.token:
        print(f"{Fore.RED}[-] No BizQuest token. Exiting.")
        sys.exit(1)

    # Pull active listings
    listings = fetch_active_listings(sb)
    total = len(listings)
    print(f"{Fore.GREEN}[+] Processing {total:,} listings\n")

    # Threaded view fetching
    start = time.time()
    updates = []
    fetched = skipped = 0

    def fetch_one(row):
        views = bq.get_views(row['url'])
        time.sleep(0.3)  # be polite
        return row['listing_number'], views

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(fetch_one, row): row for row in listings}
        for i, future in enumerate(as_completed(futures), 1):
            listing_number, views = future.result()
            if views is not None:
                updates.append({'listing_number': listing_number, 'views': views})
                fetched += 1
            else:
                skipped += 1

            # Progress
            if i % 500 == 0 or i == total:
                elapsed = time.time() - start
                rate = i / elapsed * 60
                remaining = (total - i) / rate if rate > 0 else 0
                print(f"  {i:>6,}/{total:,} | {rate:.0f}/min | ~{remaining:.0f} min left | {fetched} with views")

            # Write to Supabase in batches of 500 to avoid memory buildup
            if len(updates) >= 500:
                ok, err = write_views_batch(sb, updates)
                print(f"  {Fore.GREEN}[DB] Wrote {ok} views | Errors: {err}")
                updates = []

    # Final write
    if updates:
        ok, err = write_views_batch(sb, updates)
        print(f"  {Fore.GREEN}[DB] Final write: {ok} views | Errors: {err}")

    elapsed_min = (time.time() - start) / 60
    print(f"\n{Fore.GREEN}{'='*50}")
    print(f"{Fore.GREEN}  DONE in {elapsed_min:.1f} minutes")
    print(f"{Fore.GREEN}  Listings processed: {total:,}")
    print(f"{Fore.GREEN}  Views fetched:      {fetched:,}")
    print(f"{Fore.GREEN}  No view data:       {skipped:,}")
    print(f"{Fore.GREEN}{'='*50}")


if __name__ == "__main__":
    main()
