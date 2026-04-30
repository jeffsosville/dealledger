#!/usr/bin/env python3
"""
BizBuySell All-States Full Market Scraper
==========================================
Scrapes all listings from BizBuySell by iterating through all 51 states/regions.
Bypasses the 10k per-query cap by doing one state at a time.
Writes directly to DealLedger Supabase.

Usage:
  export SUPABASE_SERVICE_KEY='your-service-role-key'
  python3 bbs_allstates.py              # full run (resumes if interrupted)
  python3 bbs_allstates.py fresh        # fresh start, ignore completed states
  python3 bbs_allstates.py states TX,CA,NY  # specific states only

Requirements:
  pip install curl_cffi supabase colorama
"""

import json
import os
import re
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

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
PROXY = "2e675ba5977dd3336e3d:39cd7cb8adc0d68f@gw.dataimpulse.com:823"
BATCH_SIZE           = 100
STATE_FILES_DIR      = Path("state_files")
STALE_DAYS           = 14  # deactivate listings not seen in this many days

# ── Date model ─────────────────────────────────────────────────────────────────
ANCHOR_NUM  = 2_367_857
ANCHOR_DATE = date(2025, 5, 14)
RATE        = 373.8  # listings/day
TODAY       = date.today()

def estimate_listed_date(list_num):
    try:
        n = int(list_num)
        if not (1_000_000 < n < 3_500_000):
            return None
        est = ANCHOR_DATE + timedelta(days=(n - ANCHOR_NUM) / RATE)
        if est > TODAY: est = TODAY
        if est < date(2010, 1, 1): return None
        return est
    except:
        return None

# ── State map ──────────────────────────────────────────────────────────────────
STATE_REGION_IDS = {
    "AK": 1,  "AL": 2,  "AR": 3,  "AZ": 4,  "CA": 5,
    "CO": 6,  "CT": 7,  "DC": 8,  "DE": 9,  "FL": 10,
    "GA": 11, "HI": 12, "IA": 13, "ID": 14, "IL": 15,
    "IN": 16, "KS": 17, "KY": 18, "LA": 19, "MA": 20,
    "MD": 21, "ME": 22, "MI": 23, "MN": 24, "MO": 25,
    "MS": 26, "MT": 27, "NC": 28, "ND": 29, "NE": 30,
    "NH": 31, "NJ": 32, "NM": 33, "NV": 34, "NY": 35,
    "OH": 36, "OK": 37, "OR": 38, "PA": 39, "RI": 40,
    "SC": 41, "SD": 42, "TN": 43, "TX": 44, "UT": 45,
    "VA": 46, "VT": 47, "WA": 48, "WI": 49, "WV": 50,
    "WY": 51,
}

# ── Scraper ────────────────────────────────────────────────────────────────────
class BBSAllStatesScraper:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome")
        self.proxies = {
            "http":  f"http://{PROXY}",
            "https": f"http://{PROXY}",
        }
        self.headers = {
            'User-Agent':       'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept':           'application/json, text/plain, */*',
            'Accept-Language':  'en-US,en;q=0.9',
            'Content-Type':     'application/json',
            'Origin':           'https://www.bizbuysell.com',
            'Referer':          'https://www.bizbuysell.com/',
            'X-Correlation-Id': str(uuid.uuid4()),
        }
        self.token = self._get_token()

    def _get_token(self):
        print(f"{Fore.CYAN}[*] Getting auth token...")
        try:
            r = self.session.get(
                'https://www.bizbuysell.com/businesses-for-sale/new-york-ny/',
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

    def scrape_state(self, state_code, region_id, max_pages=200):
        """Scrape all listings for one state. Stops when pages run dry."""
        if not self.token:
            return []

        api_headers = {**self.headers, 'Authorization': f'Bearer {self.token}'}
        all_listings = []
        seen_ids = set()
        empty_streak = 0

        for page in range(1, max_pages + 1):
            payload = {
                "bfsSearchCriteria": {
                    "siteId": 20,
                    "languageId": 10,
                    "categories": None,
                    "locations": [{
                        "geoType": 20,
                        "regionId": region_id,
                        "countryCode": "US",
                        "countryId": "US",
                        "stateCode": state_code
                    }],
                    "excludeLocations": None,
                    "askingPriceMax": 0, "askingPriceMin": 0,
                    "pageNumber": page,
                    "keyword": None,
                    "cashFlowMin": 0, "cashFlowMax": 0,
                    "grossIncomeMin": 0, "grossIncomeMax": 0,
                    "daysListedAgo": 0,
                    "establishedAfterYear": 0,
                    "listingsWithNoAskingPrice": 0,
                    "homeBasedListings": 0,
                    "includeRealEstateForLease": 0,
                    "listingsWithSellerFinancing": 0,
                    "realEstateIncluded": 0,
                    "showRelocatableListings": False,
                    "relatedFranchises": 0,
                    "listingTypeIds": None,
                    "designationTypeIds": None,
                    "sortList": None,
                    "absenteeOwnerListings": 0,
                    "seoSearchType": None,
                }
            }

            try:
                resp = self.session.post(
                    'https://api.bizbuysell.com/bff/v2/BbsBfsSearchResults',
                    headers=api_headers,
                    json=payload,
                    proxies=self.proxies,
                    timeout=60
                )
                if resp.status_code != 200:
                    print(f"  {Fore.RED}Page {page}: HTTP {resp.status_code} — stopping")
                    break

                listings = resp.json().get("value", {}).get("bfsSearchResult", {}).get("value", [])
                if not listings:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break
                    continue

                empty_streak = 0
                new = 0
                for l in listings:
                    lid = l.get('listNumber') or l.get('specificId')
                    if lid and str(lid) not in seen_ids:
                        seen_ids.add(str(lid))
                        l['_state'] = state_code
                        all_listings.append(l)
                        new += 1

                print(f"  Page {page:>3}: +{new:>3} | Total: {len(all_listings)}", end='\r')
                if new == 0:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break

            except Exception as e:
                print(f"  {Fore.RED}Page {page}: {e}")
                break

        print(f"  {Fore.GREEN}{state_code}: {len(all_listings)} listings scraped{' '*20}")
        return all_listings


# ── Normalize for Supabase ─────────────────────────────────────────────────────
def normalize(listing, state_code):
    url = (listing.get('urlStub') or '').strip()
    if not url:
        return None

    list_num = listing.get('listNumber') or listing.get('specificId')
    try:
        list_num = int(list_num) if list_num else None
    except:
        list_num = None

    est_date = estimate_listed_date(list_num) if list_num else None
    dom = (TODAY - est_date).days if est_date else None

    contact = listing.get('contactInfo') or {}

    # Extract phone safely from nested object
    phone_obj = contact.get('contactPhoneNumber') or {}
    contact_phone = phone_obj.get('telephone') if isinstance(phone_obj, dict) else None

    # bbs_account_id: numeric broker account — null means FSBO
    raw_account = listing.get('account')
    try:
        bbs_account_id = int(raw_account) if raw_account else None
    except:
        bbs_account_id = None

    return {
        'listing_number':        list_num,
        'source':                'bizbuysell',
        'header':                (listing.get('header') or '')[:500] or None,
        'price':                 int(listing['price']) if listing.get('price') else None,
        'cash_flow':             int(listing['cashFlow']) if listing.get('cashFlow') else None,
        'state':                 state_code,
        'broker_account':        (contact.get('brokerCompany') or listing.get('brokerCompany') or '')[:100] or None,
        'bbs_account_id':        bbs_account_id,
        'contact_name':          (contact.get('contactFullName') or listing.get('brokerContactFullName') or '')[:100] or None,
        'contact_phone':         contact_phone,
        'listing_views':         int(listing['profileViews']) if listing.get('profileViews') else None,
        'url':                   url[:500],
        'first_seen':            TODAY.isoformat(),
        'last_seen':             TODAY.isoformat(),
        'estimated_listed_date': est_date.isoformat() if est_date else None,
        'days_on_market':        dom,
        'price_reduced':         str(listing.get('listingPriceReduced', '')).upper() == 'TRUE',
        'is_active':             True,
    }


# ── Supabase upsert ────────────────────────────────────────────────────────────
def upsert_to_supabase(sb, records):
    ok = err = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        try:
            sb.table('listings').upsert(batch, on_conflict='listing_number').execute()
            ok += len(batch)
        except Exception as e:
            err += len(batch)
            print(f"  {Fore.RED}Upsert error: {e}")
    return ok, err


# ── Deactivate stale listings ──────────────────────────────────────────────────
def deactivate_stale(sb):
    cutoff = str(TODAY - timedelta(days=STALE_DAYS))
    print(f"\n{Fore.YELLOW}[*] Deactivating listings not seen since {cutoff}...")
    try:
        sb.table('listings')\
            .update({'is_active': False})\
            .eq('is_active', True)\
            .lt('last_seen', cutoff)\
            .execute()
        sb.table('listings')\
            .update({'is_active': False})\
            .eq('is_active', True)\
            .is_('last_seen', 'null')\
            .execute()
        print(f"{Fore.GREEN}[+] Stale listings deactivated (cutoff: {cutoff})")
    except Exception as e:
        print(f"{Fore.RED}[-] Deactivate error: {e}")


# ── State file helpers ─────────────────────────────────────────────────────────
def get_completed_states():
    STATE_FILES_DIR.mkdir(exist_ok=True)
    return {f.stem.replace('bbs_', '').upper() for f in STATE_FILES_DIR.glob('bbs_*.json')}

def save_state_file(state_code, listings):
    STATE_FILES_DIR.mkdir(exist_ok=True)
    path = STATE_FILES_DIR / f"bbs_{state_code}.json"
    with open(path, 'w') as f:
        json.dump(listings, f)


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    mode = 'resume'
    specific_states = []
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == 'fresh':
            mode = 'fresh'
            print(f"{Fore.YELLOW}[*] FRESH mode — starting over")
        elif arg == 'states' and len(sys.argv) > 2:
            specific_states = [s.strip().upper() for s in sys.argv[2].split(',')]
            print(f"{Fore.CYAN}[*] Specific states: {', '.join(specific_states)}")

    if not SUPABASE_SERVICE_KEY:
        print(f"{Fore.RED}[-] Set SUPABASE_SERVICE_KEY env var first.")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)
    print(f"{Fore.GREEN}[+] Connected to DealLedger Supabase")

    scraper = BBSAllStatesScraper()
    if not scraper.token:
        print(f"{Fore.RED}[-] No token. Exiting.")
        sys.exit(1)

    completed = get_completed_states() if mode == 'resume' else set()
    if completed:
        print(f"{Fore.YELLOW}[*] Already completed: {', '.join(sorted(completed))}")

    if specific_states:
        states_to_run = [(c, r) for c, r in STATE_REGION_IDS.items() if c in specific_states]
    elif mode == 'fresh':
        states_to_run = list(STATE_REGION_IDS.items())
        for f in STATE_FILES_DIR.glob('bbs_*.json'):
            f.unlink()
    else:
        states_to_run = [(c, r) for c, r in STATE_REGION_IDS.items() if c not in completed]

    total = len(states_to_run)
    print(f"{Fore.GREEN}[+] States to scrape: {total}\n")

    grand_total = ok_total = err_total = 0

    for i, (state_code, region_id) in enumerate(sorted(states_to_run), 1):
        print(f"{Fore.CYAN}[{i}/{total}] {state_code}...")
        listings = scraper.scrape_state(state_code, region_id)

        if not listings:
            print(f"  {Fore.YELLOW}No listings for {state_code}")
            save_state_file(state_code, [])
            continue

        records = [r for l in listings if (r := normalize(l, state_code))]
        ok, err = upsert_to_supabase(sb, records)
        ok_total += ok
        err_total += err
        grand_total += len(listings)

        save_state_file(state_code, listings)
        print(f"  {Fore.GREEN}✓ {state_code}: {len(listings)} scraped → {ok} upserted | Running total: {grand_total}")

    deactivate_stale(sb)

    result = sb.table('listings').select('id', count='exact').eq('is_active', True).execute()
    print(f"\n{Fore.GREEN}{'='*50}")
    print(f"{Fore.GREEN}  DONE")
    print(f"{Fore.GREEN}  States scraped:    {total}")
    print(f"{Fore.GREEN}  Listings scraped:  {grand_total:,}")
    print(f"{Fore.GREEN}  Upserted:          {ok_total:,} | Errors: {err_total:,}")
    print(f"{Fore.GREEN}  Active in DB:      {result.count:,}")
    print(f"{Fore.GREEN}{'='*50}")


if __name__ == "__main__":
    main()
