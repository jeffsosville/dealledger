from curl_cffi import requests
import json
import csv
import uuid
import glob
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

class BizQuestScraper:
    def __init__(self, proxy=None):
        self.session = requests.Session(impersonate="chrome")

        self.proxies = None
        if proxy:
            proxy_string = proxy
            self.proxies = {
                "http": f"http://{proxy_string}" if not proxy_string.startswith("http") else proxy_string,
                "https": f"http://{proxy_string}" if not proxy_string.startswith("http") else proxy_string
            }
            print("[*] Using proxy: (configured)")

        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Sec-Ch-Ua': '"Chromium";v="135", "Not-A.Brand";v="8"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Site': 'same-site',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Origin': 'https://www.bizquest.com',
            'Referer': 'https://www.bizquest.com/',
            'Content-Type': 'application/json',
            'X-Correlation-Id': str(uuid.uuid4())
        }
        self.token = None
        self.states = []
        self.get_auth_token()

    def get_auth_token(self):
        """Get authentication token from BizQuest."""
        print("[*] Obtaining authentication token...")
        try:
            response = self.session.get(
                'https://www.bizquest.com/businesses-for-sale/',
                headers=self.headers,
                proxies=self.proxies,
                timeout=60
            )

            cookies = response.cookies
            self.token = cookies.get('_track_tkn')

            if self.token:
                print(f"[+] Token obtained successfully")
            else:
                print(f"[-] Failed to get token")

        except Exception as e:
            print(f"[-] Error obtaining token: {str(e)}")

    def get_all_states(self):
        """Fetch all US states from the API."""
        if not self.token:
            print("[-] No authentication token available.")
            return []

        print("[*] Fetching state data from API...")

        api_headers = self.headers.copy()
        api_headers['Authorization'] = f'Bearer {self.token}'

        # This is the key - get the actual state objects from their API
        payload = {
            "siteId": 10,  # 10 = BizQuest
            "languageId": 10,
            "query": "",
            "geographyTypes": [20]  # 20 = US States
        }

        try:
            response = self.session.post(
                'https://api.bizbuysell.com/resource/v2/Regions',
                headers=api_headers,
                json=payload,
                proxies=self.proxies,
                timeout=60
            )

            if response.status_code == 200:
                data = response.json()
                states = data.get("value", [])
                print(f"[+] Retrieved {len(states)} states from API")

                # Debug - show first state structure
                if states:
                    print(f"[DEBUG] Sample state object: {json.dumps(states[0], indent=2)}")

                self.states = states
                return states
            else:
                print(f"[-] Failed to get states. Status: {response.status_code}")
                print(f"[-] Response: {response.text[:500]}")
                return []

        except Exception as e:
            print(f"[-] Error fetching states: {str(e)}")
            return []

    def flatten_listing(self, listing):
        """Flatten nested structures for CSV export."""
        flattened = listing.copy()

        contact_info = listing.get('contactInfo') or {}

        flattened['contactInfoPersonId'] = contact_info.get('contactInfoPersonId', '')
        flattened['contactFullName'] = contact_info.get('contactFullName', '')
        flattened['contactPhoto'] = contact_info.get('contactPhoto', '')
        flattened['brokerCompany'] = contact_info.get('brokerCompany', '')
        flattened['brokerProfileUrl'] = contact_info.get('brokerProfileUrl', '')

        contact_phone = contact_info.get('contactPhoneNumber') if contact_info else None
        if contact_phone:
            flattened['contactTelephone'] = contact_phone.get('telephone', '')
            flattened['contactTpnPhone'] = contact_phone.get('tpnPhone', '')
            flattened['contactTpnPhoneExt'] = contact_phone.get('tpnPhoneExt', '')
        else:
            flattened['contactTelephone'] = ''
            flattened['contactTpnPhone'] = ''
            flattened['contactTpnPhoneExt'] = ''

        flattened['accountId'] = listing.get('account', '')
        flattened['isFSBO'] = 1 if not flattened['brokerCompany'] else 0

        return flattened

    def get_listing_detail(self, url_stub):
        """Get full listing detail including profileViews."""
        api_headers = self.headers.copy()
        api_headers['Authorization'] = f'Bearer {self.token}'

        # Strip domain - seoName needs just the path
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
            response = self.session.post(
                'https://api.bizbuysell.com/bff/v1/BqBfsListingDetail',
                headers=api_headers,
                json=payload,
                proxies=self.proxies,
                timeout=60
            )

            if response.status_code == 200:
                data = response.json()
                if data.get("status") != 1:
                    return None
                return data.get("value", {}).get("listingDetail")
        except Exception as e:
            pass

        return None

    def scrape_listings_for_state(self, state_obj, max_pages=200, workers=3):
        """Scrape business listings for a specific state using the state object from API."""
        if not self.token:
            print("[-] No token. Cannot proceed.")
            return []

        state_name = state_obj.get('regionName', 'Unknown')
        state_code = state_obj.get('stateCode', 'XX')

        api_headers = self.headers.copy()
        api_headers['Authorization'] = f'Bearer {self.token}'

        # Use the full state object as the location filter
        payload_template = {
            "bfsSearchCriteria": {
                "siteId": 10,  # BizQuest
                "languageId": 10,
                "categories": None,
                "bizQuestCategories": None,  # All categories
                "locations": [state_obj],  # Pass the FULL state object from API
                "excludeLocations": None,
                "askingPriceMax": 0,
                "askingPriceMin": 0,
                "pageNumber": 1,
                "keyword": None,
                "cashFlowMin": 0,
                "cashFlowMax": 0,
                "grossIncomeMin": 0,
                "grossIncomeMax": 0,
                "daysListedAgo": 0,  # All time
                "establishedAfterYear": 0,
                "listingsWithNoAskingPrice": 0,
                "homeBasedListings": 0,
                "includeRealEstateForLease": 0,
                "listingsWithSellerFinancing": 0,
                "realEstateIncluded": 0,
                "showRelocatableListings": False,
                "relatedFranchises": 0,
                "listingTypeIds": [30, 40, 80],
                "designationTypeIds": None,
                "sortList": None,
                "absenteeOwnerListings": 0,
                "seoSearchType": None
            },
            "bfsSearchResultsCounts": 0,
            "cmsFilteredData": 0,
            "industriesFlat": 10,
            "industriesHierarchy": 10,
            "languageTypeId": 10,
            "rightRailBrokers": 0,
            "statesRegions": 10
        }

        all_listings = []
        listing_ids = set()
        lock = Lock()
        total_for_state = [0]

        def fetch_page(page_number):
            payload = json.loads(json.dumps(payload_template))
            payload["bfsSearchCriteria"]["pageNumber"] = page_number

            try:
                response = self.session.post(
                    'https://api.bizbuysell.com/bff/v1/BqBfsSearchResults',
                    headers=api_headers,
                    json=payload,
                    proxies=self.proxies,
                    timeout=60
                )

                if response.status_code == 200:
                    data = response.json()
                    result = data.get("value", {}).get("bfsSearchResult", {})
                    listings = result.get("value", [])
                    total = result.get("total", 0)

                    with lock:
                        if page_number == 1:
                            total_for_state[0] = total

                    new_listings = []
                    with lock:
                        for listing in listings:
                            listing_id = listing.get('listNumber')
                            if listing_id and listing_id not in listing_ids:
                                listing_ids.add(listing_id)
                                listing['scraped_state'] = state_code
                                new_listings.append(listing)
                    return new_listings
            except Exception as e:
                pass
            return []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch_page, page) for page in range(1, max_pages + 1)]
            for future in as_completed(futures):
                page_listings = future.result()
                if page_listings:
                    all_listings.extend(page_listings)

        available = total_for_state[0]
        scraped = len(all_listings)
        flag = ""
        if available and scraped < available * 0.95:
            flag = f"  [!] SHORT by {available - scraped} — raise max_pages"
        print(f"    [{state_code}] {available} available, {scraped} scraped{flag}")
        return all_listings

    def scrape_all_states(self, max_pages_per_state=100, workers=3, output_dir="bizquest_states"):
        """Scrape all 50 states + DC using API-provided state data."""
        if not self.token:
            print("[-] No token. Cannot proceed.")
            return []

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Get states from API
        states = self.get_all_states()
        if not states:
            print("[-] Failed to get states from API. Exiting.")
            return []

        all_listings = []
        global_listing_ids = set()

        print(f"\n[*] Scraping {len(states)} states...")
        print(f"[*] Max pages per state: {max_pages_per_state}")
        print(f"[*] Workers: {workers}")
        print()

        for i, state_obj in enumerate(states, 1):
            state_code = state_obj.get('stateCode', 'XX')
            state_name = state_obj.get('regionName', 'Unknown')

            print(f"[{i}/{len(states)}] Scraping {state_name} ({state_code})...")

            state_listings = self.scrape_listings_for_state(
                state_obj,
                max_pages=max_pages_per_state,
                workers=workers
            )

            # Save state file
            if state_listings:
                state_file = os.path.join(output_dir, f"bizquest_{state_code}.json")
                with open(state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_listings, f, indent=2)

            # Dedupe against global list
            new_count = 0
            for listing in state_listings:
                lid = listing.get('listNumber')
                if lid and lid not in global_listing_ids:
                    global_listing_ids.add(lid)
                    all_listings.append(listing)
                    new_count += 1

            print(f"    [+] {new_count} new unique listings (Global total: {len(all_listings)})")
            time.sleep(0.5)  # Be nice between states

        return all_listings

    def enrich_with_views(self, listings, workers=5):
        """Add profileViews to listings."""
        print(f"\n[*] Enriching {len(listings)} listings with view counts...")
        print(f"[*] Using {workers} workers")

        start_time = time.time()

        def fetch_detail(listing):
            url_stub = listing.get('urlStub')
            if url_stub:
                detail = self.get_listing_detail(url_stub)
                if detail:
                    listing['profileViews'] = detail.get('profileViews', 0)
                    listing['employees'] = detail.get('employees', '')
                    listing['yearEstablished'] = detail.get('yearEstablished', '')
                    listing['grossIncome'] = detail.get('grossIncome', '')
                    listing['summary'] = detail.get('summary', '')
                    listing['bizType'] = detail.get('primaryBizTypeId', '')
                else:
                    listing['profileViews'] = None
            time.sleep(0.3)
            return listing

        enriched = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(fetch_detail, l): l for l in listings}
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                enriched.append(result)
                if (i + 1) % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed * 60
                    remaining = (len(listings) - i - 1) / rate
                    print(f"[+] Enriched {i + 1}/{len(listings)} ({rate:.0f}/min, ~{remaining:.1f} min remaining)")

        print(f"[+] Enriched {len(enriched)}/{len(listings)} in {(time.time() - start_time)/60:.1f} minutes")
        return enriched

    def save_results(self, listings, filename="bizquest_all.json"):
        """Save results to JSON file."""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(listings, f, indent=2)
        print(f"[+] Saved {len(listings)} listings to {filename}")

    def save_to_csv(self, listings, filename="bizquest_all.csv"):
        """Save results to CSV file."""
        if not listings:
            return

        flattened_listings = [self.flatten_listing(listing) for listing in listings]

        columns = [
            'listNumber', 'header', 'location', 'region', 'scraped_state', 'price', 'cashFlow',
            'grossIncome', 'ebitda', 'description', 'urlStub', 'listingTypeId', 'adLevelId',
            'profileViews', 'employees', 'yearEstablished', 'bizType',
            'contactInfoPersonId', 'contactFullName', 'contactTelephone',
            'contactTpnPhone', 'contactTpnPhoneExt', 'contactPhoto',
            'brokerCompany', 'brokerProfileUrl', 'accountId', 'isFSBO',
            'recentlyAdded', 'recentlyUpdated', 'hotProperty',
            'activeListingsCount', 'soldListingsCount', 'financingTypeId',
            'summary'
        ]

        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
            writer.writeheader()

            for listing in flattened_listings:
                row = {col: listing.get(col, '') for col in columns}
                writer.writerow(row)

        print(f"[+] Saved to {filename}")

    # ------------------------------------------------------------------
    # Supabase
    # ------------------------------------------------------------------

    @staticmethod
    def _clean_view_rows(listings):
        """Return {listing_number: views} for valid BW-range listings."""
        seen = {}
        for l in listings:
            ln = l.get("listNumber")
            pv = l.get("profileViews")
            if ln is None or pv is None:
                continue
            try:
                ln = int(ln)
                pv = int(pv)
            except (TypeError, ValueError):
                continue
            if 1_000_000 <= ln <= 2_999_999:
                seen[ln] = pv
        return seen

    def save_to_supabase(self, listings, observed_at=None):
        """Append a DATED views snapshot to listing_views_history.

        Also refreshes listings.listing_views for convenience, but the
        history table is the source of truth: it never overwrites, so
        every run adds a new layer and velocity stays computable.
        """
        import os
        from datetime import date, timezone, datetime
        from supabase import create_client

        url = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
        key = os.environ.get("SUPABASE_SERVICE_KEY", "")
        if not key:
            print("[-] SUPABASE_SERVICE_KEY not set — skipping DB write")
            return

        sb = create_client(url, key)
        observed_at = observed_at or datetime.now(timezone.utc).date().isoformat()

        seen = self._clean_view_rows(listings)
        if not seen:
            print("[-] No valid view rows to write")
            return

        print(f"\n[*] Writing {len(seen):,} dated view rows for {observed_at}...")

        BATCH = 500

        # 1. Dated history — append-only, never destructive.
        history = [
            {"listing_number": ln, "observed_at": observed_at,
             "views": v, "source": "bizquest"}
            for ln, v in seen.items()
        ]

        written = failed = 0
        for i in range(0, len(history), BATCH):
            batch = history[i:i + BATCH]
            try:
                sb.table("listing_views_history") \
                  .upsert(batch, on_conflict="listing_number,observed_at,source") \
                  .execute()
                written += len(batch)
            except Exception as e:
                failed += len(batch)
                print(f"\n  [!] history batch {i // BATCH} failed: {e}")
            print(f"  [{written:,} written, {failed:,} failed]", end="\r")

        print(f"\n[+] listing_views_history: {written:,} written | {failed:,} failed")

        # 2. Current value on listings — update only, never insert, so
        #    BizQuest-only listings don't create stub rows.
        updated = errors = 0
        for ln, v in seen.items():
            try:
                sb.table("listings") \
                  .update({"listing_views": v}) \
                  .eq("listing_number", ln) \
                  .execute()
                updated += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"\n  [!] listings update failed for {ln}: {e}")
            if updated % 1000 == 0 and updated:
                print(f"  [listings: {updated:,} updated]", end="\r")

        print(f"\n[+] listings.listing_views: {updated:,} updated | {errors:,} errors")

    def record_dom_anchor(self, listings, observed_at=None):
        """Record today's max listing number as a DOM calibration anchor."""
        import os
        from datetime import datetime, timezone
        from supabase import create_client

        key = os.environ.get("SUPABASE_SERVICE_KEY", "")
        if not key:
            return

        url = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
        sb = create_client(url, key)
        observed_at = observed_at or datetime.now(timezone.utc).date().isoformat()

        nums = [ln for ln in self._clean_view_rows(listings)]
        if not nums:
            nums = []
            for l in listings:
                try:
                    ln = int(l.get("listNumber"))
                except (TypeError, ValueError):
                    continue
                if 1_000_000 <= ln <= 2_999_999:
                    nums.append(ln)
        if not nums:
            print("[-] No valid listing numbers for DOM anchor")
            return

        ceiling = max(nums)
        try:
            sb.table("dom_anchors").upsert(
                [{"anchor_date": observed_at,
                  "listing_number": ceiling,
                  "confidence": "daily"}],
                on_conflict="anchor_date"
            ).execute()
            print(f"[+] DOM anchor recorded: {observed_at} = {ceiling:,}")
        except Exception as e:
            print(f"[-] DOM anchor write failed: {e}")


def combine_state_files(input_dir="bizquest_states", output_file="bizquest_combined.json"):
    """Combine all state JSON files into one."""
    combined = []
    listing_ids = set()

    for file in sorted(glob.glob(os.path.join(input_dir, "bizquest_*.json"))):
        print(f"[*] Loading: {file}")
        with open(file, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
                for listing in data:
                    listing_id = listing.get('listNumber')
                    if listing_id and listing_id not in listing_ids:
                        listing_ids.add(listing_id)
                        combined.append(listing)
            except json.JSONDecodeError as e:
                print(f"[-] Error decoding {file}: {e}")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(combined, f, indent=2)

    print(f"\n[+] Combined {len(combined)} unique listings into {output_file}")
    return combined


if __name__ == "__main__":
    import sys

    print("=" * 60)
    print("BizQuest Full Marketplace Scraper (API State Data)")
    print("=" * 60)

    # Configuration
    PROXY = os.environ.get("PROXY_URL")
    MAX_PAGES_PER_STATE = int(os.environ.get("MAX_PAGES_PER_STATE", "150"))
    WORKERS = int(os.environ.get("WORKERS", "5"))
    ENRICH = os.environ.get("ENRICH", "1") != "0"
    WRITE_DB = os.environ.get("WRITE_DB", "1") != "0"

    if not PROXY:
        print("[!] PROXY_URL not set — running direct (may be blocked)")

    # Parse command line args
    if len(sys.argv) > 1:
        if sys.argv[1] == "combine":
            listings = combine_state_files()
            sys.exit(0)
        elif sys.argv[1] == "enrich":
            scraper = BizQuestScraper(PROXY)
            with open("bizquest_combined.json", 'r') as f:
                listings = json.load(f)
            print(f"[*] Loaded {len(listings)} listings")
            listings = scraper.enrich_with_views(listings, workers=WORKERS)
            listings.sort(key=lambda x: x.get('profileViews') or 0, reverse=True)
            scraper.save_results(listings, "bizquest_enriched.json")
            scraper.save_to_csv(listings, "bizquest_enriched.csv")
            if WRITE_DB:
                scraper.save_to_supabase(listings)
                scraper.record_dom_anchor(listings)
            sys.exit(0)

    print(f"\n[*] Configuration:")
    print(f"    Max pages per state: {MAX_PAGES_PER_STATE}")
    print(f"    Workers: {WORKERS}")
    print(f"    Enrich with views: {ENRICH}")
    print(f"    Write to Supabase: {WRITE_DB}")
    print()

    # Create scraper
    scraper = BizQuestScraper(PROXY)

    if not scraper.token:
        print("[-] Failed to get token. Exiting.")
        sys.exit(1)

    # Scrape all states
    listings = scraper.scrape_all_states(
        max_pages_per_state=MAX_PAGES_PER_STATE,
        workers=WORKERS,
        output_dir="bizquest_states"
    )

    if listings:
        # Save raw
        scraper.save_results(listings, "bizquest_raw.json")
        scraper.save_to_csv(listings, "bizquest_raw.csv")

        # Record the DOM anchor from the raw crawl — this is the true
        # ceiling for today regardless of whether enrichment succeeds.
        if WRITE_DB:
            scraper.record_dom_anchor(listings)

        # Enrich
        if ENRICH:
            listings = scraper.enrich_with_views(listings, workers=WORKERS)
            listings.sort(key=lambda x: x.get('profileViews') or 0, reverse=True)
            scraper.save_results(listings, "bizquest_enriched.json")
            scraper.save_to_csv(listings, "bizquest_enriched.csv")
            if WRITE_DB:
                scraper.save_to_supabase(listings)

        # Stats
        print(f"\n{'=' * 60}")
        print("FINAL STATS")
        print(f"{'=' * 60}")
        print(f"Total unique listings: {len(listings)}")

        # State breakdown
        state_counts = {}
        for l in listings:
            st = l.get('scraped_state', 'UNK')
            state_counts[st] = state_counts.get(st, 0) + 1

        print(f"\nListings by state:")
        for st, count in sorted(state_counts.items(), key=lambda x: x[1], reverse=True)[:15]:
            print(f"    {st}: {count:,}")

        # View stats if enriched
        views = [l.get('profileViews') or 0 for l in listings if l.get('profileViews') is not None]
        if views:
            print(f"\nView stats:")
            print(f"    Listings with views: {len(views):,}")
            print(f"    Total views: {sum(views):,}")
            print(f"    Avg views: {sum(views)/len(views):,.0f}")
            print(f"    Max views: {max(views):,}")

        # Top 20
        if views:
            print(f"\nTOP 20 BY VIEWS:")
            for i, l in enumerate(listings[:20], 1):
                v = l.get('profileViews') or 0
                p = l.get('price') or 0
                h = (l.get('header') or '')[:45]
                st = l.get('scraped_state', '??')
                print(f"  {i:2}. {v:>6,} views | {st:>2} | ${p:>12,.0f} | {h}")

        print(f"\n[+] Done!")
    else:
        print("[-] No listings found.")
