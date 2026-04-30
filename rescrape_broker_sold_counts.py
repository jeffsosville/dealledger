"""
BizBuySell Broker Scraper - HARDCODED TOKEN + PROXY VERSION

Skips the Akamai-blocked HTML page fetch by using a JWT token copied
from a real browser session. Routes API calls through DataImpulse residential proxy.

SETUP:
    1. Open BBS in Chrome, browse for 30 sec (so cookies load)
    2. DevTools -> Application -> Cookies -> bizbuysell.com -> _track_tkn
    3. Copy the FULL value (long JWT starting with "eyJ...")
    4. Set env vars:
       export DATAIMPULSE_USER='your_user__cr.us'
       export DATAIMPULSE_PASS='your_pass'
       export BBS_TRACK_TKN='paste_the_full_jwt_here'
    5. Run: python3 bbs_scraper_with_token.py

Page-1-only diagnostic mode. If page 1 returns broker JSON with
soldListingsCount, the rest of the scrape is just looping over pages 1..250.

Token expires in a few hours. If you get 401, re-grab from browser.
"""

from curl_cffi import requests
import json
import csv
import os
from colorama import Fore, init
import threading
import base64

init(autoreset=True)
thread_local = threading.local()
print_lock = threading.Lock()
file_lock = threading.Lock()

def thread_safe_print(message):
    with print_lock:
        print(message)


# ============================================================
# Configuration
# ============================================================
PROXY_USER = os.environ.get("DATAIMPULSE_USER")
PROXY_PASS = os.environ.get("DATAIMPULSE_PASS")
PROXY_HOST = os.environ.get("DATAIMPULSE_HOST", "gw.dataimpulse.com")
PROXY_PORT = os.environ.get("DATAIMPULSE_PORT", "823")
BBS_TOKEN = os.environ.get("BBS_TRACK_TKN")

if not (PROXY_USER and PROXY_PASS):
    print(f"{Fore.RED}[-] Missing DataImpulse credentials.")
    print(f"{Fore.YELLOW}    export DATAIMPULSE_USER='your_user__cr.us'")
    print(f"{Fore.YELLOW}    export DATAIMPULSE_PASS='your_password'")
    raise SystemExit(1)

if not BBS_TOKEN:
    print(f"{Fore.RED}[-] Missing BBS auth token.")
    print(f"{Fore.YELLOW}[!] Get it from your browser:")
    print(f"      1. Open https://www.bizbuysell.com in Chrome (browse 30 sec)")
    print(f"      2. DevTools -> Application -> Cookies -> bizbuysell.com")
    print(f"      3. Find _track_tkn, copy the full value")
    print(f"      4. export BBS_TRACK_TKN='<paste full JWT here>'")
    raise SystemExit(1)

PROXY_URL = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
PROXIES = {"http": PROXY_URL, "https": PROXY_URL}

# Decode JWT expiry so the user knows when it'll die
def decode_jwt_exp(token):
    try:
        payload_part = token.split('.')[1]
        # Add padding for base64
        padding = 4 - len(payload_part) % 4
        if padding != 4:
            payload_part += '=' * padding
        decoded = base64.urlsafe_b64decode(payload_part)
        return json.loads(decoded)
    except Exception:
        return None

token_data = decode_jwt_exp(BBS_TOKEN)
if token_data and 'exp' in token_data:
    import datetime
    exp_time = datetime.datetime.fromtimestamp(token_data['exp'])
    now = datetime.datetime.now()
    remaining = exp_time - now
    if remaining.total_seconds() > 0:
        thread_safe_print(f"{Fore.GREEN}[+] Token valid until {exp_time.strftime('%Y-%m-%d %H:%M:%S')} ({remaining})")
    else:
        thread_safe_print(f"{Fore.RED}[-] Token already expired at {exp_time.strftime('%Y-%m-%d %H:%M:%S')}!")
        thread_safe_print(f"{Fore.YELLOW}[!] Grab a fresh _track_tkn from your browser.")
        raise SystemExit(1)

thread_safe_print(f"{Fore.CYAN}[*] Proxy: {PROXY_HOST}:{PROXY_PORT}")
thread_safe_print(f"{Fore.CYAN}[*] Proxy user: {PROXY_USER[:12]}...")
thread_safe_print(f"{Fore.CYAN}[*] BBS token: {BBS_TOKEN[:30]}... ({len(BBS_TOKEN)} chars)")


class BizBuySellScraper:
    def __init__(self):
        self.session = requests.Session(impersonate="chrome", proxies=PROXIES)
        self.token = BBS_TOKEN
        self.results = []
        self.results_lock = threading.Lock()
        self.test_proxy_first()

    def test_proxy_first(self):
        thread_safe_print(f"{Fore.CYAN}[*] Testing proxy connection...")
        try:
            response = self.session.get("https://api.ipify.org?format=json", timeout=15)
            if response.status_code == 200:
                ip = response.json().get('ip')
                thread_safe_print(f"{Fore.GREEN}[+] Proxy working. Outbound IP: {ip}")
            else:
                thread_safe_print(f"{Fore.RED}[-] Proxy test status {response.status_code}")
                raise SystemExit(1)
        except Exception as e:
            thread_safe_print(f"{Fore.RED}[-] Proxy test failed: {e}")
            raise SystemExit(1)

    def get_session(self):
        if not hasattr(thread_local, "session"):
            thread_local.session = requests.Session(impersonate="chrome", proxies=PROXIES)
        return thread_local.session

    def scrape_page(self, page):
        thread_safe_print(f"{Fore.YELLOW}[*] Calling broker search API, page {page}...")

        api_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json',
            'Origin': 'https://www.bizbuysell.com',
            'Referer': 'https://www.bizbuysell.com/',
            'Sec-Ch-Ua': '"Chromium";v="135", "Not-A.Brand";v="8"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-site',
            'Authorization': f'Bearer {self.token}',
        }

        payload = {
            "siteId": 20,
            "languageId": 10,
            "pageNumber": page,
            "locations": None,
            "keywordPhrases": None,
            "brokerKeywordSearchTypeIds": [20],
            "expertiseIdentifiers": None,
            "expertiseCategoryIds": None,
            "expertiseServiceIds": None,
            "expertiseLanguageIds": None,
            "hasBrokerLicense": 0,
            "certifiedBroker": 0,
            "memberOfBrokerAssociation": 0
        }

        try:
            session = self.get_session()
            response = session.post(
                'https://api.bizbuysell.com/bff/v2/brokerSearch',
                headers=api_headers,
                json=payload
            )

            thread_safe_print(f"{Fore.MAGENTA}[DEBUG] API status: {response.status_code}")

            if response.status_code == 401:
                thread_safe_print(f"{Fore.RED}[-] 401 Unauthorized - token rejected.")
                thread_safe_print(f"{Fore.YELLOW}[!] Token expired or wrong format. Re-grab _track_tkn from browser.")
                return []

            if response.status_code != 200:
                thread_safe_print(f"{Fore.RED}[-] API failed: {response.status_code}")
                thread_safe_print(f"{Fore.RED}[-] Body: {response.text[:500]}")
                return []

            data = response.json()
            broker_data = data.get("value", {}).get("brokerSearchResult", {})
            brokers = broker_data.get("value", [])

            # ===== BIG DEBUG DUMP ON PAGE 1 =====
            if page == 1 and brokers:
                thread_safe_print(f"{Fore.MAGENTA}[DEBUG] ========== BROKER JSON DIAGNOSTICS ==========")
                thread_safe_print(f"{Fore.MAGENTA}[DEBUG] Total brokers in API: {broker_data.get('total', 0)}")
                thread_safe_print(f"{Fore.MAGENTA}[DEBUG] Brokers returned on page 1: {len(brokers)}")
                thread_safe_print(f"")
                thread_safe_print(f"{Fore.MAGENTA}[DEBUG] All field names in first broker:")
                for k in brokers[0].keys():
                    thread_safe_print(f"{Fore.MAGENTA}[DEBUG]   - {k}")
                thread_safe_print(f"")
                thread_safe_print(f"{Fore.MAGENTA}[DEBUG] Full first broker JSON:")
                thread_safe_print(json.dumps(brokers[0], indent=2))

                # Highlight any field that smells like sold/active/listing/count
                interesting = [
                    k for k in brokers[0].keys()
                    if any(needle in k.lower() for needle in ['sold', 'listing', 'count', 'active', 'transaction'])
                ]
                thread_safe_print(f"")
                thread_safe_print(f"{Fore.GREEN}[+] Fields likely related to sold/active/listing counts:")
                for field in interesting:
                    thread_safe_print(f"{Fore.GREEN}    {field} = {brokers[0].get(field)}")
                thread_safe_print(f"{Fore.MAGENTA}[DEBUG] =============================================")
            # ===== END DEBUG =====

            page_results = []
            for broker in brokers:
                broker_info = {
                    'name': f"{broker.get('firstName', '')} {broker.get('lastName', '')}".strip(),
                    'business_name': broker.get('companyName', ''),
                    'phone': broker.get('telephone', ''),
                    'website': broker.get('companyUrl', ''),
                    'broker_profile_id': broker.get('brokerProfileId', '') or broker.get('id', ''),
                    'account': broker.get('account', ''),
                    'active_listings': broker.get('activeListingsCount', '') or broker.get('activeListings', ''),
                    'sold_listings': broker.get('soldListingsCount', '') or broker.get('soldListings', ''),
                    'sold_last_six_months': broker.get('soldlistingslastsixmonths', '')
                                            or broker.get('soldListingsLastSixMonths', ''),
                }
                broker_info = {k: (v.strip() if isinstance(v, str) else v) for k, v in broker_info.items()}
                page_results.append(broker_info)

                thread_safe_print(
                    f"{Fore.GREEN}[+] {broker_info['name']} | {broker_info['business_name']} | "
                    f"sold={broker_info['sold_listings']} active={broker_info['active_listings']}"
                )

            with self.results_lock:
                self.results.extend(page_results)

            return page_results

        except Exception as e:
            thread_safe_print(f"{Fore.RED}[-] Error scraping page {page}: {e}")
            import traceback
            thread_safe_print(f"{Fore.RED}{traceback.format_exc()}")
            return []

    def save_results(self, filename="bizbuysell_diagnostic_page1.csv"):
        try:
            with file_lock:
                if not self.results:
                    thread_safe_print(f"{Fore.YELLOW}[!] No results to save.")
                    return
                with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = list(self.results[0].keys())
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    for broker in self.results:
                        writer.writerow(broker)
                thread_safe_print(f"{Fore.GREEN}[+] Saved {len(self.results)} brokers to {filename}")
        except Exception as e:
            thread_safe_print(f"{Fore.RED}[-] Save error: {e}")


if __name__ == "__main__":
    thread_safe_print(f"{Fore.CYAN}{'='*60}")
    thread_safe_print(f"{Fore.CYAN}{' '*5}BBS Scraper - HARDCODED TOKEN + PROXY")
    thread_safe_print(f"{Fore.CYAN}{'='*60}\n")

    scraper = BizBuySellScraper()
    scraper.scrape_page(1)
    scraper.save_results("bizbuysell_diagnostic_page1.csv")

    thread_safe_print(f"\n{Fore.CYAN}{'='*60}")
    thread_safe_print(f"{Fore.CYAN}If page 1 returned brokers with sold counts, ")
    thread_safe_print(f"{Fore.CYAN}we're ready to scale to all 250 pages.")
    thread_safe_print(f"{Fore.CYAN}{'='*60}")