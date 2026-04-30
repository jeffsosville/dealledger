"""
BizBuySell Sold-Count Scraper - BROWSER-COOKIE VERSION

Uses the full set of cookies copied from a real Chrome browser session
(via DevTools "Copy as cURL"). This bypasses Akamai because we're presenting
the same session that already passed Akamai's bot challenge in the browser.

USAGE:

    Step 1 (one-time-per-session): Get fresh cookies from your browser.
        - Open BBS in Chrome, browse a broker profile page
        - DevTools (Cmd+Opt+I) -> Network tab
        - Cmd+R to refresh
        - Filter to "Doc"
        - Right-click the broker page request -> Copy -> Copy as cURL (bash)
        - Save the cookie portion to bbs_cookies.txt (instructions below)

    Step 2: Set proxy env vars
        export DATAIMPULSE_USER='your_user__cr.us'
        export DATAIMPULSE_PASS='your_pass'

    Step 3: Run
        python3 bbs_sold_scraper.py --test    # tests on 5 known brokers
        python3 bbs_sold_scraper.py --all     # scrapes all brokers from CSV

NOTE: Cookies typically last 30-60 minutes. If you start getting 403s
mid-run, get fresh cookies and resume.
"""

from curl_cffi import requests
import csv
import json
import os
import re
import sys
import time
import argparse
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from colorama import Fore, init

init(autoreset=True)
print_lock = threading.Lock()

def log(msg):
    with print_lock:
        print(msg)


# ============================================================
# Configuration
# ============================================================

PROXY_USER = os.environ.get("DATAIMPULSE_USER")
PROXY_PASS = os.environ.get("DATAIMPULSE_PASS")
PROXY_HOST = os.environ.get("DATAIMPULSE_HOST", "gw.dataimpulse.com")
PROXY_PORT = os.environ.get("DATAIMPULSE_PORT", "823")

# Path to your cookie file (you'll create this from your browser curl)
COOKIE_FILE = "bbs_cookies.txt"

# Path to the broker list CSV (the April 2025 master)
BROKER_CSV = "master_broker_data_bbs__1_.csv"

# Output CSV
OUTPUT_CSV = "broker_sold_counts_2026.csv"

# Polite scraping delay
DELAY_BETWEEN_REQUESTS = 2.0  # seconds
MAX_WORKERS = 3  # keep low to avoid triggering Akamai

# Headers from your real browser session
BROWSER_HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"macOS"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
}


def load_cookies():
    """Load cookies from bbs_cookies.txt.
    
    File format: a single line containing the cookie string from your browser
    (the value after -b in the curl command, without the quotes).
    """
    if not os.path.exists(COOKIE_FILE):
        log(f"{Fore.RED}[-] {COOKIE_FILE} not found.")
        log(f"{Fore.YELLOW}[!] Create it: paste the value after `-b '` from your curl command.")
        log(f"{Fore.YELLOW}[!] One line, no quotes, no -b prefix.")
        sys.exit(1)
    
    with open(COOKIE_FILE, 'r') as f:
        cookie_string = f.read().strip()
    
    if not cookie_string:
        log(f"{Fore.RED}[-] {COOKIE_FILE} is empty.")
        sys.exit(1)
    
    # Parse cookie string into dict
    cookies = {}
    for pair in cookie_string.split('; '):
        if '=' in pair:
            key, value = pair.split('=', 1)
            cookies[key.strip()] = value.strip()
    
    log(f"{Fore.GREEN}[+] Loaded {len(cookies)} cookies from {COOKIE_FILE}")
    
    # Sanity check the critical cookies
    critical = ['_track_tkn', 'ak_bmsc', 'bm_sv']
    missing = [c for c in critical if c not in cookies]
    if missing:
        log(f"{Fore.YELLOW}[!] Missing expected cookies: {missing}")
        log(f"{Fore.YELLOW}[!] Scrape may fail. Re-grab cookies from browser.")
    
    return cookies


def build_proxies():
    if not (PROXY_USER and PROXY_PASS):
        log(f"{Fore.YELLOW}[!] No proxy credentials. Running direct (your IP).")
        return None
    proxy_url = f"http://{PROXY_USER}:{PROXY_PASS}@{PROXY_HOST}:{PROXY_PORT}"
    log(f"{Fore.CYAN}[*] Proxy: {PROXY_HOST}:{PROXY_PORT}")
    return {"http": proxy_url, "https": proxy_url}


# Pattern for the sold count - matches the HTML you showed me:
# <span class="...">Listings sold on BizBuySell: <b>101</b></span>
SOLD_COUNT_PATTERN = re.compile(
    r'Listings\s+sold\s+on\s+BizBuySell\s*:\s*<b[^>]*>\s*(\d+)\s*</b>',
    re.IGNORECASE
)
# Fallback patterns
FALLBACK_PATTERNS = [
    re.compile(r'Listings\s+sold\s+on\s+BizBuySell\s*:?\s*(\d+)', re.IGNORECASE),
    re.compile(r'"soldListingsCount"\s*:\s*(\d+)'),
]


def fetch_sold_count(session, profile_url):
    """Fetch a broker profile page and extract sold count.
    
    Returns: (sold_count: int|None, status: str)
    """
    try:
        response = session.get(profile_url, headers=BROWSER_HEADERS, timeout=20)
        
        if response.status_code == 403:
            return None, f"403_BLOCKED (cookies may be stale)"
        if response.status_code == 404:
            return None, "404_NOT_FOUND"
        if response.status_code != 200:
            return None, f"HTTP_{response.status_code}"
        
        html = response.text
        
        # Try primary pattern first (matches the bold-tag structure)
        m = SOLD_COUNT_PATTERN.search(html)
        if m:
            return int(m.group(1)), "OK"
        
        # Try fallbacks
        for pattern in FALLBACK_PATTERNS:
            m = pattern.search(html)
            if m:
                return int(m.group(1)), "OK_FALLBACK"
        
        # Got 200 but no sold count found - probably a broker with 0 sold
        # or page structure changed
        if "Listings sold on BizBuySell" not in html:
            return 0, "NO_SOLD_SECTION"  # Broker has never sold anything
        
        return None, "PATTERN_MISMATCH"
        
    except Exception as e:
        return None, f"ERROR: {type(e).__name__}: {str(e)[:100]}"


def scrape_one_broker(broker_row, session, results, results_lock):
    """Scrape one broker and append result."""
    profile_url = broker_row.get('bbs_profile_url') or build_profile_url(broker_row)
    
    if not profile_url:
        log(f"{Fore.YELLOW}[!] No URL for {broker_row.get('broker_name')} (account {broker_row.get('account')})")
        return
    
    sold, status = fetch_sold_count(session, profile_url)
    
    name = broker_row.get('broker_name', 'unknown')
    company = broker_row.get('company_name', '')
    baseline = broker_row.get('sold_listings', '')
    
    if sold is not None:
        delta = ''
        try:
            delta = f"+{int(sold) - int(baseline)}" if baseline else ''
        except (ValueError, TypeError):
            pass
        log(f"{Fore.GREEN}[+] {name} | {company} | {baseline} -> {sold} {delta} [{status}]")
    else:
        log(f"{Fore.RED}[-] {name} | {company} | FAILED [{status}]")
    
    with results_lock:
        results.append({
            'account': broker_row.get('account', ''),
            'broker_name': name,
            'company_name': company,
            'profile_url': profile_url,
            'sold_listings_apr2025': baseline,
            'sold_listings_current': sold if sold is not None else '',
            'status': status,
            'scraped_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        })
    
    time.sleep(DELAY_BETWEEN_REQUESTS)


def build_profile_url(broker_row):
    """Try to construct the BBS broker profile URL from a CSV row."""
    # If the CSV already has a direct profile URL, use it
    for field in ['bbs_profile_url', 'profile_url', 'url']:
        val = broker_row.get(field)
        if val and 'business-broker' in val:
            if not val.startswith('http'):
                val = 'https://www.bizbuysell.com' + val
            return val
    
    # Otherwise, we need brokerProfileId to build the URL.
    # The April 2025 CSV doesn't have it directly - it's only in the bigger CSV.
    # Caller should pre-populate bbs_profile_url before scrape.
    return None


def load_test_brokers():
    """Five known brokers for testing."""
    return [
        {'account': '25324', 'broker_name': 'Jeff Sosville', 'company_name': 'ATM Brokerage',
         'bbs_profile_url': 'https://www.bizbuysell.com/business-broker/jeff-sosville/atm-brokerage/18464/',
         'sold_listings': '77'},
        {'account': '35275', 'broker_name': 'John Sosville', 'company_name': 'Platform Brokerage',
         'bbs_profile_url': 'https://www.bizbuysell.com/business-broker/john-sosville/platform-brokerage/27972/',
         'sold_listings': '15'},
        {'account': '4907', 'broker_name': 'Michael Nuanes', 'company_name': 'Business Brokerage Services LLC',
         'bbs_profile_url': 'https://www.bizbuysell.com/business-broker/michael-nuanes/business-brokerage-services/1687/',
         'sold_listings': '324'},
        {'account': '44355', 'broker_name': 'Greg Sosville', 'company_name': 'BBS Texas',
         'bbs_profile_url': 'https://www.bizbuysell.com/business-broker/greg-sosville/business-brokerage-services-llc/36359/',
         'sold_listings': '4'},
        # Charlie Vlahos as a control - it's the URL the original scraper used
        {'account': '342', 'broker_name': 'CJ Charlie Vlahos', 'company_name': 'Blanket Real Estate',
         'bbs_profile_url': 'https://www.bizbuysell.com/business-broker/c-j-charlie-vlahos/blanket-real-estate/342/',
         'sold_listings': ''},
    ]


def load_all_brokers():
    """Load brokers from the April 2025 master CSV."""
    if not os.path.exists(BROKER_CSV):
        log(f"{Fore.RED}[-] {BROKER_CSV} not found.")
        sys.exit(1)
    
    brokers = []
    with open(BROKER_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Need a profile URL. The April 2025 CSV doesn't always have it.
            # User should pre-populate bbs_profile_url, or we skip rows without it.
            if not row.get('bbs_profile_url'):
                # Try to build from broker_name + company_name + account
                # but BBS URL needs brokerProfileId, not account, so this often fails
                continue
            brokers.append(row)
    
    log(f"{Fore.CYAN}[*] Loaded {len(brokers)} brokers with profile URLs from {BROKER_CSV}")
    return brokers


def save_results(results, filename):
    if not results:
        log(f"{Fore.YELLOW}[!] No results to save.")
        return
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        for row in results:
            writer.writerow(row)
    log(f"{Fore.GREEN}[+] Saved {len(results)} results to {filename}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true', help='Test on 5 known brokers')
    parser.add_argument('--all', action='store_true', help='Scrape all brokers from CSV')
    args = parser.parse_args()
    
    if not (args.test or args.all):
        parser.print_help()
        log(f"\n{Fore.YELLOW}Pick --test (recommended first) or --all")
        sys.exit(1)
    
    cookies = load_cookies()
    proxies = build_proxies()
    
    # Build session with cookies, proxy, Chrome impersonation
    session = requests.Session(
        impersonate="chrome",
        proxies=proxies,
        cookies=cookies,
    )
    
    # Quick proxy test
    if proxies:
        try:
            r = session.get("https://api.ipify.org?format=json", timeout=15)
            log(f"{Fore.GREEN}[+] Proxy IP: {r.json().get('ip')}")
        except Exception as e:
            log(f"{Fore.RED}[-] Proxy test failed: {e}")
            sys.exit(1)
    
    # Load broker list
    if args.test:
        brokers = load_test_brokers()
        log(f"{Fore.CYAN}[*] Testing on {len(brokers)} known brokers")
    else:
        brokers = load_all_brokers()
    
    # Scrape
    results = []
    results_lock = threading.Lock()
    
    log(f"{Fore.CYAN}[*] Starting scrape with {MAX_WORKERS} workers, {DELAY_BETWEEN_REQUESTS}s delay")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(scrape_one_broker, broker, session, results, results_lock)
            for broker in brokers
        ]
        
        completed = 0
        for future in as_completed(futures):
            completed += 1
            if completed % 50 == 0 and not args.test:
                save_results(results, OUTPUT_CSV)
                log(f"{Fore.CYAN}[*] Progress: {completed}/{len(brokers)}")
    
    save_results(results, OUTPUT_CSV)
    
    # Summary
    successful = sum(1 for r in results if r['sold_listings_current'] != '')
    log(f"\n{Fore.CYAN}{'='*60}")
    log(f"{Fore.CYAN}Summary: {successful}/{len(results)} brokers scraped successfully")
    log(f"{Fore.CYAN}{'='*60}")


if __name__ == "__main__":
    main()
