"""
DealLedger Specialized Scrapers
===============================
Custom scrapers for major franchise brokers that need specific handling.

Each broker has unique HTML structure, pagination, or API endpoints that
the generic ML-based scraper can't handle well. These specialized scrapers
yield 10-100x more listings than generic pattern detection.

Supported Brokers:
- Murphy Business (Selenium - JS pagination)
- Hedgestone Business Advisors (Selenium)
- Transworld Business Advisors (API endpoint)
- Sunbelt Business Brokers (WordPress AJAX)
- VR Business Brokers (URL pagination)
- First Choice Business Brokers (JSON API)
- Link Business (HTML pagination)
- Executive Business Brokers / Larry Bodner (Selenium - table-based)

Requirements:
- selenium + webdriver-manager (for Murphy, Hedgestone, Bodner)
- curl_cffi (for Transworld, Sunbelt, VR, FCBB, Link)
- beautifulsoup4

License: MIT
"""

import hashlib
import re
import json
import time
import random
from typing import List, Dict, Optional
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from bs4 import BeautifulSoup

# Selenium imports (for JS-heavy sites)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# curl_cffi for anti-bot bypass
from curl_cffi import requests
from requests.exceptions import RequestException


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_money(text: str) -> Optional[float]:
    """
    Parse money values like $12m, $1.5M, $500k, $1,234,567
    
    Examples:
        parse_money("$1.5M") -> 1500000.0
        parse_money("$500k") -> 500000.0
        parse_money("$1,234,567") -> 1234567.0
    """
    if not text:
        return None
    try:
        cleaned = text.replace('$', '').replace(',', '').strip().lower()
        if 'm' in cleaned:
            return float(cleaned.replace('m', '')) * 1_000_000
        if 'k' in cleaned:
            return float(cleaned.replace('k', '')) * 1_000
        return float(cleaned)
    except:
        return None


def extract_city_state(location: str) -> tuple:
    """
    Extract city and state from location string.
    
    Examples:
        extract_city_state("Austin, TX") -> ("Austin", "TX")
        extract_city_state("New York, New York") -> ("New York", "New York")
        extract_city_state("CA") -> (None, "CA")
    """
    if not location:
        return None, None
    
    # Try "City, ST" format (2-letter state code)
    m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b', location)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    
    # Try "City, State Name" format
    m2 = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', location)
    if m2:
        return m2.group(1).strip(), m2.group(2).strip()
    
    # Just 2-letter state code
    state_match = re.search(r'\b([A-Z]{2})\b', location)
    if state_match:
        return None, state_match.group(1)
    
    return None, None


def create_chrome_driver(headless: bool = True) -> webdriver.Chrome:
    """Create a configured Chrome WebDriver instance."""
    options = Options()
    if headless:
        options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(60)
    return driver


def format_listing(
    url: str,
    broker_account: str,
    title: str = None,
    price: float = None,
    price_text: str = None,
    location: str = None,
    city: str = None,
    state: str = None,
    description: str = None,
    business_type: str = None,
    revenue: float = None,
    cash_flow: float = None
) -> Dict:
    """Create a standardized listing dict."""
    return {
        'listing_id': hashlib.md5(url.encode()).hexdigest(),
        'broker_account': broker_account,
        'title': title,
        'price': price,
        'price_text': price_text,
        'location': location,
        'city': city,
        'state': state,
        'description': description[:500] if description else None,
        'listing_url': url,
        'image_url': None,
        'category': 'business',
        'business_type': business_type,
        'revenue': revenue,
        'cash_flow': cash_flow
    }


# ============================================================================
# MURPHY BUSINESS SCRAPER
# Uses Selenium because pagination is JavaScript-driven
# ============================================================================

class MurphyScraper:
    """
    Murphy Business & Financial Corporation
    https://murphybusiness.com
    
    ~500+ listings, JavaScript pagination requires Selenium.
    """
    
    BASE = "https://murphybusiness.com"
    LIST_URL = f"{BASE}/business-brokerage/view-our-listings/"
    SDE_RE = re.compile(r"SDE:\s*\$([\d,]+)", re.I)

    @staticmethod
    def scrape(broker_account: str, max_pages: int = 50, headless: bool = True, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Murphy Business & Financial Corporation")
            print('='*60)
        
        driver = create_chrome_driver(headless)
        listings = []
        seen_urls = set()
        
        try:
            driver.get(MurphyScraper.LIST_URL)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.card-body"))
            )
            time.sleep(3)
            
            page_num = 1
            consecutive_dupes = 0
            
            while page_num <= max_pages:
                # Parse current page
                cards = driver.find_elements(By.CSS_SELECTOR, "div.card-body")
                page_listings = []
                
                for card in cards:
                    try:
                        title = card.find_element(By.CSS_SELECTOR, "h5.card-title").text.strip()
                    except:
                        title = None
                    
                    try:
                        price_txt = card.find_element(By.CSS_SELECTOR, "p.price").text.strip()
                    except:
                        price_txt = None
                    
                    txt = card.text
                    m = MurphyScraper.SDE_RE.search(txt)
                    sde_txt = m.group(1) if m else None
                    location = txt.split("|")[-1].strip() if "|" in txt else None
                    
                    try:
                        detail_url = card.find_element(By.CSS_SELECTOR, "a.btn.btn-primary").get_attribute("href")
                    except:
                        detail_url = None
                    
                    if detail_url:
                        page_listings.append({
                            'url': detail_url,
                            'title': title,
                            'price_text': price_txt,
                            'sde_text': sde_txt,
                            'location': location,
                            'text': txt
                        })
                
                # Dedupe
                new = [l for l in page_listings if l['url'] not in seen_urls]
                for l in new:
                    seen_urls.add(l['url'])
                
                if verbose:
                    print(f"[Murphy] Page {page_num}: {len(page_listings)} cards, {len(new)} new | Total: {len(listings) + len(new)}")
                
                if not new:
                    consecutive_dupes += 1
                    if consecutive_dupes >= 3:
                        break
                else:
                    consecutive_dupes = 0
                    for item in new:
                        city, state = extract_city_state(item['location'])
                        listings.append(format_listing(
                            url=item['url'],
                            broker_account=broker_account,
                            title=item['title'],
                            price=parse_money(item['price_text']),
                            price_text=item['price_text'],
                            location=item['location'],
                            city=city,
                            state=state,
                            description=item['text'],
                            cash_flow=parse_money(item['sde_text'])
                        ))
                
                # Navigate to next page — handles sliding window pagination
                next_page = page_num + 1
                try:
                    # First try direct page button
                    next_btn = driver.find_element(By.CSS_SELECTOR, f"a.page_number[data-page='{next_page}']")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(4)
                    page_num += 1
                except:
                    # Page button not visible — click >> to slide window forward
                    try:
                        next_arrow = driver.find_element(
                            By.XPATH, "//a[contains(@class,'page_number') and contains(text(),'>>')]"
                        )
                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_arrow)
                        time.sleep(0.5)
                        driver.execute_script("arguments[0].click();", next_arrow)
                        time.sleep(4)
                        try:
                            next_btn = driver.find_element(By.CSS_SELECTOR, f"a.page_number[data-page='{next_page}']")
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(4)
                        except:
                            pass
                        page_num += 1
                    except:
                        break
        
        finally:
            driver.quit()
        
        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            with_cf = sum(1 for l in listings if l.get('cash_flow'))
            print(f"\n✓ {len(listings)} Murphy listings ({with_price} with price, {with_cf} with SDE)")
        
        return listings


# ============================================================================
# HEDGESTONE BUSINESS ADVISORS SCRAPER
# Uses Selenium for JavaScript-rendered content
# ============================================================================

class HedgestoneScraper:
    """
    Hedgestone Business Advisors
    https://www.hedgestone.com
    
    JavaScript-heavy site requiring Selenium.
    """
    
    BASE = "https://www.hedgestone.com"
    LIST_URL = f"{BASE}/businesses-for-sale/"
    
    def scrape(self, broker_account: str, max_pages: int = 15, headless: bool = True, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Hedgestone Business Advisors")
            print('='*60)
        
        driver = create_chrome_driver(headless)
        listings = []
        seen_urls = set()
        
        try:
            for page_num in range(1, max_pages + 1):
                url = self.LIST_URL if page_num == 1 else f"{self.LIST_URL}page/{page_num}/"
                
                driver.get(url)
                time.sleep(3)
                
                # Scroll to load lazy content
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                listing_divs = driver.find_elements(By.CSS_SELECTOR, "div.single-listing")
                
                if not listing_divs:
                    break
                
                page_listings = []
                for div in listing_divs:
                    try:
                        link = div.find_element(By.CSS_SELECTOR, 'a[href*="business-opportunity"]')
                        url = link.get_attribute('href')
                        if not url:
                            continue
                        
                        try:
                            title = div.find_element(By.CSS_SELECTOR, 'h3').text.strip()
                        except:
                            title = None
                        
                        try:
                            location = div.find_element(By.CSS_SELECTOR, 'p.listing-location').text.strip()
                        except:
                            location = None
                        
                        price_text = None
                        cash_flow = None
                        try:
                            price_info = div.find_element(By.CSS_SELECTOR, 'div.price-info')
                            try:
                                price_text = price_info.find_element(By.CSS_SELECTOR, 'div.listing-price span.value').text.strip()
                            except:
                                pass
                            try:
                                cf_text = price_info.find_element(By.CSS_SELECTOR, 'div.listing-cashflow span.value').text.strip()
                                cash_flow = parse_money(cf_text)
                            except:
                                pass
                        except:
                            pass
                        
                        page_listings.append({
                            'url': url,
                            'title': title,
                            'price_text': price_text,
                            'location': location,
                            'cash_flow': cash_flow
                        })
                    except:
                        continue
                
                new = [l for l in page_listings if l['url'] not in seen_urls]
                for l in new:
                    seen_urls.add(l['url'])
                
                if verbose:
                    print(f"[Hedgestone] Page {page_num}: {len(page_listings)} found, {len(new)} new | Total: {len(listings) + len(new)}")
                
                for item in new:
                    city, state = extract_city_state(item['location'])
                    listings.append(format_listing(
                        url=item['url'],
                        broker_account=broker_account,
                        title=item['title'],
                        price=parse_money(item['price_text']),
                        price_text=item['price_text'],
                        location=item['location'],
                        city=city,
                        state=state,
                        cash_flow=item['cash_flow']
                    ))
                
                if not new:
                    break
                
                time.sleep(random.uniform(2, 4))
        
        finally:
            driver.quit()
        
        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} Hedgestone listings ({with_price} with price)")
        
        return listings


# ============================================================================
# TRANSWORLD BUSINESS ADVISORS SCRAPER
# Uses their internal API endpoint
# ============================================================================

class TransworldScraper:
    """
    Transworld Business Advisors
    https://www.tworld.com

    Uses Playwright for bootstrap (Cloudflare cf_clearance required),
    then curl_cffi for fast parallel API fetching.
    ~3,465 listings across 385 pages (9 per page).
    """

    BASE = "https://www.tworld.com"
    API_URL = f"{BASE}/api/listings"
    SEARCH_URL = f"{BASE}/buy-a-business/business-listing-search"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome124")
        self.api_headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": self.BASE,
            "Referer": self.SEARCH_URL,
            "sec-ch-ua": '"Chromium";v="124", "Not-A.Brand";v="24", "Google Chrome";v="124"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
        }
        self.lock = Lock()
        self.seen = set()
        self._bootstrap()

    def _bootstrap(self):
        """Bootstrap using curl_cffi requests — works without Playwright."""
        self._bootstrap_requests()

    def _bootstrap_requests(self):
        """Fallback bootstrap without Playwright."""
        try:
            self.session.get(self.BASE, timeout=30)
            r = self.session.get(self.SEARCH_URL, timeout=30)
            xsrf = self.session.cookies.get("XSRF-TOKEN")
            if xsrf:
                from urllib.parse import unquote
                self.api_headers["X-XSRF-TOKEN"] = unquote(xsrf)
                print(f"[Transworld] Got XSRF (requests fallback) ✓")
        except Exception as e:
            print(f"[Transworld] Requests bootstrap warning: {e}")

    def _fetch_page(self, page_num: int) -> List[Dict]:
        """Fetch a single page from the API."""
        payload = {
            "page": page_num,
            "per_page": 9,
            "country": {"value": 4, "name": "United States"},
            "state": None,
            "region": None,
            "assigned_to": None,
            "categories": None,
            "sort": {"value": "-c_listing_price__c", "name": "Price ($$$ to $)"},
        }

        for attempt in range(3):
            try:
                r = self.session.post(
                    self.API_URL,
                    headers=self.api_headers,
                    json=payload,
                    timeout=45
                )

                if r.status_code == 419:
                    print(f"[Transworld] 419 on page {page_num} — re-bootstrapping")
                    self._bootstrap()
                    continue

                if r.status_code == 403:
                    print(f"[Transworld] 403 on page {page_num} — Cloudflare blocked")
                    return []

                r.raise_for_status()
                data = r.json()

                # API returns "data" array and "pagination" object
                arr = data.get("data") or []

                out = []
                with self.lock:
                    for item in arr:
                        key = item.get("slug")
                        if key and key not in self.seen:
                            self.seen.add(key)
                            out.append(item)
                return out

            except Exception as e:
                if attempt == 2:
                    return []
                time.sleep(1 + attempt)
        return []

    def scrape(self, broker_account: str, max_pages: int = 385, workers: int = 6, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Transworld Business Advisors")
            print('='*60)

        all_items = []

        # First page to verify it works
        first = self._fetch_page(1)
        all_items.extend(first)
        if verbose:
            print(f"[Transworld] Page 1: {len(first)} listings")

        if not first:
            print("[Transworld] Page 1 returned 0 — check bootstrap/Cloudflare")
            return []

        # Parallel fetch remaining pages
        if max_pages > 1:
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futures = [ex.submit(self._fetch_page, p) for p in range(2, max_pages + 1)]
                completed = 0
                for fut in as_completed(futures):
                    rows = fut.result()
                    if rows:
                        all_items.extend(rows)
                    completed += 1
                    if verbose and completed % 20 == 0:
                        print(f"[Transworld] {completed}/{max_pages-1} pages done | {len(all_items)} total")
                    time.sleep(0.2)

        # Convert to standard format
        listings = []
        for item in all_items:
            location = item.get("location")
            city, state = extract_city_state(location or "")

            price = item.get("price")
            cash_flow = item.get("seller_discretionary_earnings")
            slug = item.get("slug", "")
            tribe = item.get("tribe_slug", "")

            # Build canonical URL
            if tribe and slug:
                url = f"{self.BASE}/{tribe}/listing/{slug}"
            elif slug:
                url = f"{self.BASE}/listings/{slug}"
            else:
                url = self.BASE

            listings.append(format_listing(
                url=url,
                broker_account=broker_account,
                title=item.get("heading"),
                price=float(price) if price else None,
                price_text=f"${price:,.0f}" if price else None,
                location=location,
                city=city,
                state=state,
                cash_flow=float(cash_flow) if cash_flow else None
            ))

        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} Transworld listings ({with_price} with price)")

        return listings


# ============================================================================
# SUNBELT BUSINESS BROKERS SCRAPER
# Uses WordPress AJAX endpoint
# ============================================================================

class SunbeltScraper:
    """
    Sunbelt Business Brokers
    https://www.sunbeltnetwork.com
    
    WordPress AJAX pagination. ~2500+ listings.
    """
    
    BASE = "https://www.sunbeltnetwork.com"
    AJAX_URL = f"{BASE}/wp-admin/admin-ajax.php"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        # Init cookies
        try:
            self.session.get(f"{self.BASE}/business-search/business-results/", timeout=20)
        except:
            pass

    def _fetch_page(self, page_num: int) -> str:
        """POST to AJAX endpoint."""
        payload = [
            ('action', 'sunbelt_business_results'),
            ('numberPaged', str(page_num)),
            ('filterValue', 'default'),
            ('keywords', ''),
            ('country', ''),
            ('state', ''),
            ('county', ''),
            ('idIndustry', ''),
            ('priceMin', ''),
            ('priceMax', ''),
            ('businessSearch', 'pageBusinessSearch'),
            ('status[]', 'sale_pending'),
            ('status[]', 'published'),
            ('statusPrimary', 'true')
        ]
        
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest'
        }
        
        r = self.session.post(self.AJAX_URL, data=payload, headers=headers, timeout=30)
        r.raise_for_status()
        return r.text

    def _parse_html(self, html: str) -> List[Dict]:
        """Parse AJAX HTML response."""
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        for article in soup.find_all('article', class_='latestBusinesses__item'):
            try:
                # Find listing URL
                link = article.find('a', href=re.compile(r'/listing-details/'))
                if not link:
                    continue
                
                url = link.get('href')
                if url.startswith('/'):
                    url = self.BASE + url
                
                # Extract title from text
                text = article.get_text('\n')
                title = None
                for line in text.split('\n'):
                    line = line.strip()
                    if line and len(line) > 10 and 'View Listing' not in line:
                        title = line
                        break
                
                # Extract financials
                price_match = re.search(r'Asking Price\s+\$([0-9.]+[mk]?)', text, re.I)
                price_text = f"${price_match.group(1)}" if price_match else None
                
                cf_match = re.search(r'\$([0-9.]+[mk]?)\s+Cash Flow', text, re.I)
                cf_text = f"${cf_match.group(1)}" if cf_match else None
                
                rev_match = re.search(r'\$([0-9.]+[mk]?)\s+Gross Revenue', text, re.I)
                rev_text = f"${rev_match.group(1)}" if rev_match else None
                
                # Extract location from URL
                loc_match = re.search(r'/([^/]+)-([a-z]{2})/(?:buy-a-business|listing-details)/', url)
                if loc_match:
                    city = loc_match.group(1).replace('-', ' ').title()
                    state = loc_match.group(2).upper()
                    location = f"{city}, {state}"
                else:
                    city, state, location = None, None, None
                
                listings.append({
                    'url': url,
                    'title': title,
                    'price_text': price_text,
                    'cf_text': cf_text,
                    'rev_text': rev_text,
                    'location': location,
                    'city': city,
                    'state': state
                })
            except:
                continue
        
        return listings

    def scrape(self, broker_account: str, max_pages: int = 130, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Sunbelt Business Brokers")
            print('='*60)
        
        all_items = []
        seen_urls = set()
        
        for page_num in range(1, max_pages + 1):
            try:
                html = self._fetch_page(page_num)
                page_items = self._parse_html(html)
                
                new = [l for l in page_items if l['url'] not in seen_urls]
                for l in new:
                    seen_urls.add(l['url'])
                
                if verbose and (page_num == 1 or page_num % 20 == 0):
                    print(f"[Sunbelt] Page {page_num}: {len(new)} new | Total: {len(all_items) + len(new)}")
                
                all_items.extend(new)
                
                if not page_items:
                    break
                
                time.sleep(random.uniform(0.8, 1.5))
                
            except Exception as e:
                if verbose:
                    print(f"[Sunbelt] Error page {page_num}: {e}")
                if page_num <= 5:
                    break
        
        listings = []
        for item in all_items:
            listings.append(format_listing(
                url=item['url'],
                broker_account=broker_account,
                title=item['title'],
                price=parse_money(item['price_text']),
                price_text=item['price_text'],
                location=item['location'],
                city=item['city'],
                state=item['state'],
                revenue=parse_money(item['rev_text']),
                cash_flow=parse_money(item['cf_text'])
            ))
        
        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} Sunbelt listings ({with_price} with price)")
        
        return listings


# ============================================================================
# VR BUSINESS BROKERS SCRAPER
# Standard URL pagination
# ============================================================================

class VRScraper:
    """
    VR Business Brokers
    https://www.vrbusinessbrokers.com

    Simple URL-based pagination. ~408 listings across 17 pages (24 per page).
    """

    BASE = "https://www.vrbusinessbrokers.com"
    LIST_URL = f"{BASE}/businesses-for-sale/"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")

    def _fetch_page(self, page_num: int) -> str:
        params = {
            'wpv_view_count': '35524',
            'wpv-wpcf-pretty-industry': '',
            'wpv-wpcf-price': '1000000000000000',
            'wpv-wpcf-cash-flow': '0',
            'wpv_post_search': '',
            'wpv-wpcf-type-of-location': '',
            'wpv-wpcf-year-established': '',
            'wpv_sort_orderby': 'field-wpcf-price',
            'wpv_sort_order': 'desc',
            'wpv-wpcf-listing-id': '',
            'wpv-relationship-filter': '0',
            'wpv_paged': str(page_num)
        }
        r = self.session.get(self.LIST_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.text

    def _parse_html(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        listings = []

        for box in soup.find_all('div', class_='vrbb-listing-box'):
            try:
                link = box.parent if box.parent and box.parent.name == 'a' else None
                if not link:
                    continue

                url = link.get('href')
                if not url:
                    continue
                if url.startswith('/'):
                    url = self.BASE + url

                title = box.find('div', class_='vrbb-listing-title')
                title = title.get_text(strip=True) if title else None

                price = box.find('div', class_='vrbb-listing-pretty-price')
                price_text = price.get_text(strip=True) if price else None

                loc = box.find('div', class_='vrbb-listing-loc')
                location = loc.get_text(strip=True) if loc else None

                industry = box.find('div', class_='vrbb-listing-pretty-industry-name')
                industry = industry.get_text(strip=True) if industry else None

                city, state = None, None
                if location:
                    if ',' in location:
                        parts = location.split(',')
                        city = parts[0].strip()
                        state = parts[1].strip()
                    else:
                        state = location.strip()

                listings.append({
                    'url': url,
                    'title': title,
                    'price_text': price_text,
                    'location': location,
                    'city': city,
                    'state': state,
                    'industry': industry
                })
            except:
                continue

        return listings

    def scrape(self, broker_account: str, max_pages: int = 20, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("VR Business Brokers")
            print('='*60)

        all_items = []
        seen_urls = set()

        for page_num in range(1, max_pages + 1):
            try:
                html = self._fetch_page(page_num)
                page_items = self._parse_html(html)

                new = [l for l in page_items if l['url'] not in seen_urls]
                for l in new:
                    seen_urls.add(l['url'])

                if verbose:
                    print(f"[VR] Page {page_num}: {len(page_items)} found, {len(new)} new | Total: {len(all_items) + len(new)}")

                all_items.extend(new)

                if not page_items or not new:
                    break

                time.sleep(random.uniform(1, 2))

            except Exception as e:
                if verbose:
                    print(f"[VR] Error page {page_num}: {e}")
                break

        listings = []
        for item in all_items:
            listings.append(format_listing(
                url=item['url'],
                broker_account=broker_account,
                title=item['title'],
                price=parse_money(item['price_text']),
                price_text=item['price_text'],
                location=item['location'],
                city=item['city'],
                state=item['state'],
                business_type=item['industry']
            ))

        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} VR listings ({with_price} with price)")

        return listings

# ============================================================================
# FCBB (FIRST CHOICE BUSINESS BROKERS) SCRAPER
# Has a clean JSON API - fastest scraper
# ============================================================================

class FCBBScraper:
    """
    First Choice Business Brokers
    https://fcbb.com
    
    Clean JSON API endpoint. Very fast.
    """
    
    BASE = "https://fcbb.com"
    API_URL = "https://api.fcbb.com/Fcbb/GetListings"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        self.session.headers.update({
            "Content-Type": "application/json",
            "application_api_key": "fcbb.web.api.token1",
            "website_external_id": "external.corporate.site.100001",
            "website_reference_id": "reference.corporate.site.100001"
        })

    def _fetch_page(self, page_num: int) -> Dict:
        payload = {
            "location": "",
            "sort": "",
            "keyword": "",
            "pricefrom": "",
            "priceto": "",
            "category": [""],
            "page": page_num,
            "pagesize": "10"
        }
        r = self.session.post(self.API_URL, json=payload, timeout=30)
        r.raise_for_status()
        return r.json()

    def scrape(self, broker_account: str, max_pages: int = 79, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("First Choice Business Brokers (FCBB)")
            print('='*60)
        
        all_items = []
        seen_ids = set()
        
        for page_num in range(1, max_pages + 1):
            try:
                data = self._fetch_page(page_num)
                
                if not data.get('Success'):
                    break
                
                items = data.get('Items', [])
                total_pages = data.get('TotalPages', 0)
                
                if page_num == 1 and verbose:
                    print(f"[FCBB] {data.get('TotalItems', 0)} total listings")
                
                new = [i for i in items if i.get('BusinessListingID') not in seen_ids]
                for i in new:
                    seen_ids.add(i.get('BusinessListingID'))
                
                if verbose and (page_num % 10 == 0):
                    print(f"[FCBB] Page {page_num}/{total_pages}: {len(all_items) + len(new)} total")
                
                all_items.extend(new)
                
                if not items:
                    break
                
                time.sleep(random.uniform(0.3, 0.7))
                
            except Exception as e:
                if verbose:
                    print(f"[FCBB] Error page {page_num}: {e}")
                break
        
        listings = []
        for item in all_items:
            url = item.get('ListingUrl')
            if url and url.startswith('/'):
                url = self.BASE + url
            
            listings.append(format_listing(
                url=url or f"{self.BASE}/listing/{item.get('BusinessListingID')}",
                broker_account=broker_account,
                title=item.get('BusinessName'),
                price=float(item.get('ListingPrice')) if item.get('ListingPrice') else None,
                price_text=f"${item.get('ListingPrice'):,.0f}" if item.get('ListingPrice') else None,
                location=item.get('BusinessLocation'),
                state=item.get('BusinessLocation'),
                description=item.get('BusinessDescription'),
                revenue=float(item.get('GrossSales')) if item.get('GrossSales') else None,
                cash_flow=float(item.get('TotalIncome')) if item.get('TotalIncome') else None
            ))
        
        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} FCBB listings ({with_price} with price)")
        
        return listings


# ============================================================================
# LINK BUSINESS SCRAPER
# HTML pagination with "Refer to Broker" filtering
# ============================================================================
class LinkBusinessScraper:
    """
    Link Business
    https://linkbusiness.com

    Standard URL pagination: /businesses-for-sale?page=N
    ~483 listings across ~50 pages.
    """

    BASE = "https://linkbusiness.com"
    LIST_URL = f"{BASE}/businesses-for-sale"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        self.session.headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _fetch_page(self, page_num: int) -> str:
        params = {'page': page_num} if page_num > 1 else {}
        r = self.session.get(self.LIST_URL, params=params, timeout=30)
        r.raise_for_status()
        return r.text

    def _parse_html(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        listings = []

        # Each listing card is inside a div with id starting with 'bcid_'
        cards = soup.find_all('div', id=re.compile(r'^bcid_'))

        for card in cards:
            try:
                # Title and URL
                listing_div = card.find('div', class_='vertical-listing')
                if not listing_div:
                    continue

                h3 = listing_div.find('h3')
                if not h3:
                    continue

                link = h3.find('a')
                if not link:
                    continue

                title = link.get_text(strip=True)
                href = link.get('href', '')
                url = href if href.startswith('http') else f"{self.BASE}{href}"

                if not title or not url:
                    continue

                # Price — first p.price
                price_text = None
                price_elems = listing_div.find_all('p', class_='price')
                for pe in price_elems:
                    txt = pe.get_text(strip=True)
                    txt = re.sub(r"^Price:\s*", "", txt).strip()
                    if txt and 'refer to broker' not in txt.lower() and '$' in txt:
                        price_text = txt
                        break

                # Sub-prices (profit/sales) — p.sub-price elements
                sub_prices = listing_div.find_all('p', class_='sub-price')
                cf_text = None
                revenue_text = None
                for sp in sub_prices:
                    txt = sp.get_text(strip=True)
                    if 'refer to broker' in txt.lower():
                        continue
                    if 'profit' in txt.lower() and '$' in txt:
                        m = re.search(r'\$[\d,]+', txt)
                        if m:
                            cf_text = m.group(0)
                    elif 'sales' in txt.lower() and '$' in txt:
                        m = re.search(r'\$[\d,]+', txt)
                        if m:
                            revenue_text = m.group(0)

                # Location
                location = None
                loc_elem = listing_div.find('p', class_='location')
                if loc_elem:
                    location = re.sub(r"^Location:\s*", "", loc_elem.get_text(strip=True)).strip()

                city, state = extract_city_state(location or '')

                listings.append({
                    'url': url,
                    'title': title,
                    'price_text': price_text,
                    'cf_text': cf_text,
                    'revenue_text': revenue_text,
                    'location': location,
                    'city': city,
                    'state': state
                })

            except Exception:
                continue

        return listings

    def scrape(self, broker_account: str, max_pages: int = 60, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Link Business")
            print('='*60)

        all_items = []
        seen_urls = set()

        for page_num in range(1, max_pages + 1):
            try:
                html = self._fetch_page(page_num)
                page_items = self._parse_html(html)

                new = [l for l in page_items if l['url'] not in seen_urls]
                for l in new:
                    seen_urls.add(l['url'])

                if verbose:
                    print(f"[Link] Page {page_num}: {len(page_items)} found, {len(new)} new | Total: {len(all_items) + len(new)}")

                all_items.extend(new)

                if not page_items or not new:
                    break

                time.sleep(random.uniform(1, 2))

            except Exception as e:
                if verbose:
                    print(f"[Link] Error page {page_num}: {e}")
                break

        listings = []
        for item in all_items:
            listings.append(format_listing(
                url=item['url'],
                broker_account=broker_account,
                title=item['title'],
                price=parse_money(item['price_text']),
                price_text=item['price_text'],
                location=item['location'],
                city=item['city'],
                state=item['state'],
                revenue=parse_money(item['revenue_text']),
                cash_flow=parse_money(item['cf_text'])
            ))

        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} Link Business listings ({with_price} with price)")

        return listings

# ============================================================================
# LARRY BODNER / EXECUTIVE BUSINESS BROKERS SCRAPER
# Table-based, requires Selenium for session handling
# ============================================================================

class LarryBodnerScraper:
    """
    Executive Business Brokers (Larry Bodner)
    https://execbb.com

    POSTs the buyer search form and parses the HTML results table directly.
    No Selenium needed (curl_cffi is sufficient).

    Structure notes (verified 2026-07 — the site changed since April):
    - results.asp with AllListings=ON returns the ENTIRE national listing set
      (~1,050) in one response, regardless of the State param, so a single
      POST is enough — no per-state loop.
    - Each listing spans two <tr> rows inside div.scrolllistings:
        * Title row : one <a href="listingdetail.asp?listingid=<id>, <cat>">
                      whose text is "<id>, <cat> <business type>".
        * Detail row (next sibling <tr>, 5 <td>):
            td[1] = Highlights/description
            td[2] = asking price
            td[3] = cash flow   (NOT revenue — this was mis-mapped before)
            td[4] = location "County, ST"
    - The href carries a trailing ", <cat>"; we keep only the numeric id so
      the detail URL is clean and stable.
    """

    BASE = "https://execbb.com"
    SEARCH_URL = f"{BASE}/buyer/sub/search.asp"
    RESULTS_URL = f"{BASE}/Buyer/sub/results.asp?searchtype=incsearch"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        try:
            self.session.get(self.SEARCH_URL, timeout=15)
        except Exception as e:
            print(f"[Bodner] Session init warning: {e}")

    @staticmethod
    def _clean(text: Optional[str]) -> Optional[str]:
        if text is None:
            return None
        t = text.replace('\xa0', ' ').strip()
        if t in ('', '-', '--', '-, --', 'Undisclosed'):
            return None
        return t

    def _detail_url(self, href: str) -> Optional[str]:
        """Build a clean listing-detail URL, keeping only the numeric id."""
        if not href:
            return None
        m = re.search(r'listingid=(\d+)', href)
        if not m:
            return None
        return f"{self.BASE}/Buyer/sub/listingdetail.asp?listingid={m.group(1)}"

    def _fetch_all(self) -> List[Dict]:
        data = {
            'Category': 'all',
            'State': '',
            'County': '0',
            'AllListings': 'ON',
            'searchtype': 'IncSearch',
        }
        try:
            r = self.session.post(self.RESULTS_URL, data=data, timeout=45)
            r.raise_for_status()
        except Exception as e:
            print(f"[Bodner] Error fetching results: {e}")
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        listing_links = [
            a for a in soup.find_all('a', href=True)
            if 'listingdetail' in a.get('href', '').lower()
        ]
        listings = []

        for link in listing_links:
            try:
                href = link.get('href', '')
                url = self._detail_url(href)
                if not url:
                    continue

                raw = link.get_text(strip=True)  # "<id>, <cat> <business type>"
                m = re.match(r'^\s*\d+\s*,\s*\d+\s*(.*)$', raw)
                business_type = (m.group(1).strip() if m else raw) or None
                title = business_type

                title_row = link.find_parent('tr')
                if not title_row:
                    continue
                detail_row = title_row.find_next_sibling('tr')
                cells = detail_row.find_all('td') if detail_row else []

                def cell(i):
                    return cells[i].get_text(' ', strip=True) if len(cells) > i else None

                description = self._clean(cell(1))
                if description and description.lower().startswith('highlights'):
                    description = re.sub(r'^highlights\s*:\s*', '', description, flags=re.I).strip() or None
                price_text = self._clean(cell(2))
                cash_flow_text = self._clean(cell(3))
                location = self._clean(cell(4))

                city, st = extract_city_state(location) if location else (None, None)
                # Location comes as "County, ST"; the first token is a county,
                # not a city — only trust the 2-letter state code.
                if not st and location and ',' in location:
                    tail = location.split(',')[-1].strip()
                    if len(tail) == 2 and tail.isalpha():
                        st = tail.upper()

                listings.append({
                    'url': url,
                    'title': title,
                    'business_type': business_type,
                    'price_text': price_text,
                    'cash_flow_text': cash_flow_text,
                    'location': location,
                    'city': None,
                    'state': st,
                    'description': description,
                })
            except Exception:
                continue
        return listings

    def scrape(self, broker_account: str, headless: bool = True, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Executive Business Brokers (Larry Bodner)")
            print('='*60)

        raw_items = self._fetch_all()

        # Deduplicate by clean listing URL
        all_items = []
        seen_urls = set()
        for it in raw_items:
            if it['url'] in seen_urls:
                continue
            seen_urls.add(it['url'])
            all_items.append(it)

        if verbose:
            print(f"[Bodner] Fetched {len(all_items)} unique listings (national feed)")

        listings = []
        for item in all_items:
            listings.append(format_listing(
                url=item['url'],
                broker_account=broker_account,
                title=item['title'],
                price=parse_money(item['price_text']),
                price_text=item['price_text'],
                location=item['location'],
                city=item['city'],
                state=item['state'],
                description=item['description'],
                business_type=item['business_type'],
                cash_flow=parse_money(item['cash_flow_text']),
            ))

        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            with_cf = sum(1 for l in listings if l.get('cash_flow'))
            print(f"\n✓ {len(listings)} Larry Bodner listings "
                  f"({with_price} with price, {with_cf} with cash flow)")

        return listings


# ============================================================================
# WE SELL RESTAURANTS SCRAPER
# National corporate feed via the Next.js JSON API (curl_cffi)
# ============================================================================

class WeSellRestaurantsScraper:
    """
    We Sell Restaurants
    https://www.wesellrestaurants.com

    wesellrestaurants.com is a Next.js SPA whose listings load from a JSON
    API. The full national feed comes from POST /searchFilter with an empty
    body and Laravel-style ?page=N pagination (per_page=51, ~1,300+ listings).

    Per-record fields used:
        id            -> listing id (part of detail URL)
        slug_url      -> URL slug (part of detail URL)
        bname         -> business name / title
        bsaleprice    -> asking price
        bcity/bstate  -> location
        listing_bat.grossSales     -> revenue
        listing_bat.owner_benefits -> cash flow (SDE)

    Detail URL pattern (verified against the live site):
        https://www.wesellrestaurants.com/restaurant-for-sale/{slug_url}/{id}
    """

    BASE = "https://www.wesellrestaurants.com"
    API_BASE = "https://api.wesellrestaurants.com/wsr-rebuild-prod/api"
    SEARCH_URL = f"{API_BASE}/searchFilter"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome131")
        self.api_headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json",
            "Origin": self.BASE,
            "Referer": f"{self.BASE}/",
        }

    def _fetch_page(self, page_num: int) -> Optional[Dict]:
        """POST to /searchFilter?page=N with an empty body; returns the
        Laravel paginator object under the top-level 'data' key."""
        for attempt in range(3):
            try:
                r = self.session.post(
                    f"{self.SEARCH_URL}?page={page_num}",
                    headers=self.api_headers,
                    json={},
                    timeout=45,
                )
                r.raise_for_status()
                payload = r.json()
                return payload.get("data") or {}
            except Exception as e:
                if attempt == 2:
                    print(f"[WeSell] Error page {page_num}: {e}")
                    return None
                time.sleep(1.5 * (attempt + 1))
        return None

    def _detail_url(self, rec: Dict) -> Optional[str]:
        slug = (rec.get("slug_url") or "").strip().strip("/")
        rid = rec.get("id")
        if not slug or not rid:
            return None
        return f"{self.BASE}/restaurant-for-sale/{slug}/{rid}"

    @staticmethod
    def _to_number(val) -> Optional[float]:
        """Coerce API numeric fields (may be int, str, None, or 0) to a
        positive float, treating 0/blank as 'not disclosed'."""
        if val in (None, "", "0", 0):
            return None
        try:
            n = float(str(val).replace(",", "").replace("$", "").strip())
            return n if n > 0 else None
        except (ValueError, TypeError):
            return None

    def scrape(self, broker_account: str, max_pages: int = 40, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("We Sell Restaurants (national corporate feed)")
            print('='*60)

        all_items = []
        seen_ids = set()
        last_page = None

        page_num = 1
        while page_num <= max_pages:
            data = self._fetch_page(page_num)
            if not data:
                break

            records = data.get("data", []) or []
            if last_page is None:
                last_page = data.get("last_page")
                if verbose:
                    print(f"[WeSell] {data.get('total', '?')} total listings "
                          f"across {last_page} pages (per_page={data.get('per_page')})")

            new = [r for r in records if r.get("id") not in seen_ids]
            for r in new:
                seen_ids.add(r.get("id"))
            all_items.extend(new)

            if verbose and (page_num == 1 or page_num % 5 == 0 or page_num == last_page):
                print(f"[WeSell] Page {page_num}/{last_page or '?'}: "
                      f"{len(new)} new | Total: {len(all_items)}")

            if not records:
                break
            if last_page and page_num >= last_page:
                break

            page_num += 1
            time.sleep(random.uniform(0.4, 0.9))

        listings = []
        for rec in all_items:
            url = self._detail_url(rec)
            if not url:
                continue
            bat = rec.get("listing_bat") or {}
            city = (rec.get("bcity") or "").strip() or None
            state = (rec.get("bstate") or "").strip() or None
            location = ", ".join([p for p in (city, state) if p]) or None
            title = (rec.get("bheadlinead") or rec.get("bname")
                     or rec.get("burldes") or "").strip() or None

            listings.append(format_listing(
                url=url,
                broker_account=broker_account,
                title=title,
                price=self._to_number(rec.get("bsaleprice")),
                location=location,
                city=city,
                state=state,
                description=rec.get("bmetadescription") or rec.get("burldes"),
                revenue=self._to_number(bat.get("grossSales")),
                cash_flow=self._to_number(bat.get("owner_benefits")),
            ))

        if verbose:
            with_price = sum(1 for l in listings if l.get("price"))
            print(f"\n✓ {len(listings)} We Sell Restaurants listings ({with_price} with price)")

        return listings


# ============================================================================
# ROUTER - Auto-detect and route to correct scraper
# ============================================================================

def get_specialized_broker_names() -> List[str]:
    """Return list of broker names that have specialized scrapers."""
    return [
        'Murphy Business',
        'Hedgestone Business Advisors',
        'Transworld Business Advisors',
        'Sunbelt Business Brokers',
        'VR Business Brokers',
        'First Choice Business Brokers (FCBB)',
        'Link Business',
        'Executive Business Brokers (Larry Bodner)'
    ]


def scrape_specialized_broker(broker: Dict, verbose: bool = True) -> Optional[List[Dict]]:
    """
    Auto-detect broker type and route to appropriate scraper.
    
    Args:
        broker: Dict with 'account', 'name', 'url' keys
        verbose: Print progress
    
    Returns:
        List of listings, or None if not a specialized broker
    """
    name = (broker.get('name') or '').lower()
    url = (broker.get('url') or '').lower()
    account = str(broker.get('account'))
    
    # Larry Bodner / Executive BB
    if 'execbb.com' in url or 'bodner' in name:
        return LarryBodnerScraper().scrape(broker_account=account, verbose=verbose)
    
    # Link Business
    if 'linkbusiness' in url or 'link business' in name:
        return LinkBusinessScraper().scrape(broker_account=account, verbose=verbose)
    
    # Murphy
    if 'murphy' in name or 'murphybusiness.com' in url:
        return MurphyScraper.scrape(broker_account=account, verbose=verbose)
    
    # Hedgestone
    if 'hedgestone' in name or 'hedgestone.com' in url:
        return HedgestoneScraper().scrape(broker_account=account, verbose=verbose)
    
    # Transworld
    if 'transworld' in name or 'tworld.com' in url:
        return TransworldScraper().scrape(broker_account=account, verbose=verbose)
    
    # Sunbelt
    if 'sunbelt' in name or 'sunbeltnetwork.com' in url:
        return SunbeltScraper().scrape(broker_account=account, verbose=verbose)
    
    # VR Business Brokers
    if 'vr business' in name or 'vrbbusa.com' in url or 'vrbusinessbrokers' in url:
        return VRScraper().scrape(broker_account=account, verbose=verbose)
    
    # FCBB
    if 'first choice' in name or 'fcbb' in name or 'fcbb.com' in url:
        return FCBBScraper().scrape(broker_account=account, verbose=verbose)

    # We Sell Restaurants
    if 'we sell restaurant' in name or 'wesellrestaurants.com' in url:
        return WeSellRestaurantsScraper().scrape(broker_account=account, verbose=verbose)

    # Not a specialized broker
    return None


# ============================================================================
# CLI for testing individual scrapers
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test specialized scrapers")
    parser.add_argument("broker", choices=[
        'murphy', 'hedgestone', 'transworld', 'sunbelt',
        'vr', 'fcbb', 'link', 'bodner', 'wesell', 'all'
    ])
    parser.add_argument("--account", default="test-123", help="Broker account ID")
    
    args = parser.parse_args()
    
    scrapers = {
        'murphy': lambda: MurphyScraper.scrape(args.account, max_pages=50),
        'hedgestone': lambda: HedgestoneScraper().scrape(args.account, max_pages=15),
        'transworld': lambda: TransworldScraper().scrape(args.account, max_pages=385),
        'sunbelt': lambda: SunbeltScraper().scrape(args.account, max_pages=130),
        'vr': lambda: VRScraper().scrape(args.account, max_pages=15),
        'fcbb': lambda: FCBBScraper().scrape(args.account, max_pages=79),
        'link': lambda: LinkBusinessScraper().scrape(args.account, max_pages=20),
        'bodner': lambda: LarryBodnerScraper().scrape(args.account),
        'wesell': lambda: WeSellRestaurantsScraper().scrape(args.account, max_pages=40),
    }
    
    if args.broker == 'all':
        for name, scraper_fn in scrapers.items():
            print(f"\n{'#'*60}")
            print(f"# Testing: {name.upper()}")
            print(f"{'#'*60}")
            try:
                listings = scraper_fn()
                print(f"Result: {len(listings)} listings")
            except Exception as e:
                print(f"Error: {e}")
    else:
        listings = scrapers[args.broker]()
        print(f"\nTotal: {len(listings)} listings")
        if listings:
            print(f"Sample: {listings[0]}")