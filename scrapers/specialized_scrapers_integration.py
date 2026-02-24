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
                    if consecutive_dupes >= 5:
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
                
                # Navigate to next page
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, f"a.page_number[data-page='{page_num + 1}']")
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
                    time.sleep(0.5)
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(4)
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
    
    Has an internal API endpoint - fast parallel scraping.
    ~2000+ listings.
    """
    
    BASE = "https://www.tworld.com"
    API_URL = f"{BASE}/api/listings"
    META_CSRF_RE = re.compile(r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', re.I)

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        self.api_headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": self.BASE,
            "Referer": f"{self.BASE}/listings/",
            "X-Requested-With": "XMLHttpRequest",
        }
        self.lock = Lock()
        self.seen = set()
        self._bootstrap()

    def _bootstrap(self):
        """Visit site to get CSRF tokens."""
        try:
            self.session.get(self.BASE, timeout=30)
            r = self.session.get(f"{self.BASE}/listings/", timeout=30)
            
            m = self.META_CSRF_RE.search(r.text)
            if m:
                self.api_headers["X-CSRF-TOKEN"] = m.group(1)
            
            xsrf = self.session.cookies.get("XSRF-TOKEN")
            if xsrf:
                from urllib.parse import unquote
                self.api_headers["X-XSRF-TOKEN"] = unquote(xsrf)
        except Exception as e:
            print(f"[Transworld] Bootstrap warning: {e}")

    def _fetch_page(self, page_num: int) -> List[Dict]:
        """Fetch a single page from the API."""
        payload = {
            "page": page_num,
            "per_page": 24,
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
                    data=json.dumps(payload),
                    timeout=45
                )
                
                if r.status_code == 419:
                    self._bootstrap()
                    continue
                
                r.raise_for_status()
                data = r.json()
                arr = data.get("results") or data.get("data") or []
                
                out = []
                with self.lock:
                    for item in arr:
                        key = item.get("id") or item.get("slug")
                        if key and key not in self.seen:
                            self.seen.add(key)
                            out.append(item)
                return out
                
            except Exception as e:
                if attempt == 2:
                    return []
                time.sleep(1 + attempt)
        return []

    def scrape(self, broker_account: str, max_pages: int = 150, workers: int = 8, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Transworld Business Advisors")
            print('='*60)
        
        all_items = []
        
        # First page
        first = self._fetch_page(1)
        all_items.extend(first)
        if verbose:
            print(f"[Transworld] Page 1: {len(first)} listings")
        
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
                    time.sleep(0.3)
        
        # Convert to standard format
        listings = []
        for item in all_items:
            location = item.get("location") or item.get("city_state")
            city, state = extract_city_state(location)
            
            price = item.get("c_listing_price__c") or item.get("price")
            cash_flow = item.get("c_discretionary_earnings__c")
            
            slug = item.get("slug", "")
            url = item.get("url") or f"{self.BASE}/listings/{slug}"
            
            listings.append(format_listing(
                url=url,
                broker_account=broker_account,
                title=item.get("name") or item.get("title"),
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
    
    Simple URL-based pagination.
    """
    
    BASE = "https://www.vrbusinessbrokers.com"
    LIST_URL = f"{BASE}/businesses-for-sale/"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")

    def _fetch_page(self, page_num: int) -> str:
        params = {
            'wpv_view_count': '35524',
            'wpv-wpcf-price': '1000000000000000',
            'wpv-wpcf-cash-flow': '0',
            'wpv_sort_orderby': 'field-wpcf-price',
            'wpv_sort_order': 'desc',
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

    def scrape(self, broker_account: str, max_pages: int = 15, verbose: bool = True) -> List[Dict]:
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
                    print(f"[VR] Page {page_num}: {len(new)} new | Total: {len(all_items) + len(new)}")
                
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
            print(f"\n✓ {len(listings)} VR listings")
        
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
    
    Filters out "Refer to Broker" listings (no useful data).
    """
    
    BASE = "https://linkbusiness.com"
    LIST_URL = f"{BASE}/businesses-for-sale/"

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")

    def _fetch_page(self, page_num: int) -> str:
        url = self.LIST_URL if page_num == 1 else f"{self.LIST_URL}page/{page_num}/"
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.text

    def _parse_html(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, 'html.parser')
        listings = []
        
        cards = soup.find_all('div', class_='featured-listing-item')
        
        for card in cards:
            try:
                title_tag = card.find('h3')
                if not title_tag:
                    continue
                title = title_tag.get_text(strip=True)
                
                link = card.find('a', href=True)
                if not link:
                    continue
                url = link.get('href')
                if url.startswith('/'):
                    url = self.BASE + url
                
                # Extract price - skip "Refer to Broker"
                price_text = None
                price_elem = card.find('p', class_='price')
                if price_elem:
                    txt = price_elem.get_text(strip=True)
                    if 'refer to broker' in txt.lower():
                        continue
                    match = re.search(r'\$[\d,]+', txt)
                    if match:
                        price_text = match.group(0)
                
                if not price_text:
                    continue
                
                # Revenue
                revenue_text = None
                sales = card.find(string=re.compile(r'Sales:', re.I))
                if sales:
                    txt = sales.parent.get_text()
                    if 'refer to broker' not in txt.lower():
                        match = re.search(r'\$[\d,]+', txt)
                        if match:
                            revenue_text = match.group(0)
                
                # Cash flow
                cf_text = None
                profit = card.find(string=re.compile(r'Profit', re.I))
                if profit:
                    txt = profit.parent.get_text()
                    if 'refer to broker' not in txt.lower():
                        match = re.search(r'\$[\d,]+', txt)
                        if match:
                            cf_text = match.group(0)
                
                # Location
                location = None
                loc_elem = card.find(string=re.compile(r'Location:', re.I))
                if loc_elem:
                    match = re.search(r'Location:\s*(.+)', loc_elem.parent.get_text(strip=True))
                    if match:
                        location = match.group(1).strip()
                
                city, state = extract_city_state(location)
                
                listings.append({
                    'url': url,
                    'title': title,
                    'price_text': price_text,
                    'revenue_text': revenue_text,
                    'cf_text': cf_text,
                    'location': location,
                    'city': city,
                    'state': state
                })
            except:
                continue
        
        return listings

    def scrape(self, broker_account: str, max_pages: int = 20, verbose: bool = True) -> List[Dict]:
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
                    print(f"[Link Business] Page {page_num:2d}/{max_pages}: {len(page_items):2d} found, {len(new):2d} new | Total: {len(all_items) + len(new)}")
                
                all_items.extend(new)
                
                if not page_items or not new:
                    break
                
                time.sleep(random.uniform(1, 2))
                
            except Exception as e:
                if verbose:
                    print(f"[Link Business] Error page {page_num}: {e}")
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
    
    800+ NJ listings. Requires Selenium for session cookies.
    """
    
    BASE = "https://execbb.com"
    SEARCH_URL = f"{BASE}/buyer/sub/search.asp"

    def scrape(self, broker_account: str, headless: bool = True, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Executive Business Brokers (Larry Bodner)")
            print('='*60)
        
        driver = create_chrome_driver(headless)
        listings = []
        
        try:
            # Must visit search page first to establish session
            driver.get(self.SEARCH_URL)
            time.sleep(2)
            
            # Submit search form
            try:
                submit = driver.find_element(By.CSS_SELECTOR, "input[type='submit'], input[type='image']")
                submit.click()
            except:
                driver.execute_script("document.forms[0].submit();")
            
            time.sleep(4)
            
            # Parse listings
            links = driver.find_elements(By.CSS_SELECTOR, "a[href*='listingdetail.asp']")
            
            if verbose:
                print(f"[Larry Bodner] Found {len(links)} listing links")
            
            for link in links:
                try:
                    url = link.get_attribute('href')
                    title = link.text.strip()
                    if not url or not title:
                        continue
                    
                    # Get detail row
                    title_row = link.find_element(By.XPATH, "./ancestor::tr")
                    detail_row = title_row.find_element(By.XPATH, "./following-sibling::tr[1]")
                    cells = detail_row.find_elements(By.TAG_NAME, "td")
                    
                    if len(cells) < 4:
                        continue
                    
                    revenue_text = cells[2].text.strip() if len(cells) > 2 else None
                    price_text = cells[3].text.strip() if len(cells) > 3 else None
                    location = cells[4].text.strip() if len(cells) > 4 else None
                    
                    # Clean up
                    if revenue_text in ['', 'Undisclosed', '\xa0']:
                        revenue_text = None
                    if price_text in ['', '\xa0']:
                        price_text = None
                    if location:
                        location = ' '.join(location.split())
                    
                    city, state = extract_city_state(location)
                    
                    # Business type from title
                    parts = title.split(' ', 1)
                    business_type = parts[1] if len(parts) > 1 else None
                    
                    listings.append(format_listing(
                        url=url,
                        broker_account=broker_account,
                        title=title,
                        price=parse_money(price_text),
                        price_text=price_text,
                        location=location,
                        city=city,
                        state=state,
                        business_type=business_type,
                        cash_flow=parse_money(revenue_text)
                    ))
                except:
                    continue
            
            if verbose:
                with_price = sum(1 for l in listings if l.get('price'))
                print(f"\n✓ {len(listings)} Larry Bodner listings ({with_price} with price)")
            
            return listings
            
        finally:
            driver.quit()


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
        'vr', 'fcbb', 'link', 'bodner', 'all'
    ])
    parser.add_argument("--account", default="test-123", help="Broker account ID")
    
    args = parser.parse_args()
    
    scrapers = {
        'murphy': lambda: MurphyScraper.scrape(args.account, max_pages=5),
        'hedgestone': lambda: HedgestoneScraper().scrape(args.account, max_pages=3),
        'transworld': lambda: TransworldScraper().scrape(args.account, max_pages=10),
        'sunbelt': lambda: SunbeltScraper().scrape(args.account, max_pages=10),
        'vr': lambda: VRScraper().scrape(args.account, max_pages=5),
        'fcbb': lambda: FCBBScraper().scrape(args.account, max_pages=10),
        'link': lambda: LinkBusinessScraper().scrape(args.account, max_pages=5),
        'bodner': lambda: LarryBodnerScraper().scrape(args.account),
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
