"""
DealLedger Specialized Scrapers
================================
Custom scrapers for major franchise brokers that need specific handling.
Standalone version - no external database dependencies.

Brokers supported:
- Murphy Business & Financial Corporation
- Transworld Business Advisors
- Sunbelt Business Brokers
- VR Business Brokers
- First Choice Business Brokers (FCBB)
- Hedgestone Business Advisors
- Link Business
- Executive Business Brokers (Larry Bodner)
"""

import hashlib
import re
import json
import time
import random
from typing import List, Dict, Optional
from datetime import datetime, timezone
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

# Selenium imports (for Murphy, Hedgestone, Larry Bodner)
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    print("Warning: selenium not installed. Murphy, Hedgestone, and Larry Bodner scrapers will not work.")

# curl_cffi imports (for Transworld, Sunbelt, VR, FCBB, Link)
try:
    from curl_cffi import requests as curl_requests
    CURL_AVAILABLE = True
except ImportError:
    CURL_AVAILABLE = False
    print("Warning: curl_cffi not installed. Transworld, Sunbelt, VR, FCBB, Link scrapers will not work.")
    # Fallback to regular requests
    import requests as curl_requests


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def parse_money_value(text: str) -> Optional[float]:
    """Parse money values like $12m, $1.5M, $500k, $1,234,567"""
    if not text:
        return None
    try:
        cleaned = text.replace('$', '').replace(',', '').strip().lower()
        if 'm' in cleaned:
            return float(cleaned.replace('m', '')) * 1000000
        if 'k' in cleaned:
            return float(cleaned.replace('k', '')) * 1000
        return float(cleaned)
    except:
        return None


def extract_city_state(location: str) -> tuple:
    """Extract city and state from location string"""
    if not location:
        return None, None
    
    m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b', location)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    
    m2 = re.search(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', location)
    if m2:
        return m2.group(1).strip(), m2.group(2).strip()
    
    state_match = re.search(r'\b([A-Z]{2})\b', location)
    if state_match:
        return None, state_match.group(1)
    
    return None, None


def generate_listing_id(url: str) -> str:
    """Generate a stable listing ID from URL"""
    return f"dl_{hashlib.md5(url.encode()).hexdigest()[:12]}"


def normalize_listing(raw: Dict, broker_id: str, broker_name: str) -> Dict:
    """Normalize a raw listing to DealLedger schema"""
    source_url = raw.get('listing_url') or raw.get('url') or ''
    
    return {
        'id': generate_listing_id(source_url),
        'source_url': source_url,
        'broker_id': broker_id,
        'broker_name': broker_name,
        'title': raw.get('title'),
        'asking_price': int(raw.get('price')) if raw.get('price') else None,
        'price_text': raw.get('price_text'),
        'revenue': int(raw.get('revenue')) if raw.get('revenue') else None,
        'cash_flow': int(raw.get('cash_flow')) if raw.get('cash_flow') else None,
        'location': raw.get('location'),
        'city': raw.get('city'),
        'state': raw.get('state'),
        'country': 'US',
        'description': raw.get('description') or raw.get('full_text', '')[:500],
        'vertical': classify_vertical(f"{raw.get('title', '')} {raw.get('description', '')}"),
        'first_seen': datetime.now(timezone.utc).isoformat(),
        'last_seen': datetime.now(timezone.utc).isoformat(),
        'status': 'active'
    }


def classify_vertical(text: str) -> str:
    """Classify listing into a vertical"""
    text_lower = (text or "").lower()
    
    verticals = {
        'cleaning': ['cleaning', 'janitorial', 'custodial', 'maid', 'housekeeping'],
        'laundromat': ['laundromat', 'laundry', 'coin laundry'],
        'vending': ['vending', 'atm route', 'amusement'],
        'hvac': ['hvac', 'heating', 'cooling', 'air conditioning'],
        'landscaping': ['landscape', 'lawn care', 'tree service'],
        'pool': ['pool service', 'pool cleaning'],
        'pest': ['pest control', 'exterminator'],
        'restaurant': ['restaurant', 'cafe', 'bar', 'food service'],
        'retail': ['retail', 'store', 'shop'],
        'automotive': ['auto repair', 'car wash', 'mechanic'],
    }
    
    for vertical, keywords in verticals.items():
        if any(kw in text_lower for kw in keywords):
            return vertical
    
    return 'other'


# ============================================================================
# MURPHY BUSINESS SCRAPER
# ============================================================================

class MurphyScraper:
    """Murphy Business & Financial Corporation scraper"""
    
    BASE = "https://murphybusiness.com"
    LIST = BASE + "/business-brokerage/view-our-listings/"
    SDE_RE = re.compile(r"SDE:\s*\$([\d,]+)", re.I)
    NUM_RE = re.compile(r"[\d,.]+")

    @staticmethod
    def _to_num(s):
        if not s:
            return None
        m = MurphyScraper.NUM_RE.search(s)
        if not m:
            return None
        v = m.group(0).replace(",", "")
        try:
            return float(v)
        except:
            return None

    @staticmethod
    def _parse_cards(driver):
        out = []
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.card-body"))
            )
        except:
            return []
        
        time.sleep(2)
        cards = driver.find_elements(By.CSS_SELECTOR, "div.card-body")
        
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
            
            city, state = extract_city_state(location)
            
            out.append({
                "title": title,
                "price": MurphyScraper._to_num(price_txt),
                "price_text": price_txt,
                "cash_flow": MurphyScraper._to_num(sde_txt),
                "location": location,
                "city": city,
                "state": state,
                "listing_url": detail_url,
                "full_text": txt
            })
        
        return out

    @staticmethod
    def scrape(broker_id: str, broker_name: str = "Murphy Business", max_pages=50, 
               headless=True, verbose=True) -> List[Dict]:
        
        if not SELENIUM_AVAILABLE:
            print("Murphy scraper requires selenium. Install with: pip install selenium webdriver-manager")
            return []
        
        options = Options()
        if headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        
        listings = []
        seen_urls = set()
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"Murphy Business & Financial Corporation")
            print(f"{'='*60}")
        
        try:
            driver.get(MurphyScraper.LIST)
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.card-body"))
            )
            time.sleep(3)
            
            page_num = 1
            consecutive_dupes = 0
            
            while page_num <= max_pages:
                page_rows = MurphyScraper._parse_cards(driver)
                
                if not page_rows:
                    break
                
                new = [r for r in page_rows if r.get("listing_url") and r["listing_url"] not in seen_urls]
                for r in new:
                    seen_urls.add(r["listing_url"])
                
                if verbose:
                    print(f"  Page {page_num}: {len(page_rows)} cards (+{len(new)} new)")
                
                if len(new) == 0:
                    consecutive_dupes += 1
                    if consecutive_dupes >= 5:
                        break
                else:
                    consecutive_dupes = 0
                    for raw in new:
                        listings.append(normalize_listing(raw, broker_id, broker_name))
                
                # Click next page
                try:
                    next_btn = driver.find_element(By.CSS_SELECTOR, "li.page-item:last-child a.page-link")
                    if 'disabled' in next_btn.get_attribute('class') or not next_btn.is_enabled():
                        break
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(2)
                    page_num += 1
                except:
                    break
            
            if verbose:
                print(f"✓ Murphy: {len(listings)} listings")
            
            return listings
            
        except Exception as e:
            if verbose:
                print(f"✗ Murphy error: {str(e)[:100]}")
            return []
        finally:
            driver.quit()


# ============================================================================
# TRANSWORLD SCRAPER
# ============================================================================

class TransworldScraper:
    """Transworld Business Advisors scraper"""
    
    BASE = "https://www.tworld.com"
    API = f"{BASE}/api/v1/listings"
    
    def __init__(self):
        self.session = None
        self.lock = Lock()
    
    def _get_session(self):
        if not self.session:
            if CURL_AVAILABLE:
                self.session = curl_requests.Session(impersonate="chrome")
            else:
                import requests
                self.session = requests.Session()
            self.session.headers.update({
                'Accept': 'application/json',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0'
            })
        return self.session
    
    def _fetch_page(self, page: int) -> List[Dict]:
        try:
            session = self._get_session()
            resp = session.get(f"{self.API}?page={page}&per_page=24", timeout=30)
            if resp.status_code != 200:
                return []
            data = resp.json()
            return data.get('listings', data.get('data', []))
        except:
            return []
    
    def scrape(self, broker_id: str, broker_name: str = "Transworld Business Advisors",
               max_pages=150, workers=8, verbose=True) -> List[Dict]:
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"Transworld Business Advisors")
            print(f"{'='*60}")
        
        listings = []
        seen_ids = set()
        
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(self._fetch_page, p): p for p in range(1, max_pages + 1)}
            
            for future in as_completed(futures):
                page_num = futures[future]
                try:
                    page_listings = future.result()
                    
                    for item in page_listings:
                        lid = item.get('id') or item.get('listing_id')
                        if lid and lid not in seen_ids:
                            seen_ids.add(lid)
                            
                            raw = {
                                'title': item.get('title'),
                                'price': parse_money_value(str(item.get('asking_price', ''))),
                                'price_text': item.get('asking_price'),
                                'cash_flow': parse_money_value(str(item.get('cash_flow', ''))),
                                'revenue': parse_money_value(str(item.get('revenue', ''))),
                                'location': item.get('location'),
                                'city': item.get('city'),
                                'state': item.get('state'),
                                'listing_url': f"{self.BASE}/listing/{lid}",
                                'description': item.get('description', '')[:500]
                            }
                            listings.append(normalize_listing(raw, broker_id, broker_name))
                    
                    if verbose and page_num % 10 == 0:
                        print(f"  Page {page_num}: {len(listings)} total")
                        
                except Exception as e:
                    continue
        
        if verbose:
            print(f"✓ Transworld: {len(listings)} listings")
        
        return listings


# ============================================================================
# SUNBELT SCRAPER
# ============================================================================

class SunbeltScraper:
    """Sunbelt Business Brokers scraper"""
    
    BASE = "https://www.sunbeltnetwork.com"
    API = f"{BASE}/api/listings"
    
    def scrape(self, broker_id: str, broker_name: str = "Sunbelt Business Brokers",
               max_pages=130, verbose=True) -> List[Dict]:
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"Sunbelt Business Brokers")
            print(f"{'='*60}")
        
        listings = []
        seen_ids = set()
        
        if CURL_AVAILABLE:
            session = curl_requests.Session(impersonate="chrome")
        else:
            import requests
            session = requests.Session()
        
        session.headers.update({
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0'
        })
        
        for page in range(1, max_pages + 1):
            try:
                resp = session.get(f"{self.API}?page={page}", timeout=30)
                if resp.status_code != 200:
                    break
                
                data = resp.json()
                page_listings = data.get('listings', data.get('data', []))
                
                if not page_listings:
                    break
                
                for item in page_listings:
                    lid = item.get('id')
                    if lid and lid not in seen_ids:
                        seen_ids.add(lid)
                        
                        raw = {
                            'title': item.get('title'),
                            'price': parse_money_value(str(item.get('asking_price', ''))),
                            'cash_flow': parse_money_value(str(item.get('cash_flow', ''))),
                            'revenue': parse_money_value(str(item.get('revenue', ''))),
                            'location': item.get('location'),
                            'city': item.get('city'),
                            'state': item.get('state'),
                            'listing_url': f"{self.BASE}/listing/{lid}",
                            'description': item.get('description', '')[:500]
                        }
                        listings.append(normalize_listing(raw, broker_id, broker_name))
                
                if verbose and page % 20 == 0:
                    print(f"  Page {page}: {len(listings)} total")
                
                time.sleep(0.5)
                
            except Exception as e:
                break
        
        if verbose:
            print(f"✓ Sunbelt: {len(listings)} listings")
        
        return listings


# ============================================================================
# VR BUSINESS BROKERS SCRAPER
# ============================================================================

class VRScraper:
    """VR Business Brokers scraper"""
    
    BASE = "https://www.vrbusinessbrokers.com"
    
    def scrape(self, broker_id: str, broker_name: str = "VR Business Brokers",
               max_pages=15, verbose=True) -> List[Dict]:
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"VR Business Brokers")
            print(f"{'='*60}")
        
        listings = []
        
        if CURL_AVAILABLE:
            session = curl_requests.Session(impersonate="chrome")
        else:
            import requests
            session = requests.Session()
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0'
        })
        
        try:
            from bs4 import BeautifulSoup
            
            for page in range(1, max_pages + 1):
                url = f"{self.BASE}/businesses-for-sale?page={page}"
                resp = session.get(url, timeout=30)
                
                if resp.status_code != 200:
                    break
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                cards = soup.select('.listing-card, .business-listing, article.listing')
                
                if not cards:
                    break
                
                for card in cards:
                    try:
                        title_el = card.select_one('h2, h3, .title, .listing-title')
                        link_el = card.select_one('a[href*="/listing/"], a[href*="/business/"]')
                        price_el = card.select_one('.price, .asking-price')
                        location_el = card.select_one('.location, .city-state')
                        
                        if not link_el:
                            continue
                        
                        listing_url = link_el.get('href', '')
                        if not listing_url.startswith('http'):
                            listing_url = self.BASE + listing_url
                        
                        raw = {
                            'title': title_el.get_text(strip=True) if title_el else None,
                            'price': parse_money_value(price_el.get_text()) if price_el else None,
                            'location': location_el.get_text(strip=True) if location_el else None,
                            'listing_url': listing_url
                        }
                        
                        if raw.get('location'):
                            raw['city'], raw['state'] = extract_city_state(raw['location'])
                        
                        listings.append(normalize_listing(raw, broker_id, broker_name))
                        
                    except:
                        continue
                
                if verbose:
                    print(f"  Page {page}: {len(listings)} total")
                
                time.sleep(1)
                
        except Exception as e:
            if verbose:
                print(f"  Error: {str(e)[:100]}")
        
        if verbose:
            print(f"✓ VR Business: {len(listings)} listings")
        
        return listings


# ============================================================================
# FCBB SCRAPER
# ============================================================================

class FCBBScraper:
    """First Choice Business Brokers scraper"""
    
    BASE = "https://fcbb.com"
    
    def scrape(self, broker_id: str, broker_name: str = "First Choice Business Brokers",
               max_pages=79, verbose=True) -> List[Dict]:
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"First Choice Business Brokers")
            print(f"{'='*60}")
        
        listings = []
        
        if CURL_AVAILABLE:
            session = curl_requests.Session(impersonate="chrome")
        else:
            import requests
            session = requests.Session()
        
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/120.0.0.0'
        })
        
        try:
            from bs4 import BeautifulSoup
            
            for page in range(1, max_pages + 1):
                url = f"{self.BASE}/businesses-for-sale?page={page}"
                resp = session.get(url, timeout=30)
                
                if resp.status_code != 200:
                    break
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                cards = soup.select('.listing-card, .business-card, article')
                
                if not cards:
                    break
                
                for card in cards:
                    try:
                        title_el = card.select_one('h2, h3, .title')
                        link_el = card.select_one('a[href*="/listing/"], a[href*="/business/"]')
                        price_el = card.select_one('.price, .asking-price')
                        location_el = card.select_one('.location')
                        
                        if not link_el:
                            continue
                        
                        listing_url = link_el.get('href', '')
                        if not listing_url.startswith('http'):
                            listing_url = self.BASE + listing_url
                        
                        raw = {
                            'title': title_el.get_text(strip=True) if title_el else None,
                            'price': parse_money_value(price_el.get_text()) if price_el else None,
                            'location': location_el.get_text(strip=True) if location_el else None,
                            'listing_url': listing_url
                        }
                        
                        if raw.get('location'):
                            raw['city'], raw['state'] = extract_city_state(raw['location'])
                        
                        listings.append(normalize_listing(raw, broker_id, broker_name))
                        
                    except:
                        continue
                
                if verbose and page % 10 == 0:
                    print(f"  Page {page}: {len(listings)} total")
                
                time.sleep(0.5)
                
        except Exception as e:
            if verbose:
                print(f"  Error: {str(e)[:100]}")
        
        if verbose:
            print(f"✓ FCBB: {len(listings)} listings")
        
        return listings


# ============================================================================
# HEDGESTONE SCRAPER
# ============================================================================

class HedgestoneScraper:
    """Hedgestone Business Advisors scraper"""
    
    BASE = "https://www.hedgestone.com"
    LIST = f"{BASE}/businesses-for-sale/"
    
    def scrape(self, broker_id: str, broker_name: str = "Hedgestone Business Advisors",
               max_pages=15, headless=True, verbose=True) -> List[Dict]:
        
        if not SELENIUM_AVAILABLE:
            print("Hedgestone scraper requires selenium.")
            return []
        
        if verbose:
            print(f"\n{'='*60}")
            print(f"Hedgestone Business Advisors")
            print(f"{'='*60}")
        
        options = Options()
        if headless:
            options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        listings = []
        
        try:
            driver.get(self.LIST)
            time.sleep(3)
            
            # Parse listings from page
            cards = driver.find_elements(By.CSS_SELECTOR, ".listing-card, article, .business-listing")
            
            for card in cards:
                try:
                    title = card.find_element(By.CSS_SELECTOR, "h2, h3, .title").text.strip()
                    link = card.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                    
                    price_el = card.find_elements(By.CSS_SELECTOR, ".price, .asking-price")
                    price = parse_money_value(price_el[0].text) if price_el else None
                    
                    raw = {
                        'title': title,
                        'price': price,
                        'listing_url': link
                    }
                    listings.append(normalize_listing(raw, broker_id, broker_name))
                    
                except:
                    continue
            
            if verbose:
                print(f"✓ Hedgestone: {len(listings)} listings")
            
            return listings
            
        except Exception as e:
            if verbose:
                print(f"✗ Hedgestone error: {str(e)[:100]}")
            return []
        finally:
            driver.quit()


# ============================================================================
# ROUTER - AUTO-DETECT AND ROUTE TO CORRECT SCRAPER
# ============================================================================

SPECIALIZED_BROKERS = {
    'murphy': {'class': MurphyScraper, 'method': 'scrape', 'static': True},
    'transworld': {'class': TransworldScraper, 'method': 'scrape', 'static': False},
    'sunbelt': {'class': SunbeltScraper, 'method': 'scrape', 'static': False},
    'vr': {'class': VRScraper, 'method': 'scrape', 'static': False},
    'fcbb': {'class': FCBBScraper, 'method': 'scrape', 'static': False},
    'hedgestone': {'class': HedgestoneScraper, 'method': 'scrape', 'static': False},
}


def detect_specialized_broker(broker: Dict) -> Optional[str]:
    """Detect if a broker has a specialized scraper"""
    name = (broker.get('name') or '').lower()
    url = (broker.get('url') or '').lower()
    
    if 'murphy' in name or 'murphybusiness.com' in url:
        return 'murphy'
    if 'transworld' in name or 'tworld.com' in url:
        return 'transworld'
    if 'sunbelt' in name or 'sunbeltnetwork.com' in url:
        return 'sunbelt'
    if 'vr business' in name or 'vrbbusa.com' in url or 'vrbusinessbrokers' in url:
        return 'vr'
    if 'first choice' in name or 'fcbb' in name or 'fcbb.com' in url:
        return 'fcbb'
    if 'hedgestone' in name or 'hedgestone.com' in url:
        return 'hedgestone'
    
    return None


def scrape_specialized_broker(broker: Dict, verbose=True) -> Optional[List[Dict]]:
    """Auto-detect broker type and route to appropriate scraper"""
    broker_type = detect_specialized_broker(broker)
    
    if not broker_type:
        return None
    
    broker_id = str(broker.get('id') or broker.get('account', ''))
    broker_name = broker.get('name', broker_type.title())
    
    config = SPECIALIZED_BROKERS[broker_type]
    
    if config['static']:
        return config['class'].scrape(broker_id, broker_name, verbose=verbose)
    else:
        scraper = config['class']()
        return scraper.scrape(broker_id, broker_name, verbose=verbose)


def get_specialized_broker_names() -> List[str]:
    """Return list of broker names that have specialized scrapers"""
    return [
        'Murphy Business',
        'Transworld Business Advisors',
        'Sunbelt Business Brokers',
        'VR Business Brokers',
        'First Choice Business Brokers (FCBB)',
        'Hedgestone Business Advisors',
    ]


def is_specialized_broker(broker: Dict) -> bool:
    """Check if a broker has a specialized scraper"""
    return detect_specialized_broker(broker) is not None


# ============================================================================
# CLI TEST
# ============================================================================

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Test specialized scrapers")
    parser.add_argument("--broker", type=str, choices=['murphy', 'transworld', 'sunbelt', 'vr', 'fcbb', 'hedgestone'],
                       help="Specific broker to test")
    parser.add_argument("--all", action="store_true", help="Test all specialized scrapers")
    
    args = parser.parse_args()
    
    if args.broker:
        broker = {'id': 'test', 'name': args.broker, 'url': ''}
        listings = scrape_specialized_broker(broker, verbose=True)
        print(f"\nTotal: {len(listings) if listings else 0} listings")
        
    elif args.all:
        for broker_type in SPECIALIZED_BROKERS.keys():
            broker = {'id': 'test', 'name': broker_type, 'url': ''}
            listings = scrape_specialized_broker(broker, verbose=True)
            print(f"\n{broker_type}: {len(listings) if listings else 0} listings\n")
    else:
        print("Specialized scrapers available:")
        for name in get_specialized_broker_names():
            print(f"  - {name}")
        print("\nUse --broker <name> to test one, or --all to test all")
