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
import os
import re
import json
import time
import random
from html import unescape as html_unescape
from typing import List, Dict, Optional
from datetime import date

try:
    from junk_filter import is_sold_or_pending, is_junk_title, title_from_slug
except Exception:                    # graceful no-ops if module unavailable
    def is_sold_or_pending(_title):
        return False

    def is_junk_title(_t, firm_name=None):
        return not _t or len(str(_t).strip()) < 6

    def title_from_slug(_url):
        return None
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
import requests as R_requests          # stdlib requests (curl_cffi shadows the name)
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


def _proxy_creds():
    """(user, password, host) for DataImpulse. Reads PROXY_USER/PROXY_PASS/
    PROXY_HOST, falling back to PROXY_URL (http://user:pass@host:port).
    GitHub Actions only sets the PROXY_URL secret, so without the fallback the
    Vested, FacetWP and WP-REST scrapers ran with NO proxy in CI (vestedbb then
    rate-limited the single runner IP and roughly half its pages failed).
    Mirrors dealledger_scraper_v6._load_proxy_creds()."""
    from urllib.parse import urlparse, unquote
    user = os.environ.get("PROXY_USER", "").strip()
    pw = os.environ.get("PROXY_PASS", "").strip()
    host = os.environ.get("PROXY_HOST", "gw.dataimpulse.com:823").strip()
    if not (user and pw):
        raw = os.environ.get("PROXY_URL", "").strip()
        if raw:
            u = urlparse(raw)
            user = unquote(u.username or "")
            pw = unquote(u.password or "")
            if u.hostname:
                host = f"{u.hostname}:{u.port}" if u.port else u.hostname
    # callers append "__cr.us;sessid..." themselves; drop any suffix already on
    # the login (old PROXY_URL values carried "__cr.us")
    user = user.split("__")[0]
    return user, pw, host


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

    # City words may carry hyphens/apostrophes/periods ("Miami-Dade County",
    # "St. Louis", "Coeur d'Alene") — the old [A-Z][a-z]+ split "Miami-Dade"
    # down to "Dade County".
    _w = r"[A-Z][A-Za-z.'\-]*"
    _city = _w + r"(?:\s+(?:" + _w + r"|d'|de|del|la|of))*"

    # Try "City, ST" format (2-letter state code)
    m = re.search(r'\b(' + _city + r'),\s*([A-Z]{2})\b', location)
    if m:
        return m.group(1).strip(), m.group(2).strip()

    # Try "City, State Name" format
    m2 = re.search(r'(' + _city + r'),\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', location)
    if m2:
        return m2.group(1).strip(), m2.group(2).strip()

    # Just 2-letter state code
    state_match = re.search(r'\b([A-Z]{2})\b', location)
    if state_match and state_match.group(1) in US_STATE_CODES:
        return None, state_match.group(1)

    # Bare full state name ("Virginia", "New Jersey") — Murphy cards carry only this.
    for line in location.splitlines():
        name = line.strip().rstrip(',').strip()
        if name.lower() in US_STATE_NAMES:
            return None, US_STATE_NAMES[name.lower()]

    return None, None


US_STATE_NAMES = {n.lower(): n for n in (
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut",
    "Delaware", "District of Columbia", "Florida", "Georgia", "Hawaii", "Idaho", "Illinois",
    "Indiana", "Iowa", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
    "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Montana",
    "Nebraska", "Nevada", "New Hampshire", "New Jersey", "New Mexico", "New York",
    "North Carolina", "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
    "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas", "Utah",
    "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming",
    "Puerto Rico")}
US_STATE_CODES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL",
    "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE",
    "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD",
    "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "PR"}


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


def wp_date(v) -> Optional[str]:
    """'2024-05-17T09:12:44' -> '2024-05-17'; None for missing or implausible dates."""
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", str(v or ""))
    if not m:
        return None
    try:
        d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None
    if d.year < 2000 or d > date.today():
        return None
    return d.isoformat()


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
    cash_flow: float = None,
    status: str = "active",
    broker_published_on: str = None,
    broker_modified_on: str = None,
    broker_date_source: str = None,
) -> Dict:
    """Create a standardized listing dict.

    broker_published_on / broker_modified_on are dates as the broker's own site
    reports them (e.g. WordPress `date` / `modified`). They are broker-reported,
    not proof of the original listing date."""
    return {
        'listing_id': hashlib.md5(url.encode()).hexdigest(),
        'broker_account': broker_account,
        'title': title,
        'status': status,
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
        'cash_flow': cash_flow,
        'broker_published_on': broker_published_on,
        'broker_modified_on': broker_modified_on,
        'broker_date_source': broker_date_source if broker_published_on else None,
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
    AJAX_URL = f"{BASE}/wp-admin/admin-ajax.php"
    SDE_RE = re.compile(r"SDE:\s*\$([\d,]+)", re.I)
    TOKEN_RE = re.compile(r'api_token\s*=\s*"([A-Za-z0-9]+)"')

    @staticmethod
    def scrape(broker_account: str, max_pages: int = 50, headless: bool = True, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Murphy Business & Financial Corporation")
            print('='*60)

        # 2026-10-19: headless Chrome started timing out on the listings page
        # (both runs failed with TimeoutException after ~24s, on Actions and
        # the Mac). The page's own pagination is a plain admin-ajax POST, and
        # per_page=600 returns the whole set (479 cards) in one response, so
        # no browser is needed. Selenium stays as the fallback.
        try:
            listings = MurphyScraper._scrape_ajax(broker_account, verbose)
            if listings:
                return listings
            print("[Murphy] AJAX path returned 0 listings — falling back to Selenium")
        except Exception as e:
            print(f"[Murphy] AJAX path failed ({type(e).__name__}: {e}) — falling back to Selenium")
        return MurphyScraper._scrape_selenium(broker_account, max_pages, headless, verbose)

    @staticmethod
    def _parse_card(card) -> Optional[Dict]:
        """One div.card-body -> raw fields. Same markup the Selenium path read."""
        a = card.select_one("a.btn.btn-primary[href]")
        if not a:
            return None
        title_el = card.select_one("h5.card-title")
        price_el = card.select_one("p.price")
        txt = card.get_text("\n", strip=True)
        m = MurphyScraper.SDE_RE.search(txt)
        location = None
        pin = card.select_one("li img[src*='pin']")
        if pin is not None:
            location = pin.parent.get_text(" ", strip=True) or None
        return {
            'url': a["href"].strip(),
            'title': title_el.get_text(" ", strip=True) if title_el else None,
            'price_text': price_el.get_text(" ", strip=True) if price_el else None,
            'sde_text': m.group(1) if m else None,
            'location': location,
            'text': txt,
        }

    @staticmethod
    def _scrape_ajax(broker_account: str, verbose: bool = True) -> List[Dict]:
        session = requests.Session(impersonate="chrome120")
        page = session.get(MurphyScraper.LIST_URL, timeout=40)
        page.raise_for_status()
        # The token rotates on every page load; read it fresh each run.
        m = MurphyScraper.TOKEN_RE.search(page.text)
        if not m:
            raise RuntimeError("api_token not found on listings page")
        resp = session.post(
            MurphyScraper.AJAX_URL,
            data={
                'action': 'ajax_new_business_search_result',
                'page_number': 1,
                'per_page': 2000,
                'api_token': m.group(1),
            },
            headers={'Referer': MurphyScraper.LIST_URL,
                     'X-Requested-With': 'XMLHttpRequest'},
            timeout=90,
        )
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        listings, seen = [], set()
        for card in soup.select("div.card-body"):
            item = MurphyScraper._parse_card(card)
            if not item or item['url'] in seen:
                continue
            seen.add(item['url'])
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

        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            with_cf = sum(1 for l in listings if l.get('cash_flow'))
            print(f"\n✓ {len(listings)} Murphy listings via AJAX ({with_price} with price, {with_cf} with SDE)")
        return listings

    @staticmethod
    def _scrape_selenium(broker_account: str, max_pages: int = 50, headless: bool = True, verbose: bool = True) -> List[Dict]:
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
                    # Card text ends "...|\nVirginia\nLEARN MORE" — keep only the
                    # location line, not the button label.
                    location = None
                    if "|" in txt:
                        tail = [ln.strip() for ln in txt.split("|")[-1].splitlines()]
                        tail = [ln for ln in tail if ln and ln.upper() != "LEARN MORE"]
                        location = tail[0] if tail else None
                    
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
    
    def scrape(self, broker_account: str, max_pages: int = 40, headless: bool = True, verbose: bool = True) -> List[Dict]:
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
# FEED / TITLE STATUS (2026-11-12, decisions 5 + 6) — Transworld, We Sell Restaurants
# ============================================================================
# One status policy for every writer: scrapers/listing_status.py. These two
# helpers only find WHERE a scraper's status text is; the module decides what
# it means. Defensive import, with a minimal local fallback so the scrapers
# still run when the module is not installed (principle 15: same vocabulary).
#   sold                           -> 'sold' (explicit; the writer sends it)
#   under contract / pending / LOI -> 'active' + broker_status_text verbatim
#   coming soon / for lease        -> STATUS_DROP: the caller does not emit it
#   no signal / unknown text       -> 'active', no label (unchanged behaviour)
STATUS_DROP = "DROP"
try:
    from listing_status import (lookup_status as _st_lookup, DROP as _ST_DROP,
                                SOLD_PHRASES as _ST_SOLD,
                                UNDER_CONTRACT_PHRASES as _ST_UC,
                                COMING_SOON_PHRASES as _ST_CS,
                                OUT_OF_SCOPE_PHRASES as _ST_OOS,
                                INACTIVE_PHRASES as _ST_INACTIVE)
    if _ST_DROP != STATUS_DROP:                   # never mix two sentinels
        raise ImportError("listing_status.DROP differs")
    _ST_MARKER_PHRASES = frozenset(_ST_SOLD | _ST_UC | _ST_CS | _ST_OOS | _ST_INACTIVE)
except Exception:                    # module not present (j3 not applied)
    _st_lookup = None
    _ST_FALLBACK = {
        "sold": "sold", "just sold": "sold", "under contract": "active",
        "pending": "active", "sale pending": "active", "under loi": "active",
        "loi": "active", "under offer": "active", "accepted offer": "active",
        "coming soon": STATUS_DROP, "for lease": STATUS_DROP,
    }
    _ST_MARKER_PHRASES = frozenset(_ST_FALLBACK)


def lookup_status_text(text, domain=None) -> Optional[tuple]:
    """(status, label) for a known status phrase, else None. The shared
    module when importable; otherwise the small local table above."""
    if not text or not str(text).strip():
        return None
    if _st_lookup is not None:
        return _st_lookup(text, domain)
    norm = re.sub(r"\s+", " ", re.sub(r"[!:*()\[\]\"'.]|[-_/]", " ", str(text).lower())).strip()
    st = _ST_FALLBACK.get(norm)
    if st is None:
        return None
    return st, re.sub(r"\s+", " ", str(text)).strip()


# A status phrase as a regex: words joined by space / hyphen / underscore,
# longest first so "sale pending" wins over "pending".
_ST_MARKER = "|".join(
    r"[\s_-]*".join(re.escape(w) for w in p.split())
    for p in sorted(_ST_MARKER_PHRASES, key=len, reverse=True))
_SEP = r"(?:\s*[:|!]+\s*|\s+[-–—]+\s*|\s*[–—]+\s*)"
# Title MARKERS only, never free text ("Sold With Real Estate" mid-title,
# "Closed QSR for Sale" and "Pending Patent ..." are not statuses):
#   (UNDER CONTRACT) Foo      [Sold] Foo  *Under LOI* Foo -> bracketed, any case
#   UNDER CONTRACT: Foo       Pending - Foo       -> marker + separator, any case
#   UNDER CONTRACT-Foo                            -> bare hyphen: multi-word marker only
#   UNDER CONTRACT Foo                            -> no separator: marker all caps, title not
#   Foo - UNDER CONTRACT      Foo | Under LOI     -> separator + marker at the end
#   Foo (Sold)                                    -> bracketed at the end
_TITLE_PREFIX_BRACKET = re.compile(rf"^\s*(?:[\(\[]|\*+)\s*({_ST_MARKER})\s*!*\s*(?:[\)\]]|\*+)", re.I)
_TITLE_PREFIX_SEP = re.compile(rf"^\s*({_ST_MARKER}){_SEP}(?=\S)", re.I)
_TITLE_PREFIX_HYPHEN = re.compile(rf"^\s*((?:{_ST_MARKER}))-(?=\w)", re.I)
_TITLE_PREFIX_CAPS = re.compile(rf"^\s*({_ST_MARKER})\s+(?=\S)", re.I)
_TITLE_SUFFIX_BRACKET = re.compile(rf"(?:[\(\[]|\*+)\s*({_ST_MARKER})\s*!*\s*(?:[\)\]]|\*+)[\s.!]*$", re.I)
_TITLE_SUFFIX_SEP = re.compile(rf"(?:\s*[:|]+\s*|\s+[-–—]+\s*|\s*[–—]+\s*)({_ST_MARKER})[\s.!]*$", re.I)


def title_status_marker(title, domain=None) -> Optional[tuple]:
    """(status, label) from a status MARKER at the start or end of a title,
    else None. The label is the marker as the broker wrote it ("UNDER
    CONTRACT", "Under LOI"); the title itself is not changed."""
    t = (title or "").strip()
    if not t:
        return None
    cands = []
    for rx in (_TITLE_PREFIX_BRACKET, _TITLE_PREFIX_SEP, _TITLE_SUFFIX_BRACKET, _TITLE_SUFFIX_SEP):
        m = rx.search(t)
        if m:
            cands.append(m.group(1))
    m = _TITLE_PREFIX_HYPHEN.match(t)
    if m and re.search(r"[\s_-]", m.group(1).strip()):
        cands.append(m.group(1))
    m = _TITLE_PREFIX_CAPS.match(t)
    if m and m.group(1).isupper() and not t[m.end():].isupper():
        cands.append(m.group(1))
    for c in cands:
        hit = lookup_status_text(c.strip(), domain)
        if hit and hit[1] is not None:
            return hit
    return None


def _drop_log(tag: str, dropped: Dict[str, int]) -> None:
    if dropped:
        print(f"[{tag}] status DROP: {sum(dropped.values())} rows not emitted "
              f"({', '.join(f'{k}: {v}' for k, v in sorted(dropped.items()))})")


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
        dropped: Dict[str, int] = {}
        for item in all_items:
            location = item.get("location")
            city, state = extract_city_state(location or "")

            price = item.get("price")
            cash_flow = item.get("seller_discretionary_earnings")
            slug = item.get("slug", "")

            # Reading beats building: use a URL the API hands us if it looks like
            # a real detail page (/agents/<agent>/listings/<slug>).
            api_url = (item.get("url") or item.get("permalink")
                       or item.get("link") or "")
            if "/agents/" in api_url and "/listings/" in api_url:
                url = api_url
            elif slug:
                # Transworld requires an /agents/{slug}/ prefix — bare
                # /listings/{slug} and the old /{tribe}/listing/{slug} both 404.
                # Any agent slug works (they serve any listing under any agent
                # path; it's just franchise attribution). Hardcoded because the
                # API returns no agent field. NOTE: if this agent leaves
                # Transworld these URLs break — worth a periodic check.
                url = f"{self.BASE}/agents/aaronbrownlee/listings/{slug.lower()}"
            else:
                url = self.BASE

            # categories arrives as a JSON-encoded list, e.g. '["Medical Related Biz"]'
            cats = item.get("categories")
            if isinstance(cats, str):
                try:
                    cats = json.loads(cats)
                except ValueError:
                    cats = [cats]
            business_type = (cats[0] if isinstance(cats, list) and cats else None) \
                or item.get("industry")

            # Status (2026-11-12): /api/listings has no status field (nor does
            # the detail page's listing JSON); the only signal is a marker the
            # franchise office writes into the heading ("UNDER CONTRACT: ...",
            # "... - UNDER CONTRACT", "... | Under LOI"). No marker -> active.
            status, status_text = self.heading_status(item.get("heading"))
            if status == STATUS_DROP:
                dropped[status_text or "?"] = dropped.get(status_text or "?", 0) + 1
                continue

            rec = format_listing(
                url=url,
                broker_account=broker_account,
                title=item.get("heading"),
                business_type=business_type,
                price=float(price) if price else None,
                price_text=f"${price:,.0f}" if price else None,
                location=location,
                city=city,
                state=state,
                cash_flow=float(cash_flow) if cash_flow else None,
                status=status,
            )
            if status_text:
                rec["broker_status_text"] = status_text
                rec["status_source"] = "title_marker"
            listings.append(rec)

        if verbose:
            _drop_log("Transworld", dropped)
            with_price = sum(1 for l in listings if l.get('price'))
            by = {}
            for l in listings:
                k = l["status"] + (" (labelled)" if l.get("broker_status_text") else "")
                by[k] = by.get(k, 0) + 1
            print(f"\n✓ {len(listings)} Transworld listings ({with_price} with price) status={by}")

        return listings

    DOMAIN = "tworld.com"

    @classmethod
    def heading_status(cls, heading) -> tuple:
        """(status, broker_status_text) from a heading marker; ('active', None)
        when the heading carries none."""
        hit = title_status_marker(heading, cls.DOMAIN)
        return hit if hit else ("active", None)

    # ── Revenue from detail pages ────────────────────────────────────────
    # /api/listings has no revenue field. Each detail page shows it as
    #   <li>Sales: <span class="font-bold">$942,361</span></li>
    # so revenue is read from the page, only for listings that don't have it.
    _SALES_RE = re.compile(r"Sales:\s*<span[^>]*>\s*\$\s*([\d,]+)", re.I)

    def detail_sales(self, url: str) -> Optional[float]:
        try:
            r = self.session.get(url, timeout=30)
        except Exception:
            return None
        if r.status_code != 200:
            return None
        m = self._SALES_RE.search(r.text)
        if not m:
            return None
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            return None

    def add_revenue(self, listings: List[Dict], have_revenue: set, known: set,
                    max_pages: int = 400, workers: int = 4, verbose: bool = True) -> int:
        """Fill l['revenue'] from detail pages for listings without revenue.
        New listings (not yet in the DB) go first, then existing rows still
        missing revenue, capped at max_pages fetches per run."""
        todo = [l for l in listings if l.get("listing_url")
                and l["listing_url"] not in have_revenue and not l.get("revenue")]
        todo.sort(key=lambda l: l["listing_url"] in known)   # new first
        todo = todo[:max_pages]
        if not todo:
            return 0
        filled = 0

        def work(l):
            return l, self.detail_sales(l["listing_url"])

        with ThreadPoolExecutor(max_workers=workers) as ex:
            for l, sales in ex.map(work, todo):
                if not sales or sales < 10_000:
                    continue
                cf = l.get("cash_flow")
                if cf and sales < cf:      # broker entry error; don't publish
                    continue
                l["revenue"] = sales
                filled += 1
        if verbose:
            print(f"[Transworld] revenue: {len(todo)} detail pages checked, {filled} filled")
        return filled


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

    def scrape(self, broker_account: str, max_pages: int = 40, verbose: bool = True) -> List[Dict]:
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

                # Link keeps sold deals in its index with a stamp over the
                # photo: /Images/sold-stamp.png (400 of 450 cards on Oct 11).
                sold = card.find('img', src=re.compile(r'sold-stamp', re.I)) is not None

                listings.append({
                    'url': url,
                    'title': title,
                    'sold': sold,
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
                cash_flow=parse_money(item['cf_text']),
                status='sold' if item.get('sold') else 'active'
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
    DOMAIN = "wesellrestaurants.com"

    # ── Status (2026-11-12, decisions 5 + 6) ─────────────────────────────
    # Every feed record carries `bstatuslist` (11 Nov: Available 1,171 · In
    # Contract 124 · Available not Advertised 96 · LOI 26 of 1,417). The
    # feed's own word for under contract, "In Contract", is not in the shared
    # phrase set, so it is aliased here (label kept verbatim). "Available not
    # Advertised" is not a phrase either -> active, no label (it marks the
    # lease-space listings, not a status).
    STATUS_ALIASES = {"in contract": "under contract"}
    # For Lease is out of scope (decision 6 / OUT_OF_SCOPE_PHRASES): the
    # record's primary category is "Restaurant for Lease" (id 96), or its
    # headline says "for lease" and never "sale" ("Restaurant for Sale or
    # Lease!" and "... Sale with Long Lease" stay). Title and category only
    # (principle 5) — btype id 96 alone is ALSO set on real sales (a $399K
    # Naples bar), so it is not used. "Closed QSR for Sale" is not a status.
    LEASE_CATEGORY_ID = 96
    _FOR_LEASE = re.compile(r"\bfor\s+lease\b", re.I)
    _SALE = re.compile(r"\bsale\b", re.I)

    @classmethod
    def lease_label(cls, rec: Dict) -> Optional[str]:
        """The broker's own words when the record is a space for lease, else None."""
        cat = rec.get("listing_category") or {}
        if isinstance(cat, dict) and str(cat.get("id")) == str(cls.LEASE_CATEGORY_ID):
            return (cat.get("name") or "Restaurant for Lease").strip()
        head = rec.get("bheadlinead") or rec.get("bname") or ""
        m = cls._FOR_LEASE.search(head)
        if m and not cls._SALE.search(head):
            return m.group(0)
        return None

    @classmethod
    def record_status(cls, rec: Dict) -> tuple:
        """(status, broker_status_text) for one feed record; status may be
        STATUS_DROP (For Lease) — the caller does not emit it."""
        lease = cls.lease_label(rec)
        if lease:
            hit = lookup_status_text("for lease", cls.DOMAIN) or (STATUS_DROP, None)
            return hit[0], lease
        raw = re.sub(r"\s+", " ", str(rec.get("bstatuslist") or "")).strip()
        if not raw:
            return "active", None
        hit = lookup_status_text(raw, cls.DOMAIN)
        if hit is None:
            alias = cls.STATUS_ALIASES.get(raw.lower())
            if alias:
                hit = lookup_status_text(alias, cls.DOMAIN)
                hit = (hit[0], raw) if hit else None
        return hit if hit else ("active", None)

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
        total = None

        page_num = 1
        while page_num <= max_pages:
            data = self._fetch_page(page_num)
            if not data:
                break

            records = data.get("data", []) or []
            if last_page is None:
                last_page = data.get("last_page")
                total = data.get("total")
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
        dropped: Dict[str, int] = {}
        for rec in all_items:
            url = self._detail_url(rec)
            if not url:
                continue
            status, status_text = self.record_status(rec)
            if status == STATUS_DROP:
                dropped[status_text or "?"] = dropped.get(status_text or "?", 0) + 1
                continue
            bat = rec.get("listing_bat") or {}
            city = (rec.get("bcity") or "").strip() or None
            state = (rec.get("bstate") or "").strip() or None
            location = ", ".join([p for p in (city, state) if p]) or None
            title = (rec.get("bheadlinead") or rec.get("bname")
                     or rec.get("burldes") or "").strip() or None

            out = format_listing(
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
                status=status,
            )
            if status_text:
                out["broker_status_text"] = status_text
                out["status_source"] = "feed_bstatuslist"
            listings.append(out)

        if verbose:
            _drop_log("WeSell", dropped)
            with_price = sum(1 for l in listings if l.get("price"))
            by = {}
            for l in listings:
                k = l["status"] + (" (labelled)" if l.get("broker_status_text") else "")
                by[k] = by.get(k, 0) + 1
            print(f"\n✓ {len(listings)} We Sell Restaurants listings ({with_price} with price) "
                  f"status={by} feed_total={total}")

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


# ============================================================================
# VESTED BUSINESS BROKERS SCRAPER
# PHP AJAX pagination (listing_search_ajax.php), form-encoded, ~121 pages.
# Same shape as Sunbelt: POST the search endpoint, parse the HTML fragment.
# ============================================================================

class VestedScraper:
    """
    Vested Business Brokers  (account 1593)
    https://www.vestedbb.com

    The advancesearch / businesses-for-sale page loads results from a PHP
    AJAX endpoint that returns an HTML fragment of listing cards. Paginates
    via the `page` param (~121 pages). The full param set (esp. sort_by +
    listing_key) and X-Requested-With header are required — a partial payload
    silently caps at ~20 pages.
    """

    BASE = "https://www.vestedbb.com"
    INIT_URL = f"{BASE}/advancesearch/index.html"
    AJAX_URL = f"{BASE}/script/listing_search_ajax.php"
    CARD_SEL = "div.col-md-3.col-sm-6.col-xs-6.ff100"
    # Overlay-badge alt/title text that must never be used as a listing title.
    _BADGE_WORDS = {
        "owner financing", "new listing", "sold", "pending", "price change",
        "recently updated", "featured", "under contract", "coming soon",
        "reduced", "hot listing", "price reduced", "just listed",
    }

    def __init__(self):
        self.proxies = self._build_proxies()
        self.session, self.fallback = self._make_session()

    @staticmethod
    def _build_proxies():
        """DataImpulse US sticky-session proxy from env, or None. vestedbb
        rate-limits a single IP across a full 121-page crawl, so route through
        a residential proxy when creds are available."""
        user, pw, host = _proxy_creds()
        if not (user and pw):
            return None
        sid = f"vt{random.randint(100000, 999999)}"
        url = f"http://{user}__cr.us;sessid.{sid}:{pw}@{host}"
        return {"http": url, "https": url}

    def _make_session(self):
        """Prefer curl_cffi (codebase convention); fall back to stdlib requests
        if curl_cffi's TLS stack is unavailable in this environment. vestedbb
        serves plain requests fine, so the fallback is fully functional."""
        try:
            s = requests.Session(impersonate="chrome120")
            s.get(self.INIT_URL, timeout=15, proxies=self.proxies)
            return s, False
        except Exception:
            import requests as _std
            s = _std.Session()
            s.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36"})
            try:
                s.get(self.INIT_URL, timeout=15, proxies=self.proxies)
            except Exception:
                pass
            return s, True

    def _fetch_page(self, page_num: int) -> str:
        payload = {
            "page": str(page_num),
            "default_search": "1",
            "business_type": "",
            "stateid": "",
            "countyid": "",
            "asking_price_min": "",
            "asking_price_max": "",
            "cash_flow_min": "",
            "cash_flow_max": "",
            "listing_key": "",
            "owner_financed": "",
            "down_pay": "",
            "sort_by": "ListingDate:HL",
        }
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.INIT_URL,
            "Origin": self.BASE,
        }
        r = self.session.post(self.AJAX_URL, data=payload, headers=headers,
                              timeout=30, proxies=self.proxies)
        r.raise_for_status()
        return r.text

    def _parse_html(self, html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        out = []
        for card in soup.select(self.CARD_SEL):
            # Detail URL — from the anchor, else the redirectURL() onclick.
            url = None
            a = card.find("a", href=True)
            if a and a["href"].strip():
                url = a["href"].strip()
            if not url:
                m = re.search(r"redirectURL\(['\"]([^'\"]+)['\"]\)", str(card))
                url = m.group(1) if m else None
            if not url:
                continue
            if url.startswith("/"):
                url = self.BASE + url

            # Location from the card's location line ("New Haven County, CT").
            text = card.get_text(" ", strip=True)
            loc_el = card.select_one("div.location")
            location = loc_el.get_text(" ", strip=True) if loc_el else None
            city, state = None, None
            if location and "," in location:
                parts = [p.strip() for p in location.split(",")]
                city = parts[0]
                if len(parts[-1]) == 2:
                    state = parts[-1].upper()

            # Vested cards are anonymized: the colored header (div.name) shows a
            # business TYPE — "Gas Station/CStore", "Indian Restaurant" — not a
            # business name. Use type + location as the title (skip badge
            # overlays like "Sold"), and keep the raw type in business_type.
            nm = card.select_one("div.name")
            business_type = nm.get_text(" ", strip=True) if nm else None
            if business_type and business_type.lower() in self._BADGE_WORDS:
                business_type = None
            if business_type and location:
                title = f"{business_type} in {location}"
            else:
                title = business_type or location

            price_text = self._field(text, "Asking Price")
            cf_text = self._field(text, "Cash Flow")
            out.append({
                "url": url, "title": title, "business_type": business_type,
                "location": location, "city": city, "state": state,
                "price_text": price_text, "cf_text": cf_text,
            })
        return out

    @staticmethod
    def _field(text: str, label: str) -> Optional[str]:
        m = re.search(re.escape(label) + r'\s*:?\s*(\$[\d,]+(?:\.\d+)?|Undisclosed|N/?A)',
                      text, re.I)
        if not m:
            return None
        val = m.group(1)
        # $0 / undisclosed -> no real price
        if re.match(r'^\$0+(\.0+)?$', val) or val.lower() in ("undisclosed", "na", "n/a"):
            return None
        return val

    def scrape(self, broker_account: str, max_pages: int = 130, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Vested Business Brokers")
            print('='*60)
            if self.fallback:
                print("[Vested] curl_cffi unavailable — using stdlib requests")

        all_items, seen = [], set()
        empty_streak = 0
        fail_streak = 0
        for page_num in range(1, max_pages + 1):
            # The site 500s after ~20 sequential requests from one IP. Rotate
            # to a fresh residential-proxy IP proactively every 15 pages.
            if self.proxies and page_num > 1 and page_num % 15 == 1:
                self.proxies = self._build_proxies()

            items = None
            for attempt in range(4):
                try:
                    items = self._parse_html(self._fetch_page(page_num))
                    break
                except Exception as e:
                    # Rate-limit / 500 -> rotate IP and retry the same page.
                    if self.proxies:
                        self.proxies = self._build_proxies()
                    if attempt == 3:
                        if verbose:
                            print(f"[Vested] Page {page_num} failed after retries: {e}")
                    else:
                        time.sleep(random.uniform(1.0, 2.0) * (attempt + 1))

            if items is None:
                fail_streak += 1
                if fail_streak >= 6:      # give up if the site is hard-down
                    if verbose:
                        print("[Vested] 6 consecutive page failures — stopping")
                    break
                continue
            fail_streak = 0

            new = [l for l in items if l["url"] not in seen]
            for l in new:
                seen.add(l["url"])
            all_items.extend(new)
            if verbose and (page_num == 1 or page_num % 20 == 0):
                print(f"[Vested] Page {page_num}: {len(new)} new | Total: {len(all_items)}")
            # Stop when the feed stops yielding anything new (handles the
            # site returning the last page repeatedly past the real end).
            if not new:
                empty_streak += 1
                if empty_streak >= 3:
                    break
            else:
                empty_streak = 0
            time.sleep(random.uniform(0.5, 1.1))

        listings = [format_listing(
            url=it["url"], broker_account=broker_account, title=it["title"],
            price=parse_money(it["price_text"]), price_text=it["price_text"],
            location=it["location"], city=it["city"], state=it["state"],
            business_type=it.get("business_type"),
            cash_flow=parse_money(it["cf_text"]),
        ) for it in all_items]

        if verbose:
            with_price = sum(1 for l in listings if l.get("price"))
            print(f"\n✓ {len(listings)} Vested listings ({with_price} with price)")
        return listings


# ============================================================================
# ROUTES FOR SALE SCRAPER
# Entire inventory on ONE static HTML page as ~14 <table> sections.
# ============================================================================

class RoutesForSaleScraper:
    """
    Routes For Sale (routesforsale.net) — account 13461.

    ~293 routes live on ONE static page as ~14 <table> sections, each preceded
    by a category heading ("Flowers Bread Routes", "Mission's Tortilla Routes",
    …). Row columns: Route Type | City, State | Price | Financing | Cash Flow |
    Status | Route Details(link). One GET + BeautifulSoup — no pagination, API,
    or proxy. Skips non-Active rows and rows with a blank cash flow.
    """

    BASE = "https://www.routesforsale.net"
    URL = f"{BASE}/route-listings.html"

    def __init__(self):
        self.session = self._make_session()

    @classmethod
    def _make_session(cls):
        try:
            s = requests.Session(impersonate="chrome120")
            s.get(cls.BASE, timeout=15)
            return s
        except Exception:
            import requests as _std
            s = _std.Session()
            s.headers.update({
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36"})
            return s

    @staticmethod
    def _category_for(table):
        """Nearest preceding heading, stripped of a trailing 'Routes'."""
        el = table.find_previous(["h1", "h2", "h3", "h4", "h5", "strong", "b", "caption", "p"])
        for _ in range(6):
            if el is None:
                break
            t = el.get_text(" ", strip=True)
            if t and "route" in t.lower() and len(t) <= 60:
                return re.sub(r'\s*routes?\s*$', '', t, flags=re.I).strip() or t
            el = el.find_previous(["h1", "h2", "h3", "h4", "h5", "strong", "b", "caption", "p"])
        return None

    @staticmethod
    def _is_header(low):
        return "status" in low and any("price" in c for c in low) \
            and any("route" in c for c in low)

    def scrape(self, broker_account: str, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Routes For Sale (routesforsale.net)")
            print('='*60)

        html = self.session.get(self.URL, timeout=30).text
        soup = BeautifulSoup(html, "html.parser")
        items, seen = [], set()

        for table in soup.find_all("table"):
            category = self._category_for(table)
            # Build a column-index map from this table's header row, if present.
            colmap = None
            for tr in table.find_all("tr"):
                low = [c.get_text(" ", strip=True).lower() for c in tr.find_all(["td", "th"])]
                if self._is_header(low):
                    colmap = {name: i for i, name in enumerate(low)}
                    break

            def idx(name, default):
                if colmap:
                    for key, i in colmap.items():
                        if name in key:
                            return i
                return default

            for tr in table.find_all("tr"):
                cells = tr.find_all(["td", "th"])
                if len(cells) < 5:
                    continue
                texts = [c.get_text(" ", strip=True) for c in cells]
                low = [t.lower() for t in texts]
                if self._is_header(low):
                    continue

                def col(name, default):
                    i = idx(name, default)
                    return texts[i] if 0 <= i < len(texts) else ""

                route_type = col("route type", 0) or col("type", 0)
                loc = col("city", 1)
                price_txt = col("price", 2)
                cf_txt = col("cash flow", 4)
                status = col("status", 5)

                link = tr.find("a", href=True)
                url = link["href"].strip() if link else None
                if url and url.startswith("/"):
                    url = self.BASE + url

                # Filters: Active only; must have a cash-flow figure and a link.
                if status and status.strip().lower() != "active":
                    continue
                if not cf_txt or not re.search(r'\d', cf_txt):
                    continue
                if not route_type or not url or url in seen:
                    continue
                seen.add(url)

                city, state = extract_city_state(loc)
                items.append(format_listing(
                    url=url, broker_account=broker_account,
                    title=f"{route_type} Route - {loc}".strip(),
                    price=parse_money(price_txt), price_text=price_txt,
                    location=loc, city=city, state=state,
                    business_type=category, cash_flow=parse_money(cf_txt)))

        if verbose:
            with_price = sum(1 for l in items if l.get("price"))
            print(f"\n✓ {len(items)} Routes For Sale listings ({with_price} with price)")
        return items


# ============================================================================
# GENERIC FACETWP AJAX SCRAPER
# The only way to filter by STATUS on FacetWP sites.
# ============================================================================

def facet_card_status(title, card):
    """(status, broker_status_text) for a FacetWP listing card (eatz, synergy).

    Shared phrase set (scrapers/listing_status.py, decisions 5 + 6) when
    importable: a short badge-like text node on the card ("Under Contract",
    "SOLD", "Coming Soon!") decides, most final wins; under contract /
    pending stay 'active' with the badge verbatim; Coming Soon / For Lease
    return 'DROP' (the caller does not emit the card). Then the title
    marker (is_sold_or_pending: SOLD -> sold, Under Contract / Pending ->
    active + label). Before 2026-11-12 an under-contract title was written
    'sold' here, while the facet itself lists under-contract as for sale.
    A bare all-caps SOLD anywhere on the card still means sold (unchanged).
    """
    try:
        from listing_status import lookup_status, STATUS_RANK
    except Exception:                 # module not present: previous behaviour
        lookup_status = None
    text = card.get_text(" ", strip=True) if card is not None else ""
    best = None
    if lookup_status is not None and card is not None:
        for s in card.stripped_strings:
            hit = lookup_status(s)
            if hit and hit[1] is not None and (
                    best is None or STATUS_RANK[hit[0]] > STATUS_RANK[best[0]]):
                best = hit
    if best is None and is_sold_or_pending(title):
        m = re.search(r"under[\s-]*contract|sale[\s-]*pending|pending", title, re.I)
        if m and not re.search(r"\bsold\b", title, re.I):
            best = ("active", m.group(0)) if lookup_status is not None else ("sold", None)
        else:
            best = ("sold", None)
    if (best is None or best[0] == "active") and re.search(r"\bSOLD\b", text):
        best = ("sold", "SOLD")
    return best if best else ("active", None)


class FacetWPScraper:
    """
    Generic FacetWP AJAX scraper — one class, many brokers.

        FacetWPScraper("eatz-associates.com", "listings",
                       {"status": ["for-sale", "new-listing"]}).scrape(acct)

    Why this exists: on FacetWP sites the WP REST endpoint returns the WHOLE
    archive with no status field — eatz's wp-json/wp/v2/listing is 1,026 records
    but only ~5 pages are for-sale vs ~64 pages sold (93% graveyard). The
    ?fwp_status= URL param is ignored server-side (every variant returns
    identical HTML). The facetwp_refresh POST is the only thing that actually
    filters, so it's the only safe way to import these brokers.

    Same payload shape for every FacetWP broker — only `uri` and the facet
    names change (Synergy is the same shape, also ~47% sold).
    """

    # Facet keys are sent as empty lists unless overridden; FacetWP expects the
    # site's full facet set in the payload.
    DEFAULT_FACET_KEYS = ("pagination", "state", "citydropdown", "status",
                          "business_type", "featured", "food_type",
                          "listing_categories", "service_type")

    # The WAF 429s a cookie-less POST. Any plausible returning-browser cookie
    # set satisfies it (these are just analytics/popup cookies — no auth).
    DEFAULT_COOKIES = {
        "_gauges_unique": "1", "_gauges_unique_day": "1",
        "_gauges_unique_hour": "1", "_gauges_unique_month": "1",
        "_gauges_unique_year": "1",
    }

    def __init__(self, domain, uri, facets=None, facet_keys=None,
                 link_match="/listing", cookies=None, use_proxy_fallback=False):
        self.domain = domain.strip().strip("/")
        self.uri = uri.strip("/")
        self.facets = facets or {"status": ["for-sale"]}
        self.facet_keys = tuple(facet_keys) if facet_keys else self.DEFAULT_FACET_KEYS
        self.link_match = link_match
        self.cookies = cookies or dict(self.DEFAULT_COOKIES)
        self.use_proxy_fallback = use_proxy_fallback
        # Cards not emitted by status (Coming Soon / For Lease), by label.
        self.dropped_status: List[str] = []
        # seconds between page POSTs; eatz 429s back-to-back requests
        self.page_delay = 20
        self.base = f"https://{self.domain}"
        self.endpoint = f"{self.base}/{self.uri}/"
        self.session = self._make_session()
        # DIRECT FIRST. Counter-intuitively the residential proxy makes eatz
        # WORSE — it 429s DataImpulse exit IPs while serving our direct IP 200.
        # The proxy is an escalation path only, for brokers that block us.
        self.proxies = None

    @staticmethod
    def _make_session():
        import requests as _std
        s = _std.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"})
        return s

    @staticmethod
    def _build_proxies():
        user, pw, host = _proxy_creds()
        if not (user and pw):
            return None
        sid = f"fw{random.randint(100000, 999999)}"
        return {"http": f"http://{user}__cr.us;sessid.{sid}:{pw}@{host}",
                "https": f"http://{user}__cr.us;sessid.{sid}:{pw}@{host}"}

    def _query(self, paged):
        """fwp_ query string. The comma between facet values stays URL-ENCODED
        (%2C) exactly as the browser sends it."""
        parts = [f"fwp_{k}=" + "%2C".join(v) for k, v in self.facets.items() if v]
        parts.append(f"fwp_paged={paged}")
        return "&".join(parts)

    def _page_url(self, paged):
        # The POST goes to the URL *with* the fwp_ params — not the bare path.
        return f"{self.endpoint}?{self._query(paged)}"

    def _payload(self, paged):
        facets = {k: [] for k in self.facet_keys}
        facets.update(self.facets)
        get = {}
        url_vars = {}
        for k, v in self.facets.items():
            if v:
                get[f"fwp_{k}"] = "%2C".join(v)      # encoded, as the browser sends
                url_vars[k] = list(v)
        get["fwp_paged"] = str(paged)
        return {
            "action": "facetwp_refresh",
            "data": {
                "facets": facets,
                "frozen_facets": {},
                "http_params": {"get": get, "uri": self.uri, "url_vars": url_vars},
                "template": "wp",
                "extras": {"sort": "default"},
                "soft_refresh": 1,
                "is_bfcache": 1,
                "first_load": 0,
                "paged": str(paged),                  # string, not int
            },
        }

    def _headers(self, paged):
        # Full browser header set — a bare Content-Type/XHR pair gets 429'd.
        return {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "origin": self.base,
            "priority": "u=1, i",
            "referer": self._page_url(paged),
            "sec-ch-ua": '"Google Chrome";v="149", "Chromium";v="149", '
                         '"Not)A;Brand";v="24"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/149.0.0.0 Safari/537.36",
        }

    def _post(self, paged):
        # Send the JSON as a raw body and pass cookies explicitly. A cookie-less
        # POST is 429'd, and (counter-intuitively) a warm-up GET before the POST
        # ALSO triggers the 429 — so we go straight to the POST.
        body = json.dumps(self._payload(paged), separators=(",", ":")).encode()
        return self.session.post(self._page_url(paged), data=body,
                                 headers=self._headers(paged),
                                 cookies=self.cookies, timeout=35,
                                 proxies=self.proxies)

    def _parse_template(self, html, broker_account):
        soup = BeautifulSoup(html, "html.parser")
        out, seen = [], set()
        for a in soup.select(f'a[href*="{self.link_match}"]'):
            href = (a.get("href") or "").strip()
            if not href or href in seen:
                continue
            seen.add(href)
            # Nav/archive links ("Listings", "Current Listings", "Recently Sold
            # Listings", ?fwp_ filters) aren't listings: require a slug segment
            # after the match path and no query string.
            _path = href.split("#")[0]
            if "?" in _path or not re.search(re.escape(self.link_match.rstrip("/")) + r"/[^/?#]+/?$", _path):
                continue
            # Climb to the listing card: stop once the card carries price-like
            # text, or before the parent would swallow a second listing.
            card = a
            for _ in range(6):
                if card.parent is None:
                    break
                t = card.get_text(" ", strip=True)
                if re.search(r'asking\s*price', t, re.I):
                    break
                par = card.parent
                links = {x.get("href") for x in par.select(f'a[href*="{self.link_match}"]')}
                if len(links) > 1:
                    break
                card = par
            text = card.get_text(" ", strip=True)

            title = a.get_text(" ", strip=True)
            if is_junk_title(title):
                h = card.find(["h1", "h2", "h3", "h4"])
                ht = h.get_text(" ", strip=True) if h else ""
                title = ht if not is_junk_title(ht) else (title_from_slug(href) or "")
            if not title:
                continue

            # Labelled fields first (eatz/synergy cards: "Asking Price: $250,000",
            # "Gross Sales: $1,084,587", "Net Op Income: $57,014"). A blank or
            # non-numeric value ("$(Asset Sale)") stays None. Fall back to the
            # first $ on the card only when there is no Asking Price label.
            def _lab(rx):
                m = re.search(rx + r'\s*:?\s*\$\s?([\d,]{3,}(?:\.\d+)?)', text, re.I)
                return parse_money("$" + m.group(1)) if m else None
            price = _lab(r'asking\s*price')
            if price is None and not re.search(r'asking\s*price', text, re.I):
                pm = re.search(r'\$\s?[\d,]{4,}', text)
                price = parse_money(pm.group(0)) if pm else None
            revenue = _lab(r'(?:gross\s*(?:sales|revenue)|revenue)')
            cash_flow = _lab(r'(?:net\s*op(?:erating)?\.?\s*income|cash\s*flow|sde|seller.s\s*discretionary\s*earnings)')
            # State only: the card's location line ("Far Northwest Suburb of
            # Chicago, IL") runs into the title text, so the city isn't reliable.
            city, state = (None, None)
            for lm in re.finditer(r',\s*([A-Z]{2})\b', text):
                if lm.group(1) in US_STATE_CODES:
                    state = lm.group(1)
                    break
            status, status_text = facet_card_status(title, card)
            if status == "DROP":                  # Coming Soon / For Lease
                self.__dict__.setdefault("dropped_status", []).append(status_text)
                continue
            rec = format_listing(
                url=href, broker_account=broker_account, title=title,
                price=price, price_text=f"${price:,.0f}" if price else None,
                revenue=revenue, cash_flow=cash_flow, city=city, state=state,
                status=status)
            if status_text:
                rec["broker_status_text"] = status_text
            out.append(rec)
        return out

    def scrape(self, broker_account: str, max_pages: int = 40, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print(f"FacetWP: {self.domain}/{self.uri}  facets={self.facets}")
            print('='*60)
        # NOTE: deliberately NO warm-up GET — it trips the WAF into 429ing the
        # subsequent POST. Straight to the POST with cookies.
        listings, seen = [], set()
        empty_streak = 0
        for paged in range(1, max_pages + 1):
            # Rotate the exit IP proactively once we're ON the proxy (these
            # sites rate-limit durably per-IP).
            if self.proxies and paged > 1 and paged % 15 == 1:
                self.proxies = self._build_proxies()

            data = None
            for attempt in range(5):
                try:
                    r = self._post(paged)
                    if r.status_code in (429, 500, 502, 503) or \
                            "json" not in r.headers.get("content-type", "").lower():
                        raise RuntimeError(f"HTTP {r.status_code}")
                    data = r.json()
                    break
                except Exception as e:
                    # 429 here is a plain rate-limit — BACK OFF, don't reach for
                    # the proxy: eatz 429s DataImpulse exit IPs outright, so
                    # proxying makes it strictly worse. Proxy is opt-in only.
                    if self.use_proxy_fallback and attempt >= 1:
                        self.proxies = self._build_proxies()
                    if attempt == 4:
                        if verbose:
                            print(f"[FacetWP:{self.domain}] page {paged} failed: {e}")
                    else:
                        # 5s, 15s, 30s, 60s — the limiter needs real time.
                        time.sleep((5, 15, 30, 60)[min(attempt, 3)]
                                   + random.uniform(0, 3))
            if data is None:
                break

            new = [l for l in self._parse_template(data.get("template") or "",
                                                   broker_account)
                   if l["listing_url"] not in seen]
            for l in new:
                seen.add(l["listing_url"])
            listings.extend(new)

            pager = (data.get("settings") or {}).get("pager") or {}
            total_pages = pager.get("total_pages")
            if verbose and (paged == 1 or paged % 10 == 0):
                print(f"[FacetWP:{self.domain}] page {paged}: {len(new)} new | "
                      f"total={len(listings)} | pager_total_pages={total_pages}")
            if not new:
                empty_streak += 1
                if empty_streak >= 2:
                    break
            else:
                empty_streak = 0
            if total_pages and paged >= int(total_pages):
                break
            time.sleep(random.uniform(self.page_delay, self.page_delay + 5))

        if verbose:
            wp = sum(1 for l in listings if l.get("price"))
            print(f"\n✓ {len(listings)} {self.domain} listings ({wp} with price)")
        if self.dropped_status:
            by = {}
            for k in self.dropped_status:
                by[k or "?"] = by.get(k or "?", 0) + 1
            print(f"[FacetWP:{self.domain}] status DROP: {len(self.dropped_status)} cards not emitted "
                  f"({', '.join(f'{k}: {v}' for k, v in sorted(by.items()))})")
        return listings


# ── WordPress-REST listing status (2026-11-11) ──────────────────────────────
# A WP post type holds every listing the broker ever published — sold, pulled
# and parked ones included (vikingmergers: 411 posts, 62 on its live index).
# Nothing in REST says "this is no longer for sale" unless the theme carries a
# status field, so WPRestScraper reads status from, in order of strength:
#   1. a status field — class_list taxonomy term (status-sold,
#      listing_status-past, property_status-for-sale ...), an acf/meta key
#      (availability / listing_status / status), a 'sold-' slug, a SOLD title;
#   2. the live index page (index_url): a card badge ("Coming Soon!",
#      "SOLD FOR", "Under Contract") and whether the post is on the page at all.
# The most final signal wins: sold > inactive > active.
#
# Mapping — Jeff's decisions 5 + 6 (2026-11-11 23:50 UTC). The shared
# constant set is scrapers/listing_status.py (job 3); when it is importable
# every lookup below goes through it, so WP-REST, businessusa, ninbb and the
# displayed-count rule read one vocabulary (principle 15). The local sets are
# the fallback and hold the same vocabulary:
#   sold     — the deal closed: sold, sold-out, closed, past, recently-sold.
#   active + label — still on the broker's list, under contract / pending /
#              sale pending / under LOI / under offer / accepted offer / off
#              market: status stays 'active' and the broker's words go in
#              broker_status_text verbatim.
#   DROP     — coming soon: not a listing yet, NOT emitted (counted and
#              logged by scrape()); earlier active rows retire through the
#              absence rule.
#   inactive — exists but is not for sale: hold/paused, withdrawn, expired,
#              not available — and any post that is NOT on the broker's live
#              index (a different signal from a label: see _apply_index).
#   active   — positive evidence only: for-sale, active, current, available,
#              new-listing, reduced, "No Banner".
# 'sold' and 'inactive' are both explicit statuses: upsert_listings() writes
# them through its _status_explicit path (principle 14). Both values already
# exist in listings_direct (sql/2026-11-09_held_audit.sql).
WP_SOLD_VALUES = frozenset({
    "sold", "soldout", "sold-out", "closed", "past", "recently-sold",
    "sold-closed", "sold-settled", "just-closed"})
WP_UNDER_CONTRACT_VALUES = frozenset({
    "under-contract", "undercontract", "contract-pending", "pending",
    "sale-pending", "pending-sale", "under-loi", "loi", "loi-accepted",
    "under-offer", "accepted-offer", "has-accepted-offer", "offer-accepted",
    "under-agreement", "off-market", "offmarket"})
# DROP also covers a for-lease status value (out of scope, as _parse's
# availability rule already treats it).
WP_DROP_VALUES = frozenset({"coming-soon", "comingsoon", "coming-soon-listing",
                            "for-lease", "for-rent", "lease", "leased", "for-lease-only"})
WP_INACTIVE_VALUES = frozenset({
    "hold", "on-hold", "paused", "withdrawn", "expired", "inactive",
    "not-available", "no-longer-available", "temporarily-unavailable"})
WP_ACTIVE_VALUES = frozenset({
    "for-sale", "active", "current", "available", "new-listing", "new",
    "reduced", "price-reduced", "listing", "business-for-sale",
    "businesses-for-sale", "no-banner"})
# Taxonomy prefixes that carry a listing status. The bare WP post-type class
# (type-listing) and post status (status-publish) are deliberately not
# matched as values: 'publish' is not in the vocabulary, and 'type-' alone is
# not a status prefix.
_WP_STATUS_TAX = re.compile(
    r"^(?:(?:listing[_-]?)?(?:status|availability|category)|property[_-]status|"
    r"listing[_-]type|portfolio[_-]type)[-_](.+)$", re.I)
_WP_STATUS_KEY = re.compile(r"(?:^|_)(?:availability|status|sold)$", re.I)
# Not-emitted sentinel; the same value as listing_status.DROP.
WP_DROP = "DROP"
_WP_STATUS_RANK = {"active": 0, "inactive": 1, WP_DROP: 2, "sold": 3}
try:
    from listing_status import (lookup_status as _ls_lookup,
                                classify_class_term as _ls_term,
                                DROP as _LS_DROP)
    if _LS_DROP != WP_DROP:                       # never mix two sentinels
        raise ImportError("listing_status.DROP differs")
except Exception:                    # module not present (job 3 not applied)
    _ls_lookup = _ls_term = None
# Card badges on the live index. Matched against single short text nodes, not
# free text, so "patent pending" in a description is not a badge.
_WP_BADGE_SOLD = re.compile(
    r"^\(?\s*(?:sold(?:\s+for)?|recently\s+sold|just\s+closed|closed)\s*!?\s*\)?:?$", re.I)
_WP_BADGE_UNDER_CONTRACT = re.compile(
    r"^\(?\s*(?:under\s+contract|contract\s+pending|sale\s+pending|pending(?:\s+sale)?|"
    r"under\s+(?:loi|offer|agreement)|(?:has\s+)?accepted\s+offer|offer\s+accepted|"
    r"off[\s-]market)\s*!?\s*\)?$", re.I)
_WP_BADGE_DROP = re.compile(r"^\(?\s*(?:coming\s+soon|for\s+lease)\s*!?\s*\)?$", re.I)
_WP_BADGE_INACTIVE = re.compile(
    r"^\(?\s*(?:on\s+hold|withdrawn|expired|(?:no\s+longer|not)\s+available)\s*!?\s*\)?$",
    re.I)
# Price shown but not as a number. "$40k Down" is a down payment, not a price.
# Fallback only. The shared rule is scrapers/price_disclosure.py (W14): both
# writers use it, so when it is importable WPRestScraper uses it too
# (principle 15). This fallback keeps the patch runnable without W14 and, like
# the shared list, does not treat a bare "call" or "N/A" as a broker statement.
try:
    from price_disclosure import (withheld_price_phrase as _pd_phrase,
                                  find_labelled_withheld_price as _pd_labelled)
except Exception:                    # module not present (W14 not applied)
    _pd_phrase = _pd_labelled = None
_WP_PRICE_HIDDEN = re.compile(
    r"\b(?:market\s+price|call\s+for\s+(?:price|pricing)|contact\s+(?:us|broker)|"
    r"tbd|confidential|negotiable|inquire|upon\s+request|undisclosed|"
    r"not\s+disclosed)\b", re.I)
_WP_DOWN_PAYMENT = re.compile(r"\$\s?[\d,.]+\s*[kKmM]?\s+down\b", re.I)
_WP_LABELLED_MONEY = r"\s*[:\-]?\s*(\$\s?\d[\d,]*(?:\.\d+)?\s*(?:[kK]|[mM]{1,2}|million)?)"


def wp_status(raw) -> Optional[tuple]:
    """(status, keep_label) for a status value; None outside the vocabulary
    (never a guess). 'Sold' -> ('sold', True); 'Under Contract' ->
    ('active', True); 'Coming Soon' -> (WP_DROP, True); 'For Sale' ->
    ('active', False)."""
    if isinstance(raw, bool) or raw is None:
        return None
    if _ls_term is not None:
        hit = _ls_term(str(raw).strip())
        return (hit[0], hit[1] is not None) if hit else None
    v = re.sub(r"[\s_]+", "-", str(raw).strip().lower()).strip("-")
    if v in WP_SOLD_VALUES:
        return "sold", True
    if v in WP_UNDER_CONTRACT_VALUES:
        return "active", True
    if v in WP_DROP_VALUES:
        return WP_DROP, True
    if v in WP_INACTIVE_VALUES:
        return "inactive", True
    if v in WP_ACTIVE_VALUES:
        return "active", False
    return None


def wp_status_value(raw) -> Optional[str]:
    """'Sold' / 'sold-out' / 'Under Contract' / 'Coming Soon' / 'For Sale' ->
    sold | active | DROP | active; None outside the vocabulary."""
    hit = wp_status(raw)
    return hit[0] if hit else None


def wp_slug_sold(slug: str) -> bool:
    """'sold-hvac-company-1234' / 'hvac-company-sold' -> True."""
    s = (slug or "").lower()
    return bool(re.match(r"sold[-_]", s) or re.search(r"[-_]sold(?:[-_]\d+)?$", s))


def wp_badge_status(text: str) -> Optional[str]:
    """A card badge -> 'sold' | 'active' (under contract family, label kept)
    | WP_DROP (Coming Soon / For Lease) | 'inactive'; None for anything else,
    including a plain 'For Sale' (no label to carry)."""
    t = (text or "").strip()
    if len(t) > 40:
        return None
    if _ls_lookup is not None:
        hit = _ls_lookup(t)
        return hit[0] if hit and hit[1] is not None else None
    if _WP_BADGE_SOLD.match(t):
        return "sold"
    if _WP_BADGE_UNDER_CONTRACT.match(t):
        return "active"
    if _WP_BADGE_DROP.match(t):
        return WP_DROP
    if _WP_BADGE_INACTIVE.match(t):
        return "inactive"
    return None


def wp_money(v) -> Optional[float]:
    """First money amount in a value: 1250000, '$1,250,000', '$4.99M',
    '$1,750,000 (Business) + $3,450,000 (Real Estate)' -> 1750000.0.
    0, '', 'TBD', 'Market Price' -> None."""
    if v in (None, "", False, True):
        return None
    if isinstance(v, (int, float)):
        return float(v) or None
    m = re.search(r"(\d[\d,]*(?:\.\d+)?)\s*(k|mm|m|million)?\b", str(v), re.I)
    if not m:
        return None
    try:
        n = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    mult = {"k": 1e3, "m": 1e6, "mm": 1e6, "million": 1e6}.get((m.group(2) or "").lower(), 1)
    return (n * mult) or None


def wp_price_text(text) -> Optional[str]:
    """The broker's own words when a price field withholds the price
    ('Market Price', 'Call for price', 'TBD', '$40k Down' with no total),
    verbatim; else None. Shared rule when price_disclosure is importable."""
    t = re.sub(r"\s+", " ", str(text or "")).strip()
    if not t:
        return None
    if _pd_phrase is not None:
        return _pd_phrase(t)
    if len(t) > 120 or (re.search(r"\$\s?\d", t) and not _WP_DOWN_PAYMENT.search(t)):
        return None
    return t if (_WP_PRICE_HIDDEN.search(t) or _WP_DOWN_PAYMENT.search(t)) else None


def wp_price_hidden(text) -> bool:
    """True when a price slot says it is not disclosed (see wp_price_text)."""
    return wp_price_text(text) is not None


def wp_card_financials(text: str) -> Dict:
    """Labelled money on an index card: 'Price: $1,250,000', 'Price: Market
    Price', 'Revenues: $64,939,613', 'Cash Flow: $2,799,477', 'Asking: $225,000'
    / 'Owner Benefit: $120,000' (truforte), 'Listing Price: $X' / 'Total Revenue: $Y'
    (routeconsultant). A bare '$40k
    Down' is never a price."""
    out = {"price": None, "price_not_disclosed": None, "price_text": None,
           "revenue": None, "cash_flow": None}
    t = re.sub(r"\s+", " ", text or "")
    m = re.search(r"\b(?:asking(?:\s+price)?|list(?:ing)?\s+price|price)" + _WP_LABELLED_MONEY, t, re.I)
    if m and not _WP_DOWN_PAYMENT.match(t[m.start(1):m.start(1) + 40]):
        out["price"] = wp_money(m.group(1))
    if out["price"]:
        out["price_not_disclosed"] = False
    else:
        # The broker's words go on the row as price_text (verbatim): the
        # writers set price_not_disclosed only from an explicit phrase.
        phrase = None
        if _pd_labelled is not None:
            phrase = _pd_labelled(t)
        else:
            m = re.search(r"\b(?:asking(?:\s+price)?|list(?:ing)?\s+price|price)\s*[:\-]\s*([^$\d|]{2,30})",
                          t, re.I)
            if m and wp_price_text(m.group(1)):
                phrase = re.sub(r"\s+", " ", m.group(0)).strip()
        if not phrase:
            d = _WP_DOWN_PAYMENT.search(t)      # "$100k Down" in prose (strategicdc)
            if d:
                phrase = wp_price_text(d.group(0))
        if phrase:
            out["price_not_disclosed"], out["price_text"] = True, phrase
    m = re.search(r"\b(?:(?:gross|total|annual)\s+)?revenues?" + _WP_LABELLED_MONEY, t, re.I)
    if m:
        out["revenue"] = wp_money(m.group(1))
    m = re.search(r"\b(?:cash\s*flow|sde|owner\s+benefit)" + _WP_LABELLED_MONEY, t, re.I)
    if m:
        out["cash_flow"] = wp_money(m.group(1))
    return out


# Card-built titles. listings.routeconsultant.com titles are a bare region
# ("Central Georgia", x17), which public.is_publishable() rejects by design
# (region-grid rule). The card carries region / route type / count; the title
# is rebuilt from them. Identity stays the post URL — never the title.
_WP_COUNT_RE = re.compile(r"^\d[\d,]*\+?\s+(?:routes?|runs?|accounts?|stops?|trucks?|"
                          r"territor(?:y|ies)|units?)$", re.I)


def wp_route_title(region: str, route_type: Optional[str], count: Optional[str] = None) -> str:
    """'Central Georgia', 'Bread', '15 Accounts' -> 'Central Georgia Bread Route, 15 Accounts'."""
    region = (region or "").strip()
    rt = (route_type or "").strip()
    if rt and not re.search(r"\broutes?\b", rt, re.I):
        rt += " Route"
    title = " ".join(x for x in (region, rt) if x)
    return f"{title}, {count.strip()}" if count else title


def _rc_card_title(rec: Dict, strings: List[str]) -> Optional[str]:
    """routeconsultant card: [region, route type, count?, 'Total Revenue: ..', ...]."""
    region = rec.get("title") or (strings[0] if strings else "")
    rest = [x for x in strings if x.strip().lower() != region.strip().lower()]
    rtype = next((x for x in rest if not _WP_COUNT_RE.match(x) and ":" not in x
                  and not re.search(r"view more|\$", x, re.I)), None)
    count = next((x for x in rest if _WP_COUNT_RE.match(x)), None)
    return wp_route_title(region, rtype, count) if rtype else None


WP_CARD_TITLE_BUILDERS = {"listings.routeconsultant.com": _rc_card_title}


def _wp_norm_title(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", html_unescape(t or "").lower()).strip()


def _wp_path(url: str) -> str:
    from urllib.parse import urlparse
    try:
        return (urlparse(url).path or "/").rstrip("/").lower()
    except Exception:
        return ""


# ============================================================================
# GENERIC WORDPRESS REST SCRAPER
# Many brokers run WordPress and expose listings at /wp-json/wp/v2/<type>.
# One class, many brokers — registered as (domain, rest_base, account) tuples.
# ============================================================================

class WPRestScraper:
    """
    Generic WordPress REST scraper. Pulls a custom post type from
    https://<domain>/wp-json/wp/v2/<rest_base>?per_page=100&page=N and paginates
    off the x-wp-totalpages header. Clean JSON — no HTML parsing, browser, or
    proxy. Financials are usually absent from REST (acf often empty); when
    fetch_financials=True it follows each listing's `link` to scrape price/cash
    flow from the detail page.

        WPRestScraper("eatz-associates.com", "listing").scrape(account)
    """

    # Extra status resolvers, each (item, title, fields) -> None or
    # (status, source, verbatim_text). Appended to by other handlers (e.g. a
    # broker-specific status taxonomy); combined with the built-in ones under
    # the same most-final-wins rule.
    status_handlers: list = []

    def __init__(self, domain: str, rest_base: str, fetch_financials: bool = False,
                 index_url: Optional[str] = None, off_index: str = "inactive"):
        self.domain = domain.strip().strip("/")
        self.rest_base = rest_base.strip().strip("/")
        self.fetch_financials = fetch_financials
        # The broker's live "for sale" page (data/wp_rest_brokers.csv
        # index_url; several separated by '|'). When set, a REST post that is
        # not on it is emitted as status='inactive' (see _apply_index).
        self.index_urls = [u.strip() for u in (index_url or "").split("|") if u.strip()]
        # What to do with a REST post that is not on the index:
        #   'inactive' (default) — emit it with status='inactive' so an earlier
        #       active row is retired;
        #   'drop' — leave it out of the result (counted and logged). For a
        #       broker whose off-index rows were already quarantined by hand
        #       (trufortebusinessgroup.com, 2026-11-11): emitting 'inactive'
        #       would overwrite that manual state (principle 14).
        if off_index not in ("inactive", "drop"):
            raise ValueError(f"off_index must be 'inactive' or 'drop', not {off_index!r}")
        self.off_index = off_index
        self.index_stats: Dict = {}
        self.session = self._make_session()
        self._proxies = self._build_proxies()   # used only if REST is blocked
        self._use_proxy = False

    @staticmethod
    def _build_proxies():
        user, pw, host = _proxy_creds()
        if not (user and pw):
            return None
        sid = f"wp{random.randint(100000, 999999)}"
        url = f"http://{user}__cr.us;sessid.{sid}:{pw}@{host}"
        return {"http": url, "https": url}

    def _get(self, url):
        return self.session.get(
            url, timeout=30, proxies=self._proxies if self._use_proxy else None)

    @staticmethod
    def _make_session():
        try:
            return requests.Session(impersonate="chrome120")
        except Exception:
            import requests as _std
            s = _std.Session()
            s.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac "
                              "OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36"})
            return s

    def _api_url(self, page: int) -> str:
        return (f"https://{self.domain}/wp-json/wp/v2/{self.rest_base}"
                f"?per_page=100&page={page}"
                f"&_fields=title,link,slug,date,modified,class_list,acf,meta")

    @staticmethod
    def _clean_title(raw: str) -> str:
        return html_unescape(re.sub(r"<[^>]+>", "", raw or "")).strip()

    @staticmethod
    def _taxonomy(class_list, *keys):
        """Pull a readable value from class_list slugs like
        'listing_industry-restaurant' or 'business_location-california'."""
        for c in class_list or []:
            for key in keys:
                m = re.search(r'(?:^|[_-])' + key + r'[-_](.+)$', c)
                if m:
                    return m.group(1).replace("-", " ").strip()
        return None

    @staticmethod
    def _num(v):
        # First amount only: '$1,750,000 (Business) + $3,450,000 (Real Estate)'
        # used to strip to 17500003450000.
        return wp_money(v)

    def _resolve_status(self, item, title, fields):
        """(status, source, verbatim_text) from the post itself; status is
        'active' with source None when nothing says otherwise. See the
        mapping above WP_SOLD_VALUES."""
        found = []
        for c in item.get("class_list") or []:
            m = _WP_STATUS_TAX.match(c.strip())
            hit = wp_status(m.group(1)) if m else None
            if hit is None and c.strip().lower() == "sold":
                hit = ("sold", True)
            if hit:
                found.append((hit[0], "class_list", c.strip() if hit[1] else None))
        for k, raw in fields.items():
            if not _WP_STATUS_KEY.search(k) or isinstance(raw, (dict, list)):
                continue
            if k.lower().endswith("sold") and raw is True:
                found.append(("sold", f"field:{k}", "true"))
                continue
            hit = wp_status(raw)
            if hit:
                found.append((hit[0], f"field:{k}", str(raw).strip() if hit[1] else None))
        slug = item.get("slug") or ""
        if wp_slug_sold(slug):
            found.append(("sold", "slug", slug))
        if is_sold_or_pending(title):
            m = re.search(r"\(?\s*(?:sold\b[^)]*|under[\s-]*contract|sale[\s-]*pending|"
                          r"pending|just[\s-]*closed|recently[\s-]*sold)\s*\)?", title, re.I)
            text = m.group(0).strip() if m else title
            # under contract / pending in a title: still listed (decision 6)
            v = ("active" if re.search(r"contract|pending", text, re.I)
                 and not re.search(r"\bsold\b", text, re.I) else "sold")
            found.append((v, "title", text))
        for h in self.status_handlers:
            try:
                r = h(item, title, fields)
            except Exception as e:
                print(f"[WP:{self.domain}] status handler {getattr(h, '__name__', h)} failed: {e}")
                r = None
            if r:
                found.append(r)
        if not found:
            return "active", None, None
        # Most final wins; between two 'active' hits the one with a label
        # (Under Contract) beats a plain For Sale term.
        st, src, text = max(found, key=lambda f: (_WP_STATUS_RANK[f[0]], f[2] is not None))
        return st, src, text

    def _parse(self, item, broker_account):
        link = item.get("link") or ""
        if not link:
            return None
        title = self._clean_title((item.get("title") or {}).get("rendered", ""))
        if not title:
            slug = re.sub(r"[-_]+", " ", item.get("slug") or "").strip()
            title = slug.title() if len(slug) >= 6 else None
        if not title:
            return None

        cl = item.get("class_list") or []
        # Not everything in a listing post type is a live business for sale.
        # A listing_category of space-for-lease / property / real-estate is CRE
        # or a lease, not a Main-Street business — skip it (x-wp-total overcounts).
        cat = self._taxonomy(cl, "listing_category", "category")
        if cat and re.search(r'(?:^|\b)(?:space[\s-]?for[\s-]?lease|for[\s-]?lease|'
                             r'lease|land|real[\s-]?estate|commercial[\s-]?property)\b',
                             cat, re.I):
            return None

        # Real industry taxonomy (industry-boutiques), NOT the post-type slug
        # (type-business_listing) — exclude "type".
        business_type = self._taxonomy(cl, "industry", "category", "sector", "route_type")
        loc = self._taxonomy(cl, "location", "region")
        st = self._taxonomy(cl, "state")
        state = st.upper() if st and re.fullmatch(r"[a-z]{2}", st.strip(), re.I) else None

        acf = item.get("acf") or {}
        # Some themes expose listing fields in REST `meta` instead of ACF, prefixed
        # by post type (thomashospitality.com: business_price, business_availability).
        meta = item.get("meta")
        if isinstance(meta, dict) and meta:
            folded = {re.sub(r"^(?:business|listing|property)_", "", k): v
                      for k, v in meta.items() if not isinstance(v, (dict, list))}
            acf = {**folded, **(acf if isinstance(acf, dict) else {})}
        avail = str(acf.get("availability") or "").strip().lower()
        if avail and re.search(r"\blease\b|\brent\b", avail):
            return None
        status, status_source, status_text = self._resolve_status(
            item, title, acf if isinstance(acf, dict) else {})
        raw_price = acf.get("asking_price") or acf.get("price") or acf.get("list_price")
        price = self._num(raw_price)
        price_text = None if price else wp_price_text(raw_price)
        price_not_disclosed = False if price else (True if price_text else None)
        cash_flow = self._num(acf.get("cash_flow") or acf.get("cashflow") or acf.get("sde"))
        revenue = self._num(acf.get("revenue") or acf.get("gross_revenue"))

        rec = format_listing(
            url=link, broker_account=broker_account, title=title,
            price=price, cash_flow=cash_flow, revenue=revenue,
            business_type=(business_type.title() if business_type else None),
            city=(acf.get("city") or None), state=(state or acf.get("state") or None),
            location=loc.title() if loc else None, status=status, price_text=price_text,
            broker_published_on=wp_date(item.get("date")),
            broker_modified_on=wp_date(item.get("modified")),
            broker_date_source="wordpress_rest")
        # Carried on the row for logs/snapshots. upsert_listings() builds its
        # row from named keys, so none of these reach listings_direct today
        # (there is no column for them); status itself does. price_text (set
        # through format_listing) is the broker's verbatim withheld-price
        # phrase: the W14 writers derive price_not_disclosed from it.
        rec["status_source"] = status_source
        rec["broker_status_text"] = status_text
        rec["price_not_disclosed"] = price_not_disclosed
        rec["wp_slug"] = item.get("slug") or None
        return rec

    def scrape(self, broker_account: str, max_pages: int = 60, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print(f"WordPress REST: {self.domain}/wp-json/wp/v2/{self.rest_base}")
            print('='*60)

        listings, seen = [], set()
        total_pages = None
        for page in range(1, max_pages + 1):
            try:
                r = self._get(self._api_url(page))
                # Cloudflare-blocked endpoints return an HTML challenge instead
                # of JSON — on page 1, retry once through the proxy.
                if (page == 1 and not self._use_proxy and self._proxies
                        and "json" not in r.headers.get("content-type", "").lower()):
                    if verbose:
                        print(f"[WP:{self.domain}] non-JSON on page 1 — retrying via proxy")
                    self._use_proxy = True
                    r = self._get(self._api_url(page))
            except Exception as e:
                if verbose:
                    print(f"[WP:{self.domain}] page {page} error: {e}")
                break
            if r.status_code != 200:
                # 400 rest_post_invalid_page_number = past the last page
                if verbose and page == 1:
                    print(f"[WP:{self.domain}] HTTP {r.status_code}: {r.text[:120]}")
                break
            if total_pages is None:
                try:
                    total_pages = int(r.headers.get("x-wp-totalpages", "1") or 1)
                    print(f"[WP:{self.domain}] x-wp-total="
                          f"{r.headers.get('x-wp-total','?')} pages={total_pages}")
                except ValueError:
                    total_pages = max_pages
            try:
                items = r.json()
            except Exception:
                break
            if not isinstance(items, list) or not items:
                break
            for it in items:
                rec = self._parse(it, broker_account)
                if rec and rec["listing_url"] not in seen:
                    seen.add(rec["listing_url"])
                    listings.append(rec)
            if total_pages and page >= total_pages:
                break
            time.sleep(0.2)

        if self.index_urls and listings:
            self._apply_index(listings, verbose)
        elif listings and verbose:
            print(f"[WP:{self.domain}] no index_url — status from REST fields only; "
                  f"posts the broker has taken down cannot be detected")
        self._drop_not_emitted(listings, verbose)

        if self.fetch_financials:
            self._enrich_financials(listings, verbose)

        if verbose:
            wp = sum(1 for l in listings if l.get("price"))
            by = {}
            for l in listings:
                by[l["status"]] = by.get(l["status"], 0) + 1
            print(f"\n✓ {len(listings)} listings from {self.domain} ({wp} with price) "
                  f"status={by}")
        return listings

    def _drop_not_emitted(self, listings, verbose=True):
        """Remove status=DROP rows (Coming Soon — decision 5) in place; the
        count goes in index_stats['dropped_status'] and the log. Never
        silent: a broker whose whole feed is Coming Soon shows up here."""
        dropped = [l for l in listings if l.get("status") == WP_DROP]
        if not dropped:
            return
        by = {}
        for l in dropped:
            k = l.get("broker_status_text") or "?"
            by[k] = by.get(k, 0) + 1
        listings[:] = [l for l in listings if l.get("status") != WP_DROP]
        self.index_stats = {**(self.index_stats or {}), "dropped_status": len(dropped)}
        print(f"[WP:{self.domain}] status DROP: {len(dropped)} rows not emitted "
              f"({', '.join(f'{k}: {v}' for k, v in sorted(by.items()))})")

    # ── live index intersection ────────────────────────────────────────────
    def _fetch_index_pages(self, verbose=True, max_pages=25):
        """GET each index_url and follow its plain pagination (/page/N/,
        ?paged=N, rel=next). Returns [(url, html)]; raises if the first page
        of any index_url cannot be read.

        'POST <url>' instead fetches a theme endpoint that returns the rendered
        result grid as JSON {"data": "<html>"} for an empty filter — the
        request the site's own JS makes (listings.routeconsultant.com:
        wp-json/routeconsultant/v1/filter-routes). No pagination."""
        from urllib.parse import urljoin
        pages, seen = [], set()
        for start in self.index_urls:
            if start.upper().startswith("POST "):
                url = start[5:].strip()
                r = self.session.post(url, json={}, timeout=60,
                                      proxies=self._proxies if self._use_proxy else None)
                if r.status_code != 200:
                    raise RuntimeError(f"index POST {url} HTTP {r.status_code}")
                try:
                    data = r.json()
                except Exception:
                    raise RuntimeError(f"index POST {url} returned non-JSON")
                body = data.get("data") if isinstance(data, dict) else None
                if not isinstance(body, str) or not body.strip():
                    raise RuntimeError(f"index POST {url}: no 'data' html in response")
                pages.append((url, body))
                continue
            queue = [start]
            while queue and len(pages) < max_pages:
                url = queue.pop(0)
                if url in seen or re.search(r"/page/1/?$|[?&](?:paged|page|pg)=1$", url):
                    continue
                seen.add(url)
                r = self._get(url)
                if r.status_code != 200:
                    if url == start:
                        raise RuntimeError(f"index {url} HTTP {r.status_code}")
                    continue
                body = r.text or ""
                if re.search(r"cf-chl|cf_chl_opt|<title>Just a moment", body):
                    raise RuntimeError(f"index {url} returned a Cloudflare challenge")
                pages.append((url, body))
                base = start.split("?")[0].rstrip("/")
                for h in re.findall(r'href="([^"#]+)"', body):
                    u = html_unescape(urljoin(url, h))
                    if (u.split("?")[0].rstrip("/").startswith(base)
                            and re.search(r"/page/\d+/?$|[?&](?:paged|page|pg)=\d+$", u)
                            and u not in seen):
                        queue.append(u)
                time.sleep(0.5)
        return pages

    @staticmethod
    def _card_for(tag, max_up=8):
        """The listing card around an anchor: the nearest ancestor whose
        parent repeats it (>=3 siblings with the same tag and class) — the
        grid item. Falls back to the anchor itself."""
        node = tag
        for _ in range(max_up):
            parent = node.parent
            if parent is None or parent.name in ("body", "html", "[document]"):
                break
            sig = (node.name, tuple(node.get("class") or ()))
            same = sum(1 for c in parent.find_all(node.name, recursive=False)
                       if (c.name, tuple(c.get("class") or ())) == sig)
            if same >= 3:
                return node
            node = parent
        return tag

    @staticmethod
    def _partial_index_reason(pages) -> Optional[str]:
        """An index that loads the rest of its cards by AJAX (JetEngine
        'Load More', FacetWP pager) shows only page one in the HTML; taking it
        as the whole inventory would retire every listing past page one."""
        for url, body in pages:
            jet = max((int(x) for x in re.findall(r'data-pages="(\d+)"', body)), default=0)
            if jet > 1:
                return f"{url}: JetEngine grid with data-pages={jet} (AJAX pagination)"
            m = re.search(r'"total_rows":(\d+)[^}]*?"per_page":(\d+)|"per_page":(\d+)[^}]*?"total_rows":(\d+)', body)
            if m:
                rows = int(m.group(1) or m.group(4)); per = int(m.group(2) or m.group(3))
                if rows > per:
                    return f"{url}: FacetWP pager total_rows={rows} per_page={per} (AJAX pagination)"
        return None

    @staticmethod
    def _card_for_key(tag, key, key_of, max_text=4000):
        node = tag
        while node.parent is not None and node.parent.name not in ("body", "html", "[document]"):
            parent = node.parent
            keys = {key_of(x) for x in parent.find_all("a", href=True)} - {None}
            if keys - {key} or len(parent.get_text(" ", strip=True)) > max_text:
                break
            node = parent
        return node

    def _parse_index(self, pages, rest_paths, rest_slugs):
        """{path: card} for cards linking a REST post, plus badge cards that
        link elsewhere (strategicdc's sold cards all point at one
        /recently-sold.../ page) as [(normalised text set, status, badge)]."""
        from urllib.parse import urljoin
        on_index, unlinked = {}, []
        for url, body in pages:
            soup = BeautifulSoup(body, "html.parser")
            for s in soup(["script", "style", "noscript"]):
                s.decompose()
            def key_of(tag):
                path = _wp_path(urljoin(url, tag.get("href") or ""))
                last = path.rsplit("/", 1)[-1]
                return path if path in rest_paths else (last if last in rest_slugs else None)

            done = set()
            for a in soup.find_all("a", href=True):
                key = key_of(a)
                # A card linking a REST post: the widest ancestor that links
                # no other post. Any other card: the repeated grid item.
                card = self._card_for_key(a, key, key_of) if key else self._card_for(a)
                if id(card) in done:
                    continue
                strings = [s.strip() for s in card.stripped_strings]
                badge = next(((wp_badge_status(s), s) for s in strings if wp_badge_status(s)),
                             (None, None))
                if key:
                    done.add(id(card))
                    info = on_index.setdefault(key, {"badge": None, "badge_text": None,
                                                     "label": None, "text": ""})
                    lab = next((m.group(1).strip() for m in
                                (re.match(r"^status\s*:\s*(.{2,40})$", x, re.I) for x in strings)
                                if m), None)
                    if lab and not info["label"]:
                        info["label"] = lab
                    if badge[0] and (info["badge"] is None or
                                     _WP_STATUS_RANK[badge[0]] > _WP_STATUS_RANK[info["badge"]]):
                        info["badge"], info["badge_text"] = badge
                    if len(" ".join(strings)) > len(info["text"]):
                        info["text"] = " ".join(strings)
                        info["strings"] = strings
                elif badge[0] and card is not a:
                    done.add(id(card))
                    unlinked.append(({_wp_norm_title(s) for s in strings if len(s) >= 8},
                                     badge[0], badge[1]))
                elif badge[0] and card is a and len(strings) >= 2:
                    done.add(id(card))
                    unlinked.append(({_wp_norm_title(s) for s in strings if len(s) >= 8},
                                     badge[0], badge[1]))
        return on_index, unlinked

    def _apply_index(self, listings, verbose=True):
        """Keep as active only the REST posts that are on the live index.
        A post that is not there is emitted with status='inactive'
        (status_source='not_on_index') — never dropped, never written active.
        If the index cannot be read, or matches nothing, fail loud: every row
        keeps its REST status and is flagged index_check='failed'."""
        rest_paths = {_wp_path(l["listing_url"]) for l in listings}
        rest_slugs = {l.get("wp_slug") for l in listings if l.get("wp_slug")}
        err = None
        try:
            pages = self._fetch_index_pages(verbose)
            on_index, unlinked = self._parse_index(pages, rest_paths, rest_slugs)
        except Exception as e:
            err, on_index, unlinked, pages = f"{type(e).__name__}: {e}", {}, [], []
        n_rest = len(listings)
        if not err:
            err = self._partial_index_reason(pages)
        if not err and not on_index:
            err = f"0 of {n_rest} REST posts found on {len(pages)} index page(s)"
        # A partial page (AJAX "load more", a JS grid) looks like a broker
        # that delisted most of its inventory. If REST's own status field
        # says many more are active than the index shows, trust neither.
        rest_active = sum(1 for l in listings
                          if l["status"] == "active" and l.get("status_source"))
        if not err and rest_active >= 5 and len(on_index) < 0.5 * rest_active:
            err = (f"index shows {len(on_index)} posts but REST status fields "
                   f"mark {rest_active} active — index likely partial")
        if err:
            self.index_stats = {"rest": n_rest, "index_error": err}
            if self.off_index == "drop":
                # The index IS the inventory for this broker; without it the
                # REST set is mostly archive. Return nothing (the runner closes
                # the crawl as failed/EMPTY) rather than 700 archived posts.
                print(f"[WP:{self.domain}] INDEX CHECK FAILED ({err}) — off_index=drop, "
                      f"returning 0 of {n_rest} REST posts")
                listings[:] = []
                return
            for l in listings:
                l["index_check"] = "failed"
            print(f"[WP:{self.domain}] INDEX CHECK FAILED ({err}) — returning REST "
                  f"statuses unchanged for {n_rest} posts; flagged index_check=failed")
            return

        sold_titles = {}
        for texts, st, badge in unlinked:
            for t in texts:
                if t not in sold_titles or _WP_STATUS_RANK[st] > _WP_STATUS_RANK[sold_titles[t][0]]:
                    sold_titles[t] = (st, badge)
        counts = {"rest": n_rest, "index_pages": len(pages), "on_index": 0,
                  "badge_inactive": 0, "badge_sold": 0, "badge_active": 0,
                  "badge_drop": 0, "title_matched": 0, "not_on_index": 0}
        for l in listings:
            key = _wp_path(l["listing_url"])
            card = on_index.get(key) or on_index.get(l.get("wp_slug") or "")
            if card:
                counts["on_index"] += 1
                l["index_check"] = "on_index"
                if card["badge"] and _WP_STATUS_RANK[card["badge"]] >= _WP_STATUS_RANK[l["status"]]:
                    l["status"], l["status_source"] = card["badge"], "index_badge"
                    l["broker_status_text"] = card["badge_text"]
                    counts["badge_" + card["badge"].lower()] += 1
                elif card.get("label") and not l.get("broker_status_text"):
                    # "Status: Under Contract" (routeconsultant): carried
                    # verbatim; an under-contract route is still on the
                    # broker's live list (decision 6), so status stays. A
                    # label the shared set calls sold / Coming Soon does
                    # change it.
                    l["broker_status_text"] = card["label"]
                    counts["labelled"] = counts.get("labelled", 0) + 1
                    lst = wp_badge_status(card["label"])
                    if lst and _WP_STATUS_RANK[lst] > _WP_STATUS_RANK[l["status"]]:
                        l["status"], l["status_source"] = lst, "index_label"
                builder = WP_CARD_TITLE_BUILDERS.get(self.domain)
                if builder:
                    t = builder(l, card.get("strings") or [])
                    if t:
                        l["title"] = t
                fin = wp_card_financials(card["text"])
                if not l.get("price") and fin["price"]:
                    l["price"] = fin["price"]
                if l.get("price"):
                    l["price_not_disclosed"] = False
                elif fin["price_not_disclosed"]:
                    l["price_not_disclosed"] = True
                    l["price_text"] = fin["price_text"]
                for k in ("revenue", "cash_flow"):
                    if not l.get(k) and fin[k]:
                        l[k] = fin[k]
                continue
            l["index_check"] = "not_on_index"
            if self.domain in WP_CARD_TITLE_BUILDERS and l.get("business_type"):
                l["title"] = wp_route_title(l.get("title"), l["business_type"])
            hit = sold_titles.get(_wp_norm_title(l.get("title")))
            if hit:
                counts["title_matched"] += 1
                if _WP_STATUS_RANK[hit[0]] >= _WP_STATUS_RANK[l["status"]]:
                    l["status"], l["status_source"], l["broker_status_text"] = (
                        hit[0], "index_badge_title", hit[1])
                continue
            counts["not_on_index"] += 1
            if self.off_index == "drop":
                l["_drop_off_index"] = True
                continue
            if l["status"] == "active":
                l["status"], l["status_source"], l["broker_status_text"] = (
                    "inactive", "not_on_index", None)
        if self.off_index == "drop":
            keep = [l for l in listings if not l.pop("_drop_off_index", False)]
            counts["dropped_off_index"] = len(listings) - len(keep)
            listings[:] = keep
        self.index_stats = counts
        if verbose:
            print(f"[WP:{self.domain}] index check: rest={n_rest} "
                  f"on_index={counts['on_index']} (badged inactive={counts['badge_inactive']}, "
                  f"sold={counts['badge_sold']}, under-contract active={counts['badge_active']}, "
                  f"coming-soon drop={counts['badge_drop']}) title-matched badge cards="
                  f"{counts['title_matched']} not_on_index={counts['not_on_index']} "
                  + (f"(DROPPED {counts['dropped_off_index']} — off_index=drop)"
                     if self.off_index == "drop" else "(emitted non-active, not dropped)"))

    @staticmethod
    def _detail_financials(html_text: str) -> Dict:
        """Price / revenue / cash flow from a listing's own detail page.
        Reads only the block around the FIRST price label, never the whole
        page: synergybb's detail pages carry 'Asking Price: $X' cards for
        other listings in a sidebar further down."""
        soup = BeautifulSoup(html_text or "", "html.parser")
        for t in soup(["script", "style", "noscript", "nav", "header", "footer"]):
            t.decompose()
        label = soup.find(string=re.compile(
            r"^\s*(?:asking\s+price|list(?:ing)?\s+price|price)\s*:?\s*$", re.I))
        if label is None:
            return {"price": None, "price_not_disclosed": None, "revenue": None, "cash_flow": None}
        block = label.parent
        for _ in range(3):
            up = block.parent
            if up is None or len(up.get_text(" ", strip=True)) > 600:
                break
            block = up
        return wp_card_financials(block.get_text(" ", strip=True))

    def _enrich_financials(self, listings, verbose=True):
        """Follow each ACTIVE listing's detail page for price / revenue / cash
        flow when REST and the index card gave none (opt-in per broker:
        data/wp_rest_brokers.csv fetch_financials=yes — slow, one GET each)."""
        todo = [r for r in listings if r.get("status") == "active" and not r.get("price")]
        got = failed = 0
        for i, rec in enumerate(todo):
            try:
                r = self.session.get(rec["listing_url"], timeout=20)
                fin = self._detail_financials(r.text if r.status_code == 200 else "")
            except Exception:
                fin, failed = None, failed + 1
            if fin:
                if fin["price"]:
                    rec["price"], rec["price_not_disclosed"] = fin["price"], False
                    got += 1
                elif fin["price_not_disclosed"]:
                    rec["price_not_disclosed"] = True
                    rec["price_text"] = fin["price_text"]
                for k in ("revenue", "cash_flow"):
                    if not rec.get(k) and fin[k]:
                        rec[k] = fin[k]
            time.sleep(0.5)
        if verbose:
            print(f"[WP:{self.domain}] detail financials: {len(todo)} active without price, "
                  f"{got} priced from detail page, {failed} fetch errors")


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

    # Vested Business Brokers
    if 'vested' in name or 'vestedbb.com' in url:
        return VestedScraper().scrape(broker_account=account, verbose=verbose)

    # Routes For Sale
    if 'routesforsale' in url or 'routes for sale' in name:
        return RoutesForSaleScraper().scrape(broker_account=account, verbose=verbose)

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
        'vr', 'fcbb', 'link', 'bodner', 'wesell', 'vested', 'routesforsale', 'all'
    ])
    parser.add_argument("--account", default="test-123", help="Broker account ID")
    
    args = parser.parse_args()
    
    scrapers = {
        'murphy': lambda: MurphyScraper.scrape(args.account, max_pages=50),
        'hedgestone': lambda: HedgestoneScraper().scrape(args.account, max_pages=40),
        'transworld': lambda: TransworldScraper().scrape(args.account, max_pages=385),
        'sunbelt': lambda: SunbeltScraper().scrape(args.account, max_pages=130),
        'vr': lambda: VRScraper().scrape(args.account, max_pages=40),
        'fcbb': lambda: FCBBScraper().scrape(args.account, max_pages=79),
        'link': lambda: LinkBusinessScraper().scrape(args.account, max_pages=20),
        'bodner': lambda: LarryBodnerScraper().scrape(args.account),
        'wesell': lambda: WeSellRestaurantsScraper().scrape(args.account, max_pages=40),
        'vested': lambda: VestedScraper().scrape(args.account, max_pages=130),
        'routesforsale': lambda: RoutesForSaleScraper().scrape(args.account),
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