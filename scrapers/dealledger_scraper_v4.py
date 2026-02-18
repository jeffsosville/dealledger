"""
DealLedger Standalone Scraper V4
=================================
Based on Unified Production Scraper V3 - ALL the intelligence, NONE of the Supabase.

Kept from V3:
- PatternDetector (repeating HTML structure detection)
- SmartExtractor (title, price, revenue, cashflow, location extraction)
- ML Pattern Learning (learns which patterns work per domain, predicts for new ones)
- FailureAnalyzer (classifies WHY scrapes fail)
- Pagination (auto-follows next page links, up to 100 pages)
- Proxy support
- Vertical classification (auto-tags each listing)
- File download parsing (xlsx/csv)

Changed from V3:
- No Supabase dependency - reads CSV, outputs JSON/CSV
- Pattern DB stored as local JSON (data/pattern_cache.json)
- Failure log stored as local JSON
- Specialized scrapers are OPTIONAL (graceful skip if deps missing)
- No vertical filtering by default (DealLedger captures everything)
- Outputs to data/snapshots/YYYY-MM-DD/ for public ledger

Usage:
    python dealledger_scraper_v4.py --brokers data/brokers.csv --test
    python dealledger_scraper_v4.py --brokers data/brokers.csv --top-n 50
    python dealledger_scraper_v4.py --brokers data/brokers.csv --all
    python dealledger_scraper_v4.py --brokers data/brokers.csv --top-n 100 --vertical cleaning
"""

import os
import re
import json
import hashlib
import asyncio
import random
import time
import argparse
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from datetime import datetime, timezone, date
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Try to import specialized scrapers (optional)
SPECIALIZED_AVAILABLE = False
try:
    from specialized_scrapers_integration import scrape_specialized_broker, get_specialized_broker_names
    SPECIALIZED_AVAILABLE = True
except ImportError:
    pass


# ============================================================================
# VERTICAL CONFIGURATIONS
# ============================================================================

VERTICAL_CONFIGS = {
    'cleaning': {
        'name': 'Cleaning Services',
        'include_keywords': [
            'cleaning', 'janitorial', 'custodial', 'sanitation', 'maintenance',
            'maid service', 'housekeeping', 'carpet cleaning', 'window cleaning',
            'pressure washing', 'commercial cleaning', 'residential cleaning',
            'floor care', 'disinfection', 'restoration'
        ],
        'exclude_keywords': [
            'restaurant', 'food service', 'hvac', 'plumbing', 'electrical',
            'landscaping', 'lawn care', 'pool', 'spa', 'salon'
        ]
    },
    'landscape': {
        'name': 'Landscape Services',
        'include_keywords': [
            'landscape', 'landscaping', 'lawn care', 'lawn maintenance',
            'irrigation', 'hardscape', 'tree service', 'snow removal',
            'lawn mowing', 'garden', 'turf care', 'lawn treatment',
            'landscape design', 'outdoor living'
        ],
        'exclude_keywords': [
            'restaurant', 'food service', 'hvac', 'plumbing', 'electrical',
            'cleaning', 'janitorial', 'pool', 'spa'
        ]
    },
    'hvac': {
        'name': 'HVAC Services',
        'include_keywords': [
            'hvac', 'heating', 'cooling', 'air conditioning', 'furnace',
            'ventilation', 'refrigeration', 'climate control', 'ductwork',
            'heat pump', 'ac repair', 'hvac contractor', 'hvac service'
        ],
        'exclude_keywords': [
            'restaurant', 'food service', 'cleaning', 'janitorial',
            'landscaping', 'lawn care', 'pool', 'spa', 'plumbing', 'electrical'
        ]
    },
    'vending': {
        'name': 'Vending Services',
        'include_keywords': [
            'vending', 'vending machine', 'atm', 'amusement', 'arcade',
            'coin-op', 'laundromat', 'car wash', 'self-service'
        ],
        'exclude_keywords': [
            'restaurant', 'food service', 'hvac', 'plumbing', 'electrical',
            'landscaping', 'lawn care', 'cleaning', 'janitorial'
        ]
    }
}

ALL_VERTICAL_KEYWORDS = {}
for slug, config in VERTICAL_CONFIGS.items():
    for kw in config['include_keywords']:
        ALL_VERTICAL_KEYWORDS[kw] = slug


# ============================================================================
# FINANCIAL EXTRACTION PATTERNS
# ============================================================================

PRICE_RE = re.compile(r'\$[\d,]+(?:\.\d{2})?', re.I)

REVENUE_PATTERNS = [
    r'revenue[:\s]*\$?([\d,]+)',
    r'gross sales[:\s]*\$?([\d,]+)',
    r'annual sales[:\s]*\$?([\d,]+)',
    r'sales[:\s]*\$?([\d,]+)',
]

CASHFLOW_PATTERNS = [
    r'cash flow[:\s]*\$?([\d,]+)',
    r'net income[:\s]*\$?([\d,]+)',
    r'ebitda[:\s]*\$?([\d,]+)',
    r'sde[:\s]*\$?([\d,]+)',
    r'owner benefit[:\s]*\$?([\d,]+)',
]

RE_REAL_ESTATE = re.compile(r'\bmls\s*#|\bidx\b|\d+\s*bed.*\d+\s*bath', re.I)

BUSINESS_HINTS = [
    'asking price', 'cash flow', 'revenue', 'business for sale', 'training', 'turnkey', 'profitable',
    'route type', 'financing', 'route details', 'distribution', 'delivery route', 'route business',
    'gross sales', 'net income', 'ebitda', 'asking', 'price', 'business opportunity', 'owner', 'operated',
    'franchise', 'inventory', 'equipment', 'lease', 'real estate included', 'seller financing',
    'terms available', 'business type', 'years in business', 'employees', 'customers', 'clientele',
    'ff&e', 'fixtures', 'goodwill', 'for sale by owner', 'restaurant', 'retail', 'service', 'manufacturing',
    'wholesale', 'distribution', 'franchise opportunity', 'absentee', 'semi-absentee', 'owner-operator'
]


def looks_businessy(text: str) -> bool:
    t = (text or "").lower()
    return any(k in t for k in BUSINESS_HINTS)


def parse_money_value(text: str) -> Optional[float]:
    if not text:
        return None
    try:
        cleaned = re.sub(r'[$,]', '', str(text))
        if 'k' in cleaned.lower():
            return float(cleaned.lower().replace('k', '')) * 1000
        if 'm' in cleaned.lower():
            return float(cleaned.lower().replace('m', '')) * 1000000
        return float(cleaned)
    except:
        return None


def extract_city_state(location: str) -> tuple:
    if not location:
        return None, None
    m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b', location)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    state_match = re.search(r'\b([A-Z]{2})\b', location)
    if state_match:
        return None, state_match.group(1)
    return None, None


def classify_vertical(text: str) -> Optional[str]:
    t = (text or "").lower()
    scores = defaultdict(int)
    for kw, vertical in ALL_VERTICAL_KEYWORDS.items():
        if kw in t:
            scores[vertical] += 1
    if scores:
        return max(scores, key=scores.get)
    return None


# ============================================================================
# LOCAL PATTERN DATABASE (replaces Supabase)
# ============================================================================

class PatternDatabase:
    def __init__(self, cache_path: str = "data/pattern_cache.json"):
        self.cache_path = Path(cache_path)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.patterns = {}
        self.load()

    def load(self):
        if self.cache_path.exists():
            try:
                with open(self.cache_path) as f:
                    self.patterns = json.load(f)
                print(f"Loaded {len(self.patterns)} patterns from local cache")
            except Exception as e:
                print(f"Warning: Could not load pattern cache: {e}")
                self.patterns = {}
        else:
            print("No pattern cache found (will learn from scratch)")

    def save(self):
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.patterns, f, indent=2)
        except Exception as e:
            print(f"Warning: Could not save pattern cache: {e}")

    def record_success(self, url: str, pattern_signature: str, listings_count: int):
        domain = urlparse(url).netloc.replace('www.', '')
        if domain not in self.patterns:
            self.patterns[domain] = {
                'pattern': pattern_signature,
                'success_count': 0,
                'total_listings': 0,
                'first_seen': datetime.now().isoformat(),
                'last_used': datetime.now().isoformat()
            }
        self.patterns[domain]['pattern'] = pattern_signature
        self.patterns[domain]['success_count'] += 1
        self.patterns[domain]['total_listings'] += listings_count
        self.patterns[domain]['last_used'] = datetime.now().isoformat()
        self.save()

    def get_pattern_for_domain(self, domain: str) -> Optional[Dict]:
        return self.patterns.get(domain)

    def predict_pattern(self, url: str, available_patterns: List[str]) -> Optional[str]:
        domain = urlparse(url).netloc.replace('www.', '')
        if domain in self.patterns:
            return self.patterns[domain]['pattern']
        similar = self._find_similar_domains(domain)
        if not similar:
            return None
        pattern_scores = defaultdict(float)
        for similar_domain, similarity_score in similar[:5]:
            if similar_domain in self.patterns:
                pattern = self.patterns[similar_domain]['pattern']
                if pattern in available_patterns:
                    weight = similarity_score * self.patterns[similar_domain]['success_count']
                    pattern_scores[pattern] += weight
        if pattern_scores:
            best = max(pattern_scores, key=pattern_scores.get)
            print(f"    ML Prediction: Using pattern similar to {similar[0][0]}")
            return best
        return None

    def _find_similar_domains(self, target_domain: str) -> List[tuple]:
        sims = []
        for domain in self.patterns.keys():
            similarity = self._domain_similarity(target_domain, domain)
            if similarity > 0.3:
                sims.append((domain, similarity))
        sims.sort(key=lambda x: x[1], reverse=True)
        return sims

    def _domain_similarity(self, d1: str, d2: str) -> float:
        def trigrams(s): return set(s[i:i+3] for i in range(len(s)-2))
        t1, t2 = trigrams(d1), trigrams(d2)
        if not t1 or not t2: return 0.0
        return len(t1 & t2) / len(t1 | t2)

    def get_stats(self) -> Dict:
        return {
            'total_patterns': len(self.patterns),
            'total_scrapes': sum(p['success_count'] for p in self.patterns.values()),
            'total_listings': sum(p['total_listings'] for p in self.patterns.values()),
        }


# ============================================================================
# FAILURE ANALYZER (local)
# ============================================================================

class FailureAnalyzer:
    def __init__(self, output_dir: Path):
        self.failures = []
        self.output_dir = output_dir

    def classify_failure(self, error: str, http_status: Optional[int],
                        html: Optional[str]) -> tuple:
        error_lower = (error or "").lower()
        html_lower = (html or "").lower() if html else ""

        if http_status == 403:
            return 'HTTP_403', "Site blocking (403) - anti-bot protection"
        elif http_status == 404:
            return 'HTTP_404', "Page not found (404) - URL may be outdated"
        elif http_status and http_status >= 500:
            return 'HTTP_500', f"Server error ({http_status})"
        if 'timeout' in error_lower or 'timed out' in error_lower:
            return 'TIMEOUT', "Connection timeout"
        if 'ssl' in error_lower or 'certificate' in error_lower:
            return 'SSL_ERROR', "SSL certificate error"
        if html and ('recaptcha' in html_lower or 'captcha' in html_lower):
            return 'CAPTCHA', "CAPTCHA protection detected"
        if 'no pattern' in error_lower or 'no business listings' in error_lower:
            return 'NO_PATTERN', "Could not detect listing pattern"
        if html and len(html) < 10000:
            return 'JAVASCRIPT_HEAVY', "Minimal content - JavaScript-heavy"
        return 'UNKNOWN', f"Unknown: {error[:200]}"

    def log_failure(self, broker: Dict, failure_type: str, error_detail: str,
                   http_status: Optional[int] = None):
        self.failures.append({
            'broker_id': broker.get('id') or broker.get('account'),
            'broker_name': broker.get('name'),
            'broker_url': broker.get('url'),
            'failure_type': failure_type,
            'error_detail': error_detail[:500],
            'http_status': http_status,
            'failed_at': datetime.now().isoformat()
        })

    def save(self):
        if self.failures:
            path = self.output_dir / "failures.json"
            with open(path, 'w') as f:
                json.dump(self.failures, f, indent=2)
            print(f"Saved {len(self.failures)} failures to {path}")


# ============================================================================
# PATTERN DETECTOR (from V3)
# ============================================================================

class PatternDetector:
    @staticmethod
    def find_patterns(soup: BeautifulSoup) -> List[Dict]:
        signatures = defaultdict(list)
        for element in soup.find_all(['div', 'article', 'section', 'li', 'tr']):
            depth = len(list(element.parents))
            if depth < 3 or depth > 15:
                continue
            sig = PatternDetector._get_signature(element)
            if sig:
                signatures[sig].append(element)

        patterns = []
        for sig, elements in signatures.items():
            if len(elements) >= 3:
                valid = [el for el in elements if el.find('a', href=True) and len(el.get_text(strip=True)) > 50]
                if len(valid) >= 3:
                    patterns.append({
                        'signature': sig,
                        'elements': valid,
                        'count': len(valid),
                        'avg_text_length': sum(len(el.get_text()) for el in valid) / len(valid)
                    })
        patterns.sort(key=lambda x: x['count'], reverse=True)
        return patterns

    @staticmethod
    def _get_signature(element) -> str:
        try:
            parts = [element.name]
            child_tags = [c.name for c in element.find_all(recursive=False) if hasattr(c, 'name')]
            if child_tags:
                parts.append(f"children:{','.join(sorted(set(child_tags)))}")
            if element.find('a', href=True):
                parts.append('has_link')
            if element.find('img'):
                parts.append('has_img')
            text_len = len(element.get_text(strip=True))
            if text_len > 200:
                parts.append('text:long')
            elif text_len > 50:
                parts.append('text:medium')
            if re.search(r'\$[\d,]+', element.get_text()):
                parts.append('has_price')
            return '|'.join(parts)
        except:
            return ''


# ============================================================================
# SMART EXTRACTOR (from V3)
# ============================================================================

class SmartExtractor:
    @staticmethod
    def extract(element, base_url: str) -> Optional[Dict]:
        try:
            text = element.get_text(' ', strip=True)
            if len(text) < 30:
                return None
            link = element.find('a', href=True) or element.find_parent('a', href=True)
            if not link:
                return None
            url = urljoin(base_url, link['href'])
            if any(skip in url.lower() for skip in ['#', 'javascript:', '/contact', '/about']):
                return None

            title = SmartExtractor._extract_title(element, text)
            price_text = SmartExtractor._extract_price_text(text)
            price = parse_money_value(price_text)
            location = SmartExtractor._extract_location(text)
            city, state = extract_city_state(location)
            business_type = SmartExtractor._extract_business_type(text)
            revenue = SmartExtractor._extract_revenue(text)
            cash_flow = SmartExtractor._extract_cashflow(text)
            vertical = classify_vertical(f"{title} {text}")

            return {
                'title': title,
                'url': url,
                'price': price,
                'price_text': price_text,
                'location': location,
                'city': city,
                'state': state,
                'business_type': business_type,
                'revenue': revenue,
                'cash_flow': cash_flow,
                'vertical': vertical,
                'text': text[:500],
                'full_text': text
            }
        except:
            return None

    @staticmethod
    def _extract_title(element, text: str) -> str:
        for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b']:
            el = element.find(tag)
            if el:
                t = el.get_text(strip=True)
                if 10 < len(t) < 200:
                    return t
        link = element.find('a')
        if link:
            t = link.get_text(strip=True)
            if 10 < len(t) < 200:
                return t
        for s in re.split(r'[.!?]\s+', text):
            if 10 < len(s) < 200:
                return s.strip()
        return text[:100]

    @staticmethod
    def _extract_price_text(text: str) -> Optional[str]:
        matches = re.findall(r'\$[\d,]+(?:\.\d{2})?', text)
        if matches:
            vals = []
            for m in matches:
                try:
                    vals.append((float(m.replace('$', '').replace(',', '')), m))
                except:
                    pass
            if vals:
                return max(vals)[1]
        return None

    @staticmethod
    def _extract_revenue(text: str) -> Optional[float]:
        text_lower = text.lower()
        for pattern in REVENUE_PATTERNS:
            match = re.search(pattern, text_lower, re.I)
            if match:
                value = parse_money_value(match.group(1))
                if value and value > 10000:
                    return value
        return None

    @staticmethod
    def _extract_cashflow(text: str) -> Optional[float]:
        text_lower = text.lower()
        for pattern in CASHFLOW_PATTERNS:
            match = re.search(pattern, text_lower, re.I)
            if match:
                value = parse_money_value(match.group(1))
                if value and value > 1000:
                    return value
        return None

    @staticmethod
    def _extract_location(text: str) -> Optional[str]:
        m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b', text)
        return f"{m.group(1)}, {m.group(2)}" if m else None

    @staticmethod
    def _extract_business_type(text: str) -> Optional[str]:
        types = {
            'restaurant': ['restaurant', 'cafe', 'diner', 'bistro'],
            'bar': ['bar', 'tavern', 'pub', 'lounge'],
            'retail': ['store', 'shop', 'boutique'],
            'service': ['salon', 'spa', 'cleaning'],
            'manufacturing': ['manufacturing', 'fabrication', 'production'],
            'distribution': ['distribution', 'wholesale', 'logistics'],
            'franchise': ['franchise'],
            'ecommerce': ['ecommerce', 'e-commerce', 'online store', 'amazon'],
        }
        t = text.lower()
        for category, kws in types.items():
            if any(kw in t for kw in kws):
                return category
        return None


# ============================================================================
# MAIN SCRAPER CLASS
# ============================================================================

class DealLedgerScraper:
    def __init__(self, output_dir: str = None, vertical: str = None,
                 pattern_cache: str = "data/pattern_cache.json"):
        if output_dir:
            self.output_dir = Path(output_dir)
        else:
            today = date.today().isoformat()
            self.output_dir = Path(f"data/snapshots/{today}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.vertical = vertical
        self.vertical_config = VERTICAL_CONFIGS.get(vertical) if vertical else None

        self.pattern_db = PatternDatabase(pattern_cache)
        self.failure_analyzer = FailureAnalyzer(self.output_dir)

        self.all_listings = []
        self.seen_ids = set()
        self.broker_results = []

        self.stats = {
            'attempted': 0, 'success': 0, 'failed': 0, 'listings': 0,
            'ml_predictions_used': 0, 'new_patterns_learned': 0,
            'with_price': 0, 'with_revenue': 0, 'with_cashflow': 0,
            'specialized_brokers': 0, 'specialized_listings': 0,
            'regular_brokers': 0, 'regular_listings': 0,
            'failures_by_type': defaultdict(int),
            'filtered_out': 0
        }

        self.playwright = None
        self.browser = None
        self.context = None

    def matches_vertical(self, listing: Dict) -> bool:
        if not self.vertical_config:
            return True
        title = (listing.get('title') or '').lower()
        description = (listing.get('description') or listing.get('text') or listing.get('full_text') or '').lower()
        business_type = (listing.get('business_type') or '').lower()
        search_text = f"{title} {description} {business_type}"
        for keyword in self.vertical_config['exclude_keywords']:
            if keyword.lower() in search_text:
                return False
        for keyword in self.vertical_config['include_keywords']:
            if keyword.lower() in search_text:
                return True
        return False

    def normalize_listing(self, listing: Dict, broker: Dict) -> Dict:
        source_url = listing.get('listing_url') or listing.get('url') or ''
        listing_id = hashlib.md5(source_url.encode()).hexdigest()
        return {
            'id': listing_id,
            'title': listing.get('title'),
            'source_url': source_url,
            'broker_id': broker.get('id') or broker.get('account'),
            'broker_name': broker.get('name'),
            'broker_url': broker.get('url'),
            'asking_price': listing.get('price') or listing.get('asking_price'),
            'price_text': listing.get('price_text'),
            'revenue': listing.get('revenue') or listing.get('annual_revenue'),
            'cash_flow': listing.get('cash_flow'),
            'location': listing.get('location'),
            'city': listing.get('city'),
            'state': listing.get('state'),
            'country': 'US',
            'business_type': listing.get('business_type'),
            'vertical': listing.get('vertical') or classify_vertical(
                f"{listing.get('title', '')} {listing.get('text', '')} {listing.get('full_text', '')}"
            ),
            'description': (listing.get('text') or listing.get('description') or '')[:500],
            'first_seen': datetime.now(timezone.utc).isoformat(),
            'status': 'active'
        }

    def classify_business(self, text: str) -> bool:
        s = (text or "").lower()
        if RE_REAL_ESTATE.search(s):
            return False
        return looks_businessy(s) or (PRICE_RE.search(s) is not None)

    # ==================== SCRAPING LOGIC (from V3) ====================

    async def scrape_with_learning(self, page, url: str) -> tuple:
        all_listings = []
        pages_scraped = 0
        max_pages = 100
        current_url = url
        visited_urls = set()
        consecutive_empty = 0

        while pages_scraped < max_pages:
            if current_url in visited_urls:
                break
            visited_urls.add(current_url)

            if pages_scraped > 0:
                print(f"    Page {pages_scraped + 1}: {current_url[:60]}...")
                try:
                    await page.goto(current_url, timeout=20000, wait_until="domcontentloaded")
                except:
                    break

            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except:
                pass
            await asyncio.sleep(5)

            for _ in range(3):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(1.5)

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            domain = urlparse(url).netloc.replace('www.', '')
            cached = self.pattern_db.get_pattern_for_domain(domain)

            pattern_used = None
            len_before_page = len(all_listings)

            if cached and pages_scraped == 0:
                print(f"    Using cached pattern (used {cached['success_count']}x before)")
                patterns = PatternDetector.find_patterns(soup)
                for pattern in patterns:
                    if pattern['signature'] == cached['pattern']:
                        listings = []
                        for el in pattern['elements']:
                            extracted = SmartExtractor.extract(el, page.url)
                            if extracted:
                                listings.append(extracted)
                        if listings:
                            all_listings.extend(listings)
                            pattern_used = cached['pattern']
                            break

            if not pattern_used:
                if pages_scraped == 0:
                    print("    Detecting patterns...")
                patterns = PatternDetector.find_patterns(soup)
                if not patterns:
                    break

                if pages_scraped == 0:
                    print(f"    Found {len(patterns)} patterns")
                    pattern_sigs = [p['signature'] for p in patterns]
                    predicted = self.pattern_db.predict_pattern(url, pattern_sigs)
                    if predicted:
                        self.stats['ml_predictions_used'] += 1
                        for p in patterns:
                            if p['signature'] == predicted:
                                patterns.remove(p)
                                patterns.insert(0, p)
                                break

                for i, pattern in enumerate(patterns[:3], 1):
                    if pages_scraped == 0:
                        print(f"    Pattern {i}: {pattern['count']} elements")
                    listings = []
                    for el in pattern['elements']:
                        extracted = SmartExtractor.extract(el, page.url)
                        if extracted:
                            listings.append(extracted)
                    if listings:
                        if pages_scraped == 0:
                            print(f"      Extracted {len(listings)} listings")
                        all_listings.extend(listings)
                        pattern_used = pattern['signature']
                        break

            if not pattern_used:
                break

            listings_found_this_page = len(all_listings) - len_before_page
            if listings_found_this_page == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    print(f"    Stopping: 3 consecutive empty pages")
                    break
            else:
                consecutive_empty = 0

            pages_scraped += 1
            next_url = await self._find_next_page(page, current_url)
            if not next_url:
                break
            current_url = next_url
            await asyncio.sleep(random.uniform(1, 2))

        if pages_scraped > 1:
            print(f"    Scraped {pages_scraped} pages total")
        return all_listings, pattern_used

    async def _find_next_page(self, page, current_url: str) -> Optional[str]:
        selectors = [
            'a.next', 'a.next-page', '.pagination .next', 'a:has-text("Next")',
            'a:has-text(">")', 'a[rel="next"]', '.pagination a:last-child'
        ]
        for selector in selectors:
            try:
                el = await page.query_selector(selector)
                if el:
                    href = await el.get_attribute('href')
                    if href:
                        return urljoin(current_url, href)
            except:
                continue
        parsed = urlparse(current_url)
        if 'page=' in parsed.query:
            m = re.search(r'page=(\d+)', parsed.query)
            if m:
                cur = int(m.group(1))
                return current_url.replace(f'page={cur}', f'page={cur+1}')
        if '/page/' in parsed.path:
            m = re.search(r'/page/(\d+)', parsed.path)
            if m:
                cur = int(m.group(1))
                return current_url.replace(f'/page/{cur}', f'/page/{cur+1}')
        return None

    # ==================== BROKER SCRAPING ====================

    async def scrape_broker(self, broker: Dict, index: int, total: int):
        self.stats['attempted'] += 1
        start_time = time.time()
        url = broker['url']
        name = broker.get('name', 'Unknown')

        print(f"\n{'='*70}")
        print(f"[{index}/{total}] {name}")
        print(f"  {url}")
        print(f"{'='*70}")

        page = None
        response = None
        try:
            page = await self.context.new_page()
            response = await page.goto(url, timeout=60000, wait_until="domcontentloaded")

            if not response or response.status != 200:
                duration = int(time.time() - start_time)
                status = response.status if response else 'error'
                print(f"  HTTP {status}")
                self.stats['failed'] += 1
                self.stats['failures_by_type'][f'HTTP_{status}'] += 1
                self.failure_analyzer.log_failure(broker, f'HTTP_{status}',
                    f"HTTP {status}", response.status if response else None)
                self.broker_results.append({
                    'broker': name, 'url': url, 'status': 'failed',
                    'error': f'HTTP {status}', 'listings': 0, 'duration': duration
                })
                return

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            # Check for downloadable files
            download_links = soup.find_all('a', href=re.compile(r'\.(xlsx?|csv)', re.I))
            if download_links:
                print(f"    Found {len(download_links)} downloadable file(s)")
                for link_el in download_links[:1]:
                    file_url = urljoin(url, link_el['href'])
                    print(f"    Downloading: {file_url}")
                    listings = await self._download_and_parse_file(page, file_url, broker)
                    if listings:
                        matched = [l for l in listings if self.matches_vertical(l)]
                        self.stats['filtered_out'] += len(listings) - len(matched)
                        duration = int(time.time() - start_time)
                        for listing in matched:
                            normalized = self.normalize_listing(listing, broker)
                            lid = normalized['id']
                            if lid not in self.seen_ids:
                                self.seen_ids.add(lid)
                                self.all_listings.append(normalized)
                                if normalized.get('asking_price'): self.stats['with_price'] += 1
                                if normalized.get('revenue'): self.stats['with_revenue'] += 1
                                if normalized.get('cash_flow'): self.stats['with_cashflow'] += 1
                        count = len(matched)
                        print(f"\n  SUCCESS: {count} listings from file")
                        self.stats['success'] += 1
                        self.stats['listings'] += count
                        self.broker_results.append({
                            'broker': name, 'url': url, 'status': 'success',
                            'listings': count, 'duration': duration, 'method': 'file_download'
                        })
                        return

            # Pattern-based scraping
            listings, pattern_sig = await self.scrape_with_learning(page, url)
            pattern_used = bool(pattern_sig)

            business_count = 0
            for listing in listings:
                text = listing.get('full_text') or listing.get('text') or ''
                if pattern_used:
                    include = not RE_REAL_ESTATE.search(text or '')
                else:
                    include = bool(PRICE_RE.search(text) or looks_businessy(text))
                if not include:
                    continue
                if not self.matches_vertical(listing):
                    self.stats['filtered_out'] += 1
                    continue

                normalized = self.normalize_listing(listing, broker)
                lid = normalized['id']
                if lid in self.seen_ids:
                    continue
                self.seen_ids.add(lid)
                self.all_listings.append(normalized)
                business_count += 1
                if normalized.get('asking_price'): self.stats['with_price'] += 1
                if normalized.get('revenue'): self.stats['with_revenue'] += 1
                if normalized.get('cash_flow'): self.stats['with_cashflow'] += 1

            duration = int(time.time() - start_time)

            if business_count > 0:
                print(f"\n  SUCCESS: {business_count} business listings")
                with_financials = sum(1 for l in self.all_listings[-business_count:]
                                     if l.get('asking_price') or l.get('revenue') or l.get('cash_flow'))
                print(f"  {with_financials}/{business_count} with financial data")
                self.stats['success'] += 1
                self.stats['listings'] += business_count
                self.stats['regular_listings'] += business_count
                if pattern_sig:
                    self.pattern_db.record_success(url, pattern_sig, business_count)
                    self.stats['new_patterns_learned'] += 1
                    print(f"  Pattern learned and cached")
                self.broker_results.append({
                    'broker': name, 'url': url, 'status': 'success',
                    'listings': business_count, 'duration': duration, 'method': 'pattern_detection'
                })
            else:
                print(f"\n  NO BUSINESS LISTINGS FOUND")
                self.stats['failed'] += 1
                self.failure_analyzer.log_failure(broker, 'NO_PATTERN', "No business listings detected")
                self.stats['failures_by_type']['NO_PATTERN'] += 1
                self.broker_results.append({
                    'broker': name, 'url': url, 'status': 'failed',
                    'error': 'No listings found', 'listings': 0, 'duration': duration
                })

        except Exception as e:
            duration = int(time.time() - start_time)
            error_str = str(e)
            http_status = response.status if response else None
            html_content = None
            try:
                if page:
                    html_content = await page.content()
            except:
                pass
            failure_type, detail = self.failure_analyzer.classify_failure(
                error_str, http_status, html_content
            )
            self.failure_analyzer.log_failure(broker, failure_type, detail, http_status)
            self.stats['failures_by_type'][failure_type] += 1
            print(f"\n  {failure_type}: {detail}")
            self.stats['failed'] += 1
            self.broker_results.append({
                'broker': name, 'url': url, 'status': 'failed',
                'error': detail, 'listings': 0, 'duration': duration
            })
        finally:
            if page:
                await page.close()

    async def _download_and_parse_file(self, page, file_url: str, broker: Dict) -> List[Dict]:
        try:
            import io, aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(file_url) as resp:
                    if resp.status != 200:
                        return []
                    content = await resp.read()
            if file_url.lower().endswith('.csv'):
                df = pd.read_csv(io.BytesIO(content))
            else:
                df = pd.read_excel(io.BytesIO(content))
            print(f"      Parsed {len(df)} rows from file")
            listings = []
            title_cols = [c for c in df.columns if any(x in c.lower() for x in ['name', 'title', 'business', 'description'])]
            price_cols = [c for c in df.columns if any(x in c.lower() for x in ['price', 'asking', 'value'])]
            location_cols = [c for c in df.columns if any(x in c.lower() for x in ['location', 'city', 'state', 'area'])]
            for idx, row in df.iterrows():
                try:
                    title = None
                    for col in title_cols:
                        if pd.notna(row[col]):
                            title = str(row[col]); break
                    if not title:
                        title = f"Business Listing {idx + 1}"
                    parts = [f"{col}: {val}" for col, val in row.items() if pd.notna(val)]
                    description = " | ".join(parts)
                    price_text = None
                    for col in price_cols:
                        if pd.notna(row[col]):
                            price_text = str(row[col]); break
                    location = None
                    for col in location_cols:
                        if pd.notna(row[col]):
                            location = str(row[col]); break
                    city, state = extract_city_state(location)
                    if self.classify_business(description):
                        listings.append({
                            'title': title, 'price': parse_money_value(price_text),
                            'price_text': price_text, 'location': location,
                            'city': city, 'state': state,
                            'description': description[:500],
                            'url': f"{file_url}#row{idx}", 'text': description
                        })
                except:
                    continue
            return listings
        except Exception as e:
            print(f"      Error parsing file: {e}")
            return []

    # ==================== BROKER LOADING ====================

    def load_brokers(self, csv_path: str, top_n: Optional[int] = None,
                     test_mode: bool = False) -> List[Dict]:
        print(f"\nLoading brokers from {csv_path}...")
        df = pd.read_csv(csv_path)

        url_col = None
        for col in ['listing_url', 'listings_url', 'active lisitng url', 'url', 'companyurl']:
            if col in df.columns:
                url_col = col; break
        if not url_col:
            raise ValueError(f"No URL column found. Columns: {list(df.columns)}")

        name_col = None
        for col in ['companyname', 'broker_name', 'name']:
            if col in df.columns:
                name_col = col; break

        id_col = None
        for col in ['account', 'id', 'broker_id']:
            if col in df.columns:
                id_col = col; break

        df = df[df[url_col].notna() & (df[url_col] != '')]

        if 'activeListingsCount' in df.columns:
            df = df.sort_values('activeListingsCount', ascending=False)
        elif 'leaderboard_score' in df.columns:
            df = df.sort_values('leaderboard_score', ascending=False)

        if test_mode:
            df = df.head(10)
        elif top_n:
            df = df.head(top_n)

        brokers = []
        for _, row in df.iterrows():
            brokers.append({
                'id': str(row[id_col]) if id_col else str(row.name),
                'account': str(row[id_col]) if id_col else str(row.name),
                'name': row[name_col] if name_col and pd.notna(row[name_col]) else 'Unknown',
                'url': row[url_col],
            })
        print(f"Loaded {len(brokers)} brokers with listing URLs")
        return brokers

    # ==================== SAVE & STATS ====================

    def save_results(self):
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # Listings JSON
        listings_path = self.output_dir / f"listings_{timestamp}.json"
        output = {
            'metadata': {
                'scraper': 'DealLedger V4',
                'run_at': datetime.now(timezone.utc).isoformat(),
                'vertical_filter': self.vertical,
                'brokers_attempted': self.stats['attempted'],
                'brokers_success': self.stats['success'],
                'total_listings': len(self.all_listings)
            },
            'listings': self.all_listings
        }
        with open(listings_path, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\n  Saved {len(self.all_listings)} listings to {listings_path}")

        # Listings CSV
        if self.all_listings:
            csv_path = self.output_dir / f"listings_{timestamp}.csv"
            df = pd.DataFrame(self.all_listings)
            cols = ['id', 'source_url', 'broker_name', 'title', 'asking_price',
                    'revenue', 'cash_flow', 'city', 'state', 'vertical',
                    'business_type', 'first_seen', 'status']
            cols = [c for c in cols if c in df.columns]
            df[cols].to_csv(csv_path, index=False)
            print(f"  Saved CSV to {csv_path}")

        # Broker results
        broker_path = self.output_dir / f"broker_results_{timestamp}.json"
        with open(broker_path, 'w') as f:
            json.dump(self.broker_results, f, indent=2)

        # Failures
        self.failure_analyzer.save()

        # Summary
        summary = {
            'run_at': datetime.now(timezone.utc).isoformat(),
            'vertical_filter': self.vertical,
            'stats': {
                'brokers_attempted': self.stats['attempted'],
                'brokers_success': self.stats['success'],
                'brokers_failed': self.stats['failed'],
                'success_rate': f"{self.stats['success']/max(1,self.stats['attempted'])*100:.1f}%",
                'listings_total': self.stats['listings'],
                'with_price': self.stats['with_price'],
                'with_revenue': self.stats['with_revenue'],
                'with_cashflow': self.stats['with_cashflow'],
                'patterns_learned': self.stats['new_patterns_learned'],
                'ml_predictions_used': self.stats['ml_predictions_used'],
            },
            'failures_by_type': dict(self.stats['failures_by_type'])
        }
        with open(self.output_dir / "summary.json", 'w') as f:
            json.dump(summary, f, indent=2)

    def print_stats(self):
        kb = self.pattern_db.get_stats()
        label = self.vertical_config['name'].upper() if self.vertical_config else "ALL VERTICALS"

        print(f"\n{'='*70}")
        print(f"SCRAPING RESULTS - {label}")
        print(f"{'='*70}")
        print(f"Brokers attempted:    {self.stats['attempted']}")
        print(f"  Success:            {self.stats['success']}")
        print(f"  Failed:             {self.stats['failed']}")
        print(f"  Success rate:       {self.stats['success']/max(1,self.stats['attempted'])*100:.1f}%")

        print(f"\nTotal Listings:       {self.stats['listings']}")
        print(f"  With price:         {self.stats['with_price']}")
        print(f"  With revenue:       {self.stats['with_revenue']}")
        print(f"  With cash flow:     {self.stats['with_cashflow']}")

        print(f"\nLearning System:")
        print(f"  Patterns cached:    {kb['total_patterns']}")
        print(f"  New this run:       {self.stats['new_patterns_learned']}")
        print(f"  ML predictions:     {self.stats['ml_predictions_used']}")

        if self.stats['failures_by_type']:
            print(f"\nFailure Breakdown:")
            for ftype, count in sorted(self.stats['failures_by_type'].items(),
                                       key=lambda x: x[1], reverse=True):
                pct = (count / max(1, self.stats['failed']) * 100)
                print(f"  {ftype:20s}: {count:4d} ({pct:5.1f}%)")

        print(f"\nOutput: {self.output_dir}")
        print(f"{'='*70}")

    # ==================== MAIN RUN ====================

    async def run_async(self, brokers: List[Dict]):
        if not brokers:
            print("No brokers to scrape")
            return

        # Separate specialized vs regular
        specialized_brokers = []
        regular_brokers = []

        if SPECIALIZED_AVAILABLE:
            for broker in brokers:
                name = (broker.get('name') or '').lower()
                url = (broker.get('url') or '').lower()
                is_specialized = any([
                    'murphy' in name or 'murphybusiness.com' in url,
                    'transworld' in name or 'tworld.com' in url,
                    'sunbelt' in name or 'sunbeltnetwork.com' in url,
                    'hedgestone' in name or 'hedgestone.com' in url,
                    'vr business' in name or 'vrbusinessbrokers' in url or 'vrbbusa.com' in url,
                    'first choice' in name or 'fcbb' in name or 'firstchoicebusinessbrokers' in url or 'fcbb.com' in url,
                    'linkbusiness' in url or 'link business' in name,
                    'execbb.com' in url or 'bodner' in name,
                ])
                if is_specialized:
                    specialized_brokers.append(broker)
                else:
                    regular_brokers.append(broker)
        else:
            regular_brokers = brokers

        label = self.vertical_config['name'] if self.vertical_config else "All Verticals"
        print("\n" + "="*70)
        print(f"DEALLEDGER SCRAPER V4 - {label.upper()}")
        print("="*70)
        print(f"Brokers to scrape:  {len(brokers)}")
        print(f"  Specialized:      {len(specialized_brokers)}")
        print(f"  Regular (ML):     {len(regular_brokers)}")
        print(f"Output:             {self.output_dir}")
        if self.vertical:
            print(f"Vertical filter:    {self.vertical}")
        else:
            print(f"Vertical filter:    None (capturing everything)")
        print(f"Specialized deps:   {'Available' if SPECIALIZED_AVAILABLE else 'Not installed (using generic ML for all)'}")
        print("="*70 + "\n")

        # Phase 1: Specialized
        if specialized_brokers:
            print("="*70)
            print("PHASE 1: SPECIALIZED FRANCHISE SCRAPERS")
            print("="*70 + "\n")
            for i, broker in enumerate(specialized_brokers, 1):
                self.stats['attempted'] += 1
                self.stats['specialized_brokers'] += 1
                start_time = time.time()
                try:
                    listings = scrape_specialized_broker(broker, verbose=True)
                    duration = int(time.time() - start_time)
                    if listings:
                        matched = [l for l in listings if self.matches_vertical(l)]
                        self.stats['filtered_out'] += len(listings) - len(matched)
                        if matched:
                            for listing in matched:
                                normalized = self.normalize_listing(listing, broker)
                                lid = normalized['id']
                                if lid not in self.seen_ids:
                                    self.seen_ids.add(lid)
                                    self.all_listings.append(normalized)
                                    if normalized.get('asking_price'): self.stats['with_price'] += 1
                                    if normalized.get('revenue'): self.stats['with_revenue'] += 1
                                    if normalized.get('cash_flow'): self.stats['with_cashflow'] += 1
                            self.stats['success'] += 1
                            self.stats['listings'] += len(matched)
                            self.stats['specialized_listings'] += len(matched)
                        else:
                            self.stats['failed'] += 1
                    else:
                        self.stats['failed'] += 1
                except Exception as e:
                    print(f"\n  ERROR: {str(e)[:100]}")
                    self.stats['failed'] += 1
                if i < len(specialized_brokers):
                    await asyncio.sleep(random.uniform(3, 5))

        # Phase 2: Regular ML
        if regular_brokers:
            print("\n" + "="*70)
            print("PHASE 2: ML-BASED GENERAL SCRAPING")
            print("="*70 + "\n")

            print("Starting browser...")
            self.playwright = await async_playwright().start()

            proxy_host = os.getenv("PROXY_HOST")
            proxy_port = os.getenv("PROXY_PORT")
            proxy_user = os.getenv("PROXY_USER")
            proxy_pass = os.getenv("PROXY_PASS")

            browser_args = {'headless': True, 'args': ['--disable-blink-features=AutomationControlled']}
            context_args = {
                'user_agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                'viewport': {'width': 1920, 'height': 1080}
            }
            if proxy_host and proxy_port:
                proxy_config = {'server': f'http://{proxy_host}:{proxy_port}'}
                if proxy_user and proxy_pass:
                    proxy_config['username'] = proxy_user
                    proxy_config['password'] = proxy_pass
                context_args['proxy'] = proxy_config
                print(f"Using proxy: {proxy_host}:{proxy_port}")

            self.browser = await self.playwright.chromium.launch(**browser_args)
            self.context = await self.browser.new_context(**context_args)
            print("Ready\n")

            total = len(regular_brokers)
            for i, broker in enumerate(regular_brokers, 1):
                self.stats['regular_brokers'] += 1
                await self.scrape_broker(broker, i, total)
                if i < total:
                    await asyncio.sleep(random.uniform(2, 4))

            await self.browser.close()
            await self.playwright.stop()

        self.save_results()
        self.print_stats()

    def run(self, brokers: List[Dict]):
        asyncio.run(self.run_async(brokers))


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="DealLedger Scraper V4 - Full V3 power, no Supabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dealledger_scraper_v4.py --brokers data/brokers.csv --test
  python dealledger_scraper_v4.py --brokers data/brokers.csv --top-n 50
  python dealledger_scraper_v4.py --brokers data/brokers.csv --all
  python dealledger_scraper_v4.py --brokers data/brokers.csv --top-n 100 --vertical cleaning
        """
    )
    parser.add_argument("--brokers", type=str, required=True, help="Path to broker CSV")
    parser.add_argument("--output", type=str, default=None, help="Output directory")
    parser.add_argument("--vertical", type=str, choices=list(VERTICAL_CONFIGS.keys()),
                       default=None, help="Filter by vertical (default: all)")
    parser.add_argument("--pattern-cache", type=str, default="data/pattern_cache.json")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--top-n", type=int, help="Limit to top N brokers")
    group.add_argument("--all", action="store_true", help="Scrape all brokers")
    group.add_argument("--test", action="store_true", help="Test mode: 10 brokers")

    args = parser.parse_args()

    scraper = DealLedgerScraper(
        output_dir=args.output, vertical=args.vertical,
        pattern_cache=args.pattern_cache
    )

    if args.test:
        print("TEST MODE - Scraping 10 brokers\n")
        brokers = scraper.load_brokers(args.brokers, test_mode=True)
    elif args.top_n:
        print(f"Scraping top {args.top_n} brokers\n")
        brokers = scraper.load_brokers(args.brokers, top_n=args.top_n)
    elif args.all:
        print("Scraping ALL brokers\n")
        brokers = scraper.load_brokers(args.brokers)
    else:
        print("Default: Scraping top 20 brokers\n")
        brokers = scraper.load_brokers(args.brokers, top_n=20)

    scraper.run(brokers)


if __name__ == "__main__":
    main()
