"""
DealLedger Unified Scraper
==========================
Open source scraper for business-for-sale listings.
Combines specialized franchise scrapers with ML-based pattern detection.

https://github.com/dealledger/dealledger
https://dealledger.org

License: MIT
"""

import os
import re
import json
import hashlib
import asyncio
import random
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from supabase import create_client
from playwright.async_api import async_playwright
from dotenv import load_dotenv

load_dotenv()

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_PAGES_PER_BROKER = 100
REQUEST_DELAY_MIN = 2
REQUEST_DELAY_MAX = 4
PAGE_LOAD_TIMEOUT = 60000
SCROLL_PAUSE = 1.5

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

# Filter out real estate listings
RE_REAL_ESTATE = re.compile(r'\bmls\s*#|\bidx\b|\d+\s*bed.*\d+\s*bath', re.I)

# Keywords that indicate business listings
BUSINESS_HINTS = [
    'asking price', 'cash flow', 'revenue', 'business for sale', 'training',
    'turnkey', 'profitable', 'route type', 'financing', 'route details',
    'distribution', 'delivery route', 'route business', 'gross sales',
    'net income', 'ebitda', 'asking', 'price', 'business opportunity',
    'owner', 'operated', 'franchise', 'inventory', 'equipment', 'lease',
    'real estate included', 'seller financing', 'terms available',
    'business type', 'years in business', 'employees', 'customers',
    'clientele', 'ff&e', 'fixtures', 'goodwill', 'for sale by owner',
    'restaurant', 'retail', 'service', 'manufacturing', 'wholesale',
    'distribution', 'franchise opportunity', 'absentee', 'semi-absentee',
    'owner-operator'
]


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def looks_like_business(text: str) -> bool:
    """Check if text contains business listing indicators."""
    t = (text or "").lower()
    return any(k in t for k in BUSINESS_HINTS)


def parse_money(text: str) -> Optional[float]:
    """Parse $123,456 or 123456 or 123k into float."""
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
    """Extract city and state from location string."""
    if not location:
        return None, None
    m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b', location)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    state_match = re.search(r'\b([A-Z]{2})\b', location)
    if state_match:
        return None, state_match.group(1)
    return None, None


# ============================================================================
# PATTERN DATABASE - Stores learned scraping patterns
# ============================================================================

class PatternDatabase:
    """
    Stores and retrieves learned scraping patterns.
    Patterns are saved to Supabase for persistence across runs.
    """
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
        self.patterns = {}
        self.load()

    def load(self):
        """Load existing patterns from database."""
        try:
            response = self.supabase.table('scraper_patterns').select('*').execute()
            for row in response.data:
                self.patterns[row['domain']] = {
                    'pattern': row['pattern_signature'],
                    'success_count': row['success_count'],
                    'total_listings': row['total_listings'],
                    'first_seen': row['first_seen'],
                    'last_used': row['last_used']
                }
            print(f"Loaded {len(self.patterns)} patterns from knowledge base")
        except Exception as e:
            print(f"Note: Could not load patterns: {e}")

    def record_success(self, url: str, pattern_signature: str, listings_count: int):
        """Record a successful scrape pattern."""
        domain = urlparse(url).netloc.replace('www.', '')
        try:
            self.supabase.table('scraper_patterns').upsert({
                'domain': domain,
                'pattern_signature': pattern_signature,
                'success_count': self.patterns.get(domain, {}).get('success_count', 0) + 1,
                'total_listings': self.patterns.get(domain, {}).get('total_listings', 0) + listings_count,
                'last_used': datetime.now().isoformat()
            }, on_conflict='domain').execute()

            self.supabase.table('scraper_history').insert({
                'domain': domain,
                'pattern_signature': pattern_signature,
                'listings_count': listings_count,
                'scraped_at': datetime.now().isoformat()
            }).execute()

            if domain not in self.patterns:
                self.patterns[domain] = {
                    'pattern': pattern_signature,
                    'success_count': 0,
                    'total_listings': 0,
                    'first_seen': datetime.now().isoformat(),
                    'last_used': datetime.now().isoformat()
                }
            self.patterns[domain]['success_count'] += 1
            self.patterns[domain]['total_listings'] += listings_count

        except Exception as e:
            print(f"    Warning: Could not save pattern: {e}")

    def get_pattern_for_domain(self, domain: str) -> Optional[Dict]:
        """Get cached pattern for a domain."""
        return self.patterns.get(domain)

    def predict_pattern(self, url: str, available_patterns: List[str]) -> Optional[str]:
        """Use ML to predict best pattern based on similar domains."""
        domain = urlparse(url).netloc.replace('www.', '')
        
        if domain in self.patterns:
            return self.patterns[domain]['pattern']

        # Find similar domains
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
            print(f"    ML: Using pattern similar to {similar[0][0]}")
            return best
        return None

    def _find_similar_domains(self, target_domain: str) -> List[tuple]:
        """Find domains with similar structure."""
        sims = []
        for domain in self.patterns.keys():
            similarity = self._domain_similarity(target_domain, domain)
            if similarity > 0.3:
                sims.append((domain, similarity))
        sims.sort(key=lambda x: x[1], reverse=True)
        return sims

    def _domain_similarity(self, d1: str, d2: str) -> float:
        """Calculate trigram similarity between domains."""
        def trigrams(s):
            return set(s[i:i+3] for i in range(len(s)-2))
        t1, t2 = trigrams(d1), trigrams(d2)
        if not t1 or not t2:
            return 0.0
        return len(t1 & t2) / len(t1 | t2)

    def get_stats(self) -> Dict:
        """Get knowledge base statistics."""
        return {
            'total_patterns': len(self.patterns),
            'total_scrapes': sum(p['success_count'] for p in self.patterns.values()),
            'total_listings': sum(p['total_listings'] for p in self.patterns.values()),
            'domains_learned': list(self.patterns.keys())
        }


# ============================================================================
# PATTERN DETECTOR - Finds repeating structures in HTML
# ============================================================================

class PatternDetector:
    """Detects repeating listing patterns in HTML."""
    
    @staticmethod
    def find_patterns(soup: BeautifulSoup) -> List[Dict]:
        """Find all repeating patterns that look like listings."""
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
                valid = [
                    el for el in elements 
                    if el.find('a', href=True) and len(el.get_text(strip=True)) > 50
                ]
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
        """Generate a signature for an HTML element."""
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
# SMART EXTRACTOR - Extracts structured data from HTML elements
# ============================================================================

class SmartExtractor:
    """Extract structured listing data from HTML elements."""
    
    @staticmethod
    def extract(element, base_url: str) -> Optional[Dict]:
        """Extract listing data from an HTML element."""
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
            location = SmartExtractor._extract_location(text)
            city, state = extract_city_state(location)
            
            return {
                'title': title,
                'url': url,
                'price': parse_money(price_text),
                'price_text': price_text,
                'location': location,
                'city': city,
                'state': state,
                'business_type': SmartExtractor._extract_business_type(text),
                'revenue': SmartExtractor._extract_revenue(text),
                'cash_flow': SmartExtractor._extract_cashflow(text),
                'text': text[:500],
                'full_text': text
            }
        except:
            return None

    @staticmethod
    def _extract_title(element, text: str) -> str:
        """Extract the listing title."""
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
        """Extract price from text."""
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
        """Extract revenue from text."""
        text_lower = text.lower()
        for pattern in REVENUE_PATTERNS:
            match = re.search(pattern, text_lower, re.I)
            if match:
                value = parse_money(match.group(1))
                if value and value > 10000:
                    return value
        return None

    @staticmethod
    def _extract_cashflow(text: str) -> Optional[float]:
        """Extract cash flow from text."""
        text_lower = text.lower()
        for pattern in CASHFLOW_PATTERNS:
            match = re.search(pattern, text_lower, re.I)
            if match:
                value = parse_money(match.group(1))
                if value and value > 1000:
                    return value
        return None

    @staticmethod
    def _extract_location(text: str) -> Optional[str]:
        """Extract location from text."""
        m = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),\s*([A-Z]{2})\b', text)
        return f"{m.group(1)}, {m.group(2)}" if m else None

    @staticmethod
    def _extract_business_type(text: str) -> Optional[str]:
        """Classify the business type."""
        types = {
            'restaurant': ['restaurant', 'cafe', 'diner', 'bistro', 'pizzeria'],
            'bar': ['bar', 'tavern', 'pub', 'lounge', 'nightclub'],
            'retail': ['store', 'shop', 'boutique', 'retail'],
            'service': ['salon', 'spa', 'cleaning', 'landscaping'],
            'automotive': ['auto', 'car wash', 'mechanic', 'tire'],
            'healthcare': ['medical', 'dental', 'pharmacy', 'clinic'],
            'manufacturing': ['manufacturing', 'fabrication', 'production'],
        }
        t = text.lower()
        for category, keywords in types.items():
            if any(kw in t for kw in keywords):
                return category
        return None


# ============================================================================
# FAILURE ANALYZER - Tracks and classifies scraping failures
# ============================================================================

class FailureAnalyzer:
    """Analyzes and logs broker scraping failures."""
    
    def __init__(self, supabase_client):
        self.supabase = supabase_client
    
    def classify_failure(self, error: str, http_status: Optional[int], 
                        html: Optional[str]) -> tuple:
        """Classify failure type and provide reason."""
        error_lower = (error or "").lower()
        html_lower = (html or "").lower() if html else ""
        
        if http_status == 403:
            return 'HTTP_403', "Site blocking (403) - anti-bot protection"
        elif http_status == 404:
            return 'HTTP_404', "Page not found (404) - URL may be outdated"
        elif http_status and http_status >= 500:
            return 'HTTP_500', f"Server error ({http_status})"
        
        if 'timeout' in error_lower:
            return 'TIMEOUT', "Connection timeout"
        
        if 'ssl' in error_lower or 'certificate' in error_lower:
            return 'SSL_ERROR', "SSL certificate error"
        
        if html and ('recaptcha' in html_lower or 'captcha' in html_lower):
            return 'CAPTCHA', "CAPTCHA protection detected"
        
        if 'no pattern' in error_lower:
            return 'NO_PATTERN', "Could not detect listing pattern"
        
        if html and len(html) < 10000:
            return 'JAVASCRIPT_HEAVY', "Minimal content - JavaScript-heavy"
        
        return 'UNKNOWN', f"Unknown: {error[:200]}"
    
    def log_failure(self, broker: Dict, failure_type: str, error_detail: str, 
                   http_status: Optional[int] = None):
        """Log failure to database."""
        try:
            self.supabase.table('scraper_failures').insert({
                'broker_account': broker.get('account'),
                'broker_name': broker.get('name'),
                'broker_url': broker.get('url'),
                'failure_type': failure_type,
                'error_detail': error_detail[:500],
                'http_status': http_status,
                'failed_at': datetime.now().isoformat()
            }).execute()
        except:
            pass  # Don't break scraping if logging fails


# ============================================================================
# MAIN SCRAPER
# ============================================================================

class DealLedgerScraper:
    """
    Main scraper that combines ML-based pattern detection with 
    specialized scrapers for major franchise brokers.
    """
    
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            raise ValueError("Set SUPABASE_URL and SUPABASE_KEY environment variables")
        
        self.supabase = create_client(url, key)
        self.pattern_db = PatternDatabase(self.supabase)
        self.failure_analyzer = FailureAnalyzer(self.supabase)
        
        self.all_listings = []
        self.seen_ids = set()
        
        self.stats = {
            'attempted': 0,
            'success': 0,
            'failed': 0,
            'listings': 0,
            'with_price': 0,
            'with_revenue': 0,
            'with_cashflow': 0,
            'failures_by_type': defaultdict(int)
        }
        
        self.playwright = None
        self.browser = None
        self.context = None

    async def scrape_page(self, page, url: str) -> tuple:
        """Scrape a single page using pattern detection."""
        all_listings = []
        pages_scraped = 0
        current_url = url
        visited_urls = set()
        consecutive_empty = 0

        while pages_scraped < MAX_PAGES_PER_BROKER:
            if current_url in visited_urls:
                break
            visited_urls.add(current_url)

            if pages_scraped > 0:
                print(f"      Page {pages_scraped + 1}...")
                try:
                    await page.goto(current_url, timeout=20000, wait_until="domcontentloaded")
                except:
                    break

            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except:
                pass
            await asyncio.sleep(5)

            # Scroll to load lazy content
            for _ in range(3):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(SCROLL_PAUSE)

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            domain = urlparse(url).netloc.replace('www.', '')
            cached = self.pattern_db.get_pattern_for_domain(domain)

            pattern_used = None
            len_before = len(all_listings)

            # Try cached pattern first
            if cached and pages_scraped == 0:
                print(f"    Using cached pattern (used {cached['success_count']}x)")
                patterns = PatternDetector.find_patterns(soup)
                for pattern in patterns:
                    if pattern['signature'] == cached['pattern']:
                        for el in pattern['elements']:
                            extracted = SmartExtractor.extract(el, page.url)
                            if extracted:
                                all_listings.append(extracted)
                        if all_listings:
                            pattern_used = cached['pattern']
                        break

            # Detect new patterns
            if not pattern_used:
                patterns = PatternDetector.find_patterns(soup)
                if not patterns:
                    break

                if pages_scraped == 0:
                    print(f"    Found {len(patterns)} patterns, best has {patterns[0]['count']} elements")
                    
                    # Try ML prediction
                    pattern_sigs = [p['signature'] for p in patterns]
                    predicted = self.pattern_db.predict_pattern(url, pattern_sigs)
                    if predicted:
                        for p in patterns:
                            if p['signature'] == predicted:
                                patterns.remove(p)
                                patterns.insert(0, p)
                                break

                for pattern in patterns[:3]:
                    listings = []
                    for el in pattern['elements']:
                        extracted = SmartExtractor.extract(el, page.url)
                        if extracted:
                            listings.append(extracted)
                    if listings:
                        all_listings.extend(listings)
                        pattern_used = pattern['signature']
                        break

            if not pattern_used:
                break

            listings_this_page = len(all_listings) - len_before
            if listings_this_page == 0:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
            else:
                consecutive_empty = 0

            pages_scraped += 1
            next_url = await self._find_next_page(page, current_url)
            if not next_url:
                break
            current_url = next_url
            await asyncio.sleep(random.uniform(1, 2))

        return all_listings, pattern_used

    async def _find_next_page(self, page, current_url: str) -> Optional[str]:
        """Find the next page URL."""
        selectors = [
            'a.next', 'a.next-page', '.pagination .next',
            'a:has-text("Next")', 'a:has-text(">")', 'a[rel="next"]',
            '.pagination a:last-child'
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

        # Try URL patterns
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

    async def scrape_broker(self, broker: Dict, index: int, total: int):
        """Scrape a single broker."""
        self.stats['attempted'] += 1
        
        url = broker['url']
        account = broker['account']
        name = broker['name']

        print(f"\n[{index}/{total}] {name}")
        print(f"    URL: {url[:60]}...")

        page = None
        response = None
        try:
            page = await self.context.new_page()
            response = await page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
            
            if not response or response.status != 200:
                status = response.status if response else 'error'
                print(f"    ✗ HTTP {status}")
                self.stats['failed'] += 1
                self.stats['failures_by_type'][f'HTTP_{status}'] += 1
                return

            listings, pattern_sig = await self.scrape_page(page, url)

            # Filter and dedupe
            business_count = 0
            for listing in listings:
                text = listing.get('full_text') or listing.get('text') or ''
                
                # Skip real estate
                if RE_REAL_ESTATE.search(text):
                    continue
                
                # Generate unique ID
                lid_source = listing.get('url') or (url + '#' + hashlib.md5(text[:256].encode()).hexdigest())
                lid = hashlib.md5(lid_source.encode()).hexdigest()
                
                if lid in self.seen_ids:
                    continue
                self.seen_ids.add(lid)

                self.all_listings.append({
                    'listing_id': lid,
                    'broker_account': account,
                    'title': listing.get('title'),
                    'price': listing.get('price'),
                    'price_text': listing.get('price_text'),
                    'location': listing.get('location'),
                    'city': listing.get('city'),
                    'state': listing.get('state'),
                    'description': listing.get('text'),
                    'listing_url': listing.get('url'),
                    'category': 'business',
                    'business_type': listing.get('business_type'),
                    'revenue': listing.get('revenue'),
                    'cash_flow': listing.get('cash_flow'),
                    'scraped_at': datetime.now().isoformat()
                })
                business_count += 1
                
                if listing.get('price'):
                    self.stats['with_price'] += 1
                if listing.get('revenue'):
                    self.stats['with_revenue'] += 1
                if listing.get('cash_flow'):
                    self.stats['with_cashflow'] += 1

            if business_count > 0:
                print(f"    ✓ {business_count} listings")
                self.stats['success'] += 1
                self.stats['listings'] += business_count
                if pattern_sig:
                    self.pattern_db.record_success(url, pattern_sig, business_count)
            else:
                print(f"    ✗ No listings found")
                self.stats['failed'] += 1
                self.stats['failures_by_type']['NO_PATTERN'] += 1

        except Exception as e:
            error_str = str(e)
            http_status = response.status if response else None
            
            failure_type, detail = self.failure_analyzer.classify_failure(
                error_str, http_status, None
            )
            self.failure_analyzer.log_failure(broker, failure_type, detail, http_status)
            self.stats['failures_by_type'][failure_type] += 1
            
            print(f"    ✗ {failure_type}: {detail[:50]}")
            self.stats['failed'] += 1
            
        finally:
            if page:
                await page.close()

    def load_brokers(self, limit: Optional[int] = None) -> List[Dict]:
        """Load brokers from database."""
        print("\nLoading brokers...")
        
        try:
            all_brokers = []
            page_size = 1000
            offset = 0
            
            while True:
                query = self.supabase.table('broker_master')\
                    .select('account, broker_name, "active lisitng url"')\
                    .order('leaderboard_score', desc=True)\
                    .range(offset, offset + page_size - 1)
                
                response = query.execute()
                
                if not response.data:
                    break
                
                for row in response.data:
                    if row.get('active lisitng url') and row.get('account'):
                        all_brokers.append({
                            'account': str(row['account']),
                            'name': row.get('broker_name') or 'Unknown',
                            'url': row['active lisitng url']
                        })
                
                if len(response.data) < page_size:
                    break
                
                offset += page_size
                
                if limit and len(all_brokers) >= limit:
                    all_brokers = all_brokers[:limit]
                    break
            
            print(f"Loaded {len(all_brokers)} brokers")
            return all_brokers
            
        except Exception as e:
            print(f"Error loading brokers: {e}")
            return []

    def save(self):
        """Save listings to database."""
        if not self.all_listings:
            print("\n⚠️  No listings to save")
            return
            
        print(f"\nSaving {len(self.all_listings)} listings...")
        batch_size = 50
        
        for i in range(0, len(self.all_listings), batch_size):
            batch = self.all_listings[i:i+batch_size]
            try:
                self.supabase.table("listings").upsert(
                    batch, on_conflict="listing_id"
                ).execute()
                print(f"  ✓ Batch {i//batch_size + 1}")
            except Exception as e:
                print(f"  ✗ Batch {i//batch_size + 1}: {e}")

    def export_json(self, filepath: str):
        """Export listings to JSON file."""
        with open(filepath, 'w') as f:
            json.dump(self.all_listings, f, indent=2, default=str)
        print(f"Exported {len(self.all_listings)} listings to {filepath}")

    def export_csv(self, filepath: str):
        """Export listings to CSV file."""
        df = pd.DataFrame(self.all_listings)
        df.to_csv(filepath, index=False)
        print(f"Exported {len(self.all_listings)} listings to {filepath}")

    def print_stats(self):
        """Print scraping statistics."""
        kb = self.pattern_db.get_stats()
        
        print(f"\n{'='*60}")
        print("SCRAPE RESULTS")
        print(f"{'='*60}")
        print(f"Brokers attempted:  {self.stats['attempted']}")
        print(f"  ✓ Success:        {self.stats['success']}")
        print(f"  ✗ Failed:         {self.stats['failed']}")
        print(f"\nListings found:     {self.stats['listings']}")
        print(f"  With price:       {self.stats['with_price']}")
        print(f"  With revenue:     {self.stats['with_revenue']}")
        print(f"  With cash flow:   {self.stats['with_cashflow']}")
        
        if self.stats['failures_by_type']:
            print(f"\nFailures by type:")
            for ftype, count in sorted(
                self.stats['failures_by_type'].items(),
                key=lambda x: x[1], reverse=True
            ):
                print(f"  {ftype:20s}: {count}")
        
        print(f"\nKnowledge base: {kb['total_patterns']} patterns learned")
        print(f"{'='*60}")

    async def run(self, limit: Optional[int] = None, export_path: Optional[str] = None):
        """Main run method."""
        brokers = self.load_brokers(limit=limit)
        
        if not brokers:
            print("No brokers to scrape")
            return
        
        print(f"\n{'='*60}")
        print("DEALLEDGER SCRAPER")
        print(f"{'='*60}")
        print(f"Brokers: {len(brokers)}")
        print(f"{'='*60}")
        
        # Start browser
        print("\nStarting browser...")
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--disable-blink-features=AutomationControlled']
        )
        self.context = await self.browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1920, 'height': 1080}
        )
        print("Ready")
        
        # Scrape
        total = len(brokers)
        for i, broker in enumerate(brokers, 1):
            await self.scrape_broker(broker, i, total)
            if i < total:
                await asyncio.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
        
        # Cleanup
        await self.browser.close()
        await self.playwright.stop()
        
        # Save
        self.save()
        
        # Export
        if export_path:
            date_str = datetime.now().strftime('%Y-%m-%d')
            self.export_json(f"{export_path}/{date_str}.json")
            self.export_csv(f"{export_path}/{date_str}.csv")
        
        self.print_stats()


# ============================================================================
# CLI
# ============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="DealLedger Scraper - Open source business listing aggregator"
    )
    parser.add_argument(
        "--limit", "-n", type=int,
        help="Limit number of brokers to scrape"
    )
    parser.add_argument(
        "--export", "-e", type=str,
        help="Export path for JSON/CSV output"
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Scrape all brokers (no limit)"
    )
    
    args = parser.parse_args()
    
    scraper = DealLedgerScraper()
    limit = None if args.all else (args.limit or 50)
    
    asyncio.run(scraper.run(limit=limit, export_path=args.export))


if __name__ == "__main__":
    main()
