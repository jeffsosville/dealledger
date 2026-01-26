"""
DealLedger Standalone Scraper
=============================
Based on UnifiedBrokerScraper V3, stripped of Supabase dependencies.
Reads broker list from CSV, outputs to local JSON/CSV.

Usage:
    python standalone_scraper.py --brokers data/brokers.csv --top-n 50
    python standalone_scraper.py --brokers data/brokers.csv --all --output data/raw/
    python standalone_scraper.py --brokers data/brokers.csv --test
"""

import os
import re
import json
import hashlib
import asyncio
import random
import time
import csv
import argparse
from typing import List, Dict, Optional, Any
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Try to import specialized scrapers
try:
    from scrapers.specialized import (
        scrape_specialized_broker, 
        is_specialized_broker,
        get_specialized_broker_names
    )
    SPECIALIZED_AVAILABLE = True
except ImportError:
    try:
        from specialized import (
            scrape_specialized_broker, 
            is_specialized_broker,
            get_specialized_broker_names
        )
        SPECIALIZED_AVAILABLE = True
    except ImportError:
        SPECIALIZED_AVAILABLE = False
        print("Note: Specialized scrapers not available. Using pattern detection only.")


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
    """Parse $123,456 or 123456 into float"""
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
    """Extract city and state from location string"""
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
# VERTICAL CLASSIFICATION
# ============================================================================

VERTICAL_KEYWORDS = {
    'cleaning': ['cleaning', 'janitorial', 'custodial', 'maid', 'housekeeping', 'carpet cleaning', 
                 'window cleaning', 'pressure washing', 'commercial cleaning', 'residential cleaning'],
    'laundromat': ['laundromat', 'laundry', 'coin laundry', 'wash and fold', 'dry cleaning'],
    'vending': ['vending', 'vending machine', 'atm route', 'amusement', 'arcade', 'coin-op'],
    'hvac': ['hvac', 'heating', 'cooling', 'air conditioning', 'furnace', 'ventilation', 'refrigeration'],
    'landscaping': ['landscape', 'landscaping', 'lawn care', 'lawn maintenance', 'irrigation', 
                   'tree service', 'snow removal', 'lawn mowing'],
    'pool': ['pool service', 'pool cleaning', 'pool maintenance', 'pool route'],
    'pest': ['pest control', 'exterminator', 'termite', 'pest management'],
    'plumbing': ['plumbing', 'plumber', 'drain', 'sewer'],
    'electrical': ['electrical', 'electrician', 'electric service'],
    'automotive': ['auto repair', 'car wash', 'auto body', 'mechanic', 'tire'],
    'restaurant': ['restaurant', 'cafe', 'diner', 'bistro', 'bar', 'tavern', 'food service', 'catering'],
    'retail': ['retail', 'store', 'shop', 'boutique'],
    'ecommerce': ['ecommerce', 'e-commerce', 'online business', 'amazon', 'shopify'],
    'manufacturing': ['manufacturing', 'fabrication', 'production', 'factory'],
    'distribution': ['distribution', 'wholesale', 'distributor', 'logistics'],
    'professional': ['consulting', 'accounting', 'staffing', 'insurance agency'],
    'healthcare': ['medical', 'dental', 'healthcare', 'clinic', 'pharmacy', 'home health'],
}


def classify_vertical(text: str) -> str:
    """Classify listing into a vertical based on text content."""
    text_lower = (text or "").lower()
    
    for vertical, keywords in VERTICAL_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            return vertical
    
    return 'other'


# ============================================================================
# PATTERN DETECTOR
# ============================================================================

class PatternDetector:
    """Detects repeating patterns in HTML"""
    
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
# SMART EXTRACTOR
# ============================================================================

class SmartExtractor:
    """Extract structured data from HTML elements"""
    
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
            revenue = SmartExtractor._extract_revenue(text)
            cash_flow = SmartExtractor._extract_cashflow(text)
            vertical = classify_vertical(f"{title} {text}")

            return {
                'title': title,
                'source_url': url,
                'asking_price': int(price) if price else None,
                'price_text': price_text,
                'location': location,
                'city': city,
                'state': state,
                'country': 'US',
                'revenue': int(revenue) if revenue else None,
                'cash_flow': int(cash_flow) if cash_flow else None,
                'vertical': vertical,
                'description': text[:500],
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


# ============================================================================
# MAIN SCRAPER
# ============================================================================

class StandaloneScraper:
    """
    Standalone scraper for DealLedger.
    No external dependencies - reads CSV, outputs JSON/CSV.
    """

    def __init__(self, output_dir: str = "data/raw"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.all_listings = []
        self.seen_ids = set()
        self.run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        
        self.stats = {
            'brokers_attempted': 0,
            'brokers_success': 0,
            'brokers_failed': 0,
            'listings_total': 0,
            'with_price': 0,
            'with_revenue': 0,
            'with_cashflow': 0,
            'by_vertical': defaultdict(int),
            'failures': []
        }

        self.playwright = None
        self.browser = None
        self.context = None

    def load_brokers(self, csv_path: str, top_n: Optional[int] = None) -> List[Dict]:
        """Load brokers from CSV file."""
        print(f"\nLoading brokers from {csv_path}...")
        
        df = pd.read_csv(csv_path)
        
        # Find the URL column (might be 'listings_url' or 'active lisitng url')
        url_col = None
        for col in ['listings_url', 'active lisitng url', 'url', 'companyurl']:
            if col in df.columns:
                url_col = col
                break
        
        if not url_col:
            raise ValueError(f"No URL column found. Columns: {list(df.columns)}")
        
        # Find name column
        name_col = None
        for col in ['companyname', 'broker_name', 'name']:
            if col in df.columns:
                name_col = col
                break
        
        # Filter to rows with URLs
        df = df[df[url_col].notna() & (df[url_col] != '')]
        
        # Sort by active listings if available
        if 'activeListingsCount' in df.columns:
            df = df.sort_values('activeListingsCount', ascending=False)
        
        # Limit if requested
        if top_n:
            df = df.head(top_n)
        
        brokers = []
        for _, row in df.iterrows():
            brokers.append({
                'id': str(row.get('account', row.name)),
                'name': row.get(name_col, 'Unknown') if name_col else 'Unknown',
                'url': row[url_col],
                'expected_listings': row.get('activeListingsCount', 0)
            })
        
        print(f"Loaded {len(brokers)} brokers with listing URLs")
        return brokers

    async def scrape_with_patterns(self, page, url: str) -> List[Dict]:
        """Scrape listings using pattern detection."""
        all_listings = []
        pages_scraped = 0
        max_pages = 20  # Limit for safety
        current_url = url
        visited_urls = set()

        while pages_scraped < max_pages:
            if current_url in visited_urls:
                break
            visited_urls.add(current_url)

            if pages_scraped > 0:
                print(f"      Page {pages_scraped + 1}...")
                try:
                    await page.goto(current_url, timeout=30000, wait_until="domcontentloaded")
                except:
                    break

            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except:
                pass
            
            await asyncio.sleep(3)

            # Scroll to load lazy content
            for _ in range(3):
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                await asyncio.sleep(1)

            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            # Detect patterns
            patterns = PatternDetector.find_patterns(soup)
            if not patterns:
                break

            if pages_scraped == 0:
                print(f"    Found {len(patterns)} patterns, best has {patterns[0]['count']} elements")

            # Try top patterns
            listings_this_page = []
            for pattern in patterns[:3]:
                for el in pattern['elements']:
                    extracted = SmartExtractor.extract(el, page.url)
                    if extracted:
                        # Filter out real estate
                        if RE_REAL_ESTATE.search(extracted.get('full_text', '')):
                            continue
                        listings_this_page.append(extracted)
                
                if listings_this_page:
                    break

            if not listings_this_page:
                break

            all_listings.extend(listings_this_page)
            pages_scraped += 1

            # Find next page
            next_url = await self._find_next_page(page, current_url)
            if not next_url:
                break
            current_url = next_url
            await asyncio.sleep(random.uniform(1, 2))

        return all_listings

    async def _find_next_page(self, page, current_url: str) -> Optional[str]:
        """Find the next page URL."""
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

        # Try URL patterns
        parsed = urlparse(current_url)
        if 'page=' in parsed.query:
            m = re.search(r'page=(\d+)', parsed.query)
            if m:
                cur = int(m.group(1))
                return current_url.replace(f'page={cur}', f'page={cur+1}')
        
        return None

    async def scrape_broker(self, broker: Dict, index: int, total: int) -> List[Dict]:
        """Scrape a single broker."""
        self.stats['brokers_attempted'] += 1
        
        url = broker['url']
        name = broker['name']
        broker_id = broker['id']
        
        print(f"\n[{index}/{total}] {name}")
        print(f"    URL: {url[:60]}...")

        page = None
        try:
            page = await self.context.new_page()
            response = await page.goto(url, timeout=60000, wait_until="domcontentloaded")
            
            if not response or response.status != 200:
                status = response.status if response else 'error'
                print(f"    ✗ HTTP {status}")
                self.stats['brokers_failed'] += 1
                self.stats['failures'].append({
                    'broker_id': broker_id,
                    'broker_name': name,
                    'url': url,
                    'error': f'HTTP {status}'
                })
                return []

            listings = await self.scrape_with_patterns(page, url)
            
            if listings:
                # Dedupe and add broker info
                unique_listings = []
                for listing in listings:
                    listing_id = hashlib.md5(listing['source_url'].encode()).hexdigest()
                    if listing_id not in self.seen_ids:
                        self.seen_ids.add(listing_id)
                        listing['id'] = f"dl_{listing_id[:12]}"
                        listing['broker_id'] = broker_id
                        listing['broker_name'] = name
                        listing['first_seen'] = datetime.now(timezone.utc).isoformat()
                        listing['last_seen'] = datetime.now(timezone.utc).isoformat()
                        listing['status'] = 'active'
                        unique_listings.append(listing)
                        
                        # Stats
                        if listing.get('asking_price'):
                            self.stats['with_price'] += 1
                        if listing.get('revenue'):
                            self.stats['with_revenue'] += 1
                        if listing.get('cash_flow'):
                            self.stats['with_cashflow'] += 1
                        self.stats['by_vertical'][listing.get('vertical', 'other')] += 1

                print(f"    ✓ {len(unique_listings)} listings")
                self.stats['brokers_success'] += 1
                self.stats['listings_total'] += len(unique_listings)
                return unique_listings
            else:
                print(f"    ✗ No listings found")
                self.stats['brokers_failed'] += 1
                self.stats['failures'].append({
                    'broker_id': broker_id,
                    'broker_name': name,
                    'url': url,
                    'error': 'No listings detected'
                })
                return []

        except Exception as e:
            print(f"    ✗ Error: {str(e)[:50]}")
            self.stats['brokers_failed'] += 1
            self.stats['failures'].append({
                'broker_id': broker_id,
                'broker_name': name,
                'url': url,
                'error': str(e)[:200]
            })
            return []
        
        finally:
            if page:
                await page.close()

    def save_results(self):
        """Save results to JSON and CSV."""
        if not self.all_listings:
            print("\n⚠️  No listings to save")
            return
        
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        # Save JSON
        json_path = self.output_dir / f"{timestamp}.json"
        output = {
            'run_id': self.run_id,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'stats': {
                'brokers_attempted': self.stats['brokers_attempted'],
                'brokers_success': self.stats['brokers_success'],
                'brokers_failed': self.stats['brokers_failed'],
                'listings_total': self.stats['listings_total'],
                'with_price': self.stats['with_price'],
                'with_revenue': self.stats['with_revenue'],
                'with_cashflow': self.stats['with_cashflow'],
                'by_vertical': dict(self.stats['by_vertical'])
            },
            'listings': self.all_listings
        }
        
        with open(json_path, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"\n✓ Saved {len(self.all_listings)} listings to {json_path}")
        
        # Save CSV
        csv_path = self.output_dir / f"{timestamp}.csv"
        if self.all_listings:
            df = pd.DataFrame(self.all_listings)
            # Select key columns for CSV
            cols = ['id', 'source_url', 'broker_id', 'broker_name', 'title', 'asking_price',
                   'revenue', 'cash_flow', 'city', 'state', 'vertical', 'first_seen', 'status']
            cols = [c for c in cols if c in df.columns]
            df[cols].to_csv(csv_path, index=False)
            print(f"✓ Saved CSV to {csv_path}")
        
        # Save failures
        if self.stats['failures']:
            failures_path = self.output_dir / f"{timestamp}_failures.json"
            with open(failures_path, 'w') as f:
                json.dump(self.stats['failures'], f, indent=2)
            print(f"✓ Saved {len(self.stats['failures'])} failures to {failures_path}")

    def print_stats(self):
        """Print run statistics."""
        print(f"\n{'='*60}")
        print("SCRAPE RESULTS")
        print(f"{'='*60}")
        print(f"Brokers attempted:  {self.stats['brokers_attempted']}")
        print(f"  ✓ Success:        {self.stats['brokers_success']}")
        print(f"  ✗ Failed:         {self.stats['brokers_failed']}")
        print(f"\nListings found:     {self.stats['listings_total']}")
        print(f"  With price:       {self.stats['with_price']}")
        print(f"  With revenue:     {self.stats['with_revenue']}")
        print(f"  With cash flow:   {self.stats['with_cashflow']}")
        
        if self.stats['by_vertical']:
            print(f"\nBy vertical:")
            for vertical, count in sorted(self.stats['by_vertical'].items(), 
                                         key=lambda x: x[1], reverse=True):
                print(f"  {vertical:15s}: {count}")
        print(f"{'='*60}")

    async def run_async(self, brokers: List[Dict]):
        """Main async run method."""
        if not brokers:
            print("No brokers to scrape")
            return

        print(f"\n{'='*60}")
        print("DEALLEDGER STANDALONE SCRAPER")
        print(f"{'='*60}")
        print(f"Brokers to scrape: {len(brokers)}")
        print(f"Output directory:  {self.output_dir}")
        if SPECIALIZED_AVAILABLE:
            print(f"Specialized scrapers: {', '.join(get_specialized_broker_names())}")
        print(f"{'='*60}\n")

        # Separate specialized vs regular brokers
        specialized_brokers = []
        regular_brokers = []
        
        if SPECIALIZED_AVAILABLE:
            for broker in brokers:
                if is_specialized_broker(broker):
                    specialized_brokers.append(broker)
                else:
                    regular_brokers.append(broker)
            print(f"Specialized brokers: {len(specialized_brokers)}")
            print(f"Regular brokers: {len(regular_brokers)}\n")
        else:
            regular_brokers = brokers

        # Phase 1: Specialized scrapers (run synchronously, they have their own async handling)
        if specialized_brokers:
            print("="*60)
            print("PHASE 1: SPECIALIZED SCRAPERS")
            print("="*60 + "\n")
            
            for i, broker in enumerate(specialized_brokers, 1):
                self.stats['brokers_attempted'] += 1
                print(f"[{i}/{len(specialized_brokers)}] {broker.get('name', 'Unknown')}")
                
                try:
                    listings = scrape_specialized_broker(broker, verbose=True)
                    
                    if listings:
                        # Dedupe
                        for listing in listings:
                            lid = listing.get('id', '')
                            if lid not in self.seen_ids:
                                self.seen_ids.add(lid)
                                self.all_listings.append(listing)
                                
                                if listing.get('asking_price'):
                                    self.stats['with_price'] += 1
                                if listing.get('revenue'):
                                    self.stats['with_revenue'] += 1
                                if listing.get('cash_flow'):
                                    self.stats['with_cashflow'] += 1
                                self.stats['by_vertical'][listing.get('vertical', 'other')] += 1
                        
                        self.stats['brokers_success'] += 1
                        self.stats['listings_total'] += len(listings)
                    else:
                        self.stats['brokers_failed'] += 1
                        self.stats['failures'].append({
                            'broker_id': broker.get('id'),
                            'broker_name': broker.get('name'),
                            'url': broker.get('url'),
                            'error': 'No listings returned'
                        })
                except Exception as e:
                    self.stats['brokers_failed'] += 1
                    self.stats['failures'].append({
                        'broker_id': broker.get('id'),
                        'broker_name': broker.get('name'),
                        'url': broker.get('url'),
                        'error': str(e)[:200]
                    })
                    print(f"  ✗ Error: {str(e)[:50]}")
                
                time.sleep(2)

        # Phase 2: Regular brokers with pattern detection
        if regular_brokers:
            print("\n" + "="*60)
            print("PHASE 2: PATTERN DETECTION SCRAPERS")
            print("="*60 + "\n")

            # Start browser
            print("Starting browser...")
            self.playwright = await async_playwright().start()
            self.browser = await self.playwright.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            self.context = await self.browser.new_context(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                viewport={'width': 1920, 'height': 1080}
            )
            print("Ready\n")

            # Scrape each broker
            total = len(regular_brokers)
            for i, broker in enumerate(regular_brokers, 1):
                listings = await self.scrape_broker(broker, i, total)
                self.all_listings.extend(listings)
                
                if i < total:
                    await asyncio.sleep(random.uniform(2, 4))

            # Cleanup
            await self.browser.close()
            await self.playwright.stop()

        # Save and report
        self.save_results()
        self.print_stats()

    def run(self, brokers: List[Dict]):
        """Main entry point."""
        asyncio.run(self.run_async(brokers))


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="DealLedger Standalone Scraper")
    parser.add_argument("--brokers", type=str, required=True,
                       help="Path to broker CSV file")
    parser.add_argument("--output", type=str, default="data/raw",
                       help="Output directory (default: data/raw)")
    parser.add_argument("--top-n", type=int,
                       help="Limit to top N brokers")
    parser.add_argument("--test", action="store_true",
                       help="Test mode: scrape 5 brokers")
    parser.add_argument("--all", action="store_true",
                       help="Scrape all brokers")

    args = parser.parse_args()

    scraper = StandaloneScraper(output_dir=args.output)
    
    # Determine how many brokers
    top_n = None
    if args.test:
        top_n = 5
        print("🧪 TEST MODE - Scraping 5 brokers\n")
    elif args.top_n:
        top_n = args.top_n
        print(f"📊 Scraping top {top_n} brokers\n")
    elif args.all:
        print("🚀 Scraping ALL brokers\n")
    else:
        top_n = 10
        print("📊 Default: Scraping top 10 brokers\n")

    brokers = scraper.load_brokers(args.brokers, top_n=top_n)
    scraper.run(brokers)


if __name__ == "__main__":
    main()
