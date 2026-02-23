#!/usr/bin/env python3
"""
DealLedger Scraper V5 — Standalone, No Dependencies on Supabase
================================================================
Combines V4's ML pattern detection with standalone simplicity.
Outputs to local files only. Git is the ledger.

Usage:
    python3 dealledger_scraper_v5.py --brokers data/brokers.csv --test          # 5 brokers
    python3 dealledger_scraper_v5.py --brokers data/brokers.csv --top-n 50      # top 50
    python3 dealledger_scraper_v5.py --brokers data/brokers.csv --top-n 200     # scale up
    python3 dealledger_scraper_v5.py --brokers data/brokers.csv --all           # everything

Requirements:
    pip3 install pandas beautifulsoup4 playwright requests --break-system-packages
    playwright install chromium
"""

import argparse
import csv
import hashlib
import json
import os
import random
import re
import sys
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

# Optional: Playwright for JS-rendered sites
try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    print("⚠️  Playwright not installed. JS-rendered sites will be skipped.")
    print("   Install: pip3 install playwright && playwright install chromium")


# ============================================================
# CONFIGURATION
# ============================================================

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

LISTING_KEYWORDS = [
    "for sale", "asking price", "cash flow", "revenue", "ebitda", "sde",
    "seller discretionary", "business for sale", "listing", "gross revenue",
    "net income", "annual revenue", "inventory", "franchise", "established",
    "profitable", "turnkey", "owner operator", "relocatable", "absentee",
]

PRICE_PATTERN = re.compile(r'\$[\d,]+(?:\.\d{2})?|\d{1,3}(?:,\d{3})+')
MONEY_PATTERN = re.compile(r'\$\s*[\d,]+(?:\.\d{2})?')

# Vertical classification keywords
VERTICALS = {
    "cleaning": ["cleaning", "janitorial", "maid", "custodial", "housekeeping", "carpet cleaning", "pressure wash", "window cleaning", "sanitation"],
    "hvac": ["hvac", "heating", "cooling", "air conditioning", "furnace", "refrigeration", "mechanical contractor"],
    "landscaping": ["landscaping", "lawn care", "lawn maintenance", "tree service", "irrigation", "hardscaping", "snow removal"],
    "plumbing": ["plumbing", "plumber", "drain", "sewer", "water heater", "pipe"],
    "electrical": ["electrical", "electrician", "wiring", "lighting"],
    "pest_control": ["pest control", "exterminator", "termite", "pest management"],
    "roofing": ["roofing", "roofer", "roof repair", "shingle"],
    "vending": ["vending", "vending machine", "vending route", "amusement", "atm route"],
    "restaurant": ["restaurant", "cafe", "bar", "grill", "pizzeria", "bakery", "catering", "food truck"],
    "automotive": ["auto repair", "auto body", "car wash", "oil change", "tire", "mechanic", "automotive"],
}


# ============================================================
# PATTERN DETECTION (ML-lite)
# ============================================================

class PatternCache:
    """Learns and caches scraping patterns across broker sites."""
    
    def __init__(self, cache_path="data/pattern_cache.json"):
        self.cache_path = cache_path
        self.patterns = self._load()
    
    def _load(self):
        if os.path.exists(self.cache_path):
            with open(self.cache_path, "r") as f:
                return json.load(f)
        return {}
    
    def save(self):
        os.makedirs(os.path.dirname(self.cache_path) or ".", exist_ok=True)
        with open(self.cache_path, "w") as f:
            json.dump(self.patterns, f, indent=2)
    
    def get(self, domain):
        return self.patterns.get(domain)
    
    def store(self, domain, pattern):
        self.patterns[domain] = pattern
        self.save()
    
    def predict(self, html, url):
        """Try to predict pattern from similar cached sites."""
        if not self.patterns:
            return None
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Try each cached pattern
        best_match = None
        best_score = 0
        
        for domain, pattern in self.patterns.items():
            score = 0
            container_sel = pattern.get("container_selector", "")
            
            if container_sel:
                try:
                    elements = soup.select(container_sel)
                    if elements:
                        score = len(elements)
                except:
                    pass
            
            if score > best_score and score >= 3:  # At least 3 matches
                best_score = score
                best_match = pattern
        
        return best_match


class PatternDetector:
    """Detects listing patterns on a page."""
    
    @staticmethod
    def detect(html, url):
        """Analyze HTML and detect listing container patterns."""
        soup = BeautifulSoup(html, "html.parser")
        candidates = []
        
        # Strategy 1: Find repeated elements with price-like content
        for tag in ["div", "article", "li", "tr", "a", "section"]:
            elements = soup.find_all(tag)
            
            # Group by class
            class_groups = defaultdict(list)
            for el in elements:
                classes = el.get("class", [])
                if classes:
                    key = f"{tag}.{'.'.join(classes)}"
                    class_groups[key].append(el)
            
            for selector, group in class_groups.items():
                if len(group) >= 3:  # Need at least 3 similar elements
                    # Check if they contain listing-like content
                    listing_score = 0
                    for el in group[:10]:
                        text = el.get_text(separator=" ", strip=True).lower()
                        if PRICE_PATTERN.search(text):
                            listing_score += 2
                        if any(kw in text for kw in LISTING_KEYWORDS[:6]):
                            listing_score += 1
                    
                    if listing_score >= 3:
                        css_selector = tag
                        classes = group[0].get("class", [])
                        if classes:
                            css_selector += "." + ".".join(classes)
                        
                        candidates.append({
                            "container_selector": css_selector,
                            "count": len(group),
                            "score": listing_score,
                            "sample_text": group[0].get_text(separator=" ", strip=True)[:200],
                        })
        
        # Strategy 2: Look for common listing page structures
        for selector in [
            "div.listing", "div.listing-item", "div.property-listing",
            "article.listing", "div.business-listing", "div.result",
            "div.search-result", "div.card", "div.listing-card",
            "tr.listing-row", "div.property-card", "li.listing",
            "div.item", "div.post", "div.entry",
        ]:
            try:
                elements = soup.select(selector)
                if len(elements) >= 3:
                    listing_score = 0
                    for el in elements[:10]:
                        text = el.get_text(separator=" ", strip=True).lower()
                        if PRICE_PATTERN.search(text):
                            listing_score += 2
                        if any(kw in text for kw in LISTING_KEYWORDS[:6]):
                            listing_score += 1
                    
                    if listing_score >= 2:
                        candidates.append({
                            "container_selector": selector,
                            "count": len(elements),
                            "score": listing_score + 5,  # Bonus for known patterns
                            "sample_text": elements[0].get_text(separator=" ", strip=True)[:200],
                        })
            except:
                pass
        
        if not candidates:
            return None
        
        # Return highest scoring pattern
        candidates.sort(key=lambda x: x["score"], reverse=True)
        return candidates[0]


# ============================================================
# LISTING EXTRACTOR
# ============================================================

class ListingExtractor:
    """Extract structured listing data from HTML elements."""
    
    @staticmethod
    def extract_price(text):
        """Extract the most likely asking price from text."""
        prices = MONEY_PATTERN.findall(text)
        if not prices:
            return None
        
        # Parse all prices, return the one most likely to be asking price
        parsed = []
        for p in prices:
            try:
                val = float(p.replace("$", "").replace(",", "").strip())
                if 1000 <= val <= 100_000_000:  # Reasonable business price range
                    parsed.append(val)
            except:
                pass
        
        if not parsed:
            return None
        
        # If multiple prices, the largest is usually the asking price
        return max(parsed)
    
    @staticmethod
    def extract_revenue(text):
        """Extract revenue/gross from text."""
        text_lower = text.lower()
        for keyword in ["revenue", "gross", "annual sales", "sales"]:
            idx = text_lower.find(keyword)
            if idx >= 0:
                nearby = text[max(0, idx-10):idx+80]
                prices = MONEY_PATTERN.findall(nearby)
                for p in prices:
                    try:
                        val = float(p.replace("$", "").replace(",", "").strip())
                        if val >= 1000:
                            return val
                    except:
                        pass
        return None
    
    @staticmethod
    def extract_cash_flow(text):
        """Extract cash flow/SDE/EBITDA from text."""
        text_lower = text.lower()
        for keyword in ["cash flow", "sde", "ebitda", "seller discretionary", "net income", "owner benefit"]:
            idx = text_lower.find(keyword)
            if idx >= 0:
                nearby = text[max(0, idx-10):idx+80]
                prices = MONEY_PATTERN.findall(nearby)
                for p in prices:
                    try:
                        val = float(p.replace("$", "").replace(",", "").strip())
                        if val >= 1000:
                            return val
                    except:
                        pass
        return None
    
    @staticmethod
    def extract_location(text):
        """Extract city/state from text."""
        # Common patterns: "City, ST" or "City, State"
        state_pattern = re.compile(
            r'([A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\s*,\s*([A-Z]{2})\b'
        )
        match = state_pattern.search(text)
        if match:
            return {"city": match.group(1), "state": match.group(2)}
        return None
    
    @staticmethod
    def classify_vertical(text):
        """Classify listing into a vertical based on keywords."""
        text_lower = text.lower()
        scores = {}
        for vertical, keywords in VERTICALS.items():
            score = sum(1 for kw in keywords if kw in text_lower)
            if score > 0:
                scores[vertical] = score
        
        if scores:
            return max(scores, key=scores.get)
        return "other"
    
    @staticmethod
    def extract_listing(element, base_url, broker_name):
        """Extract a structured listing from an HTML element."""
        text = element.get_text(separator=" ", strip=True)
        
        if len(text) < 20:
            return None
        
        # Get title
        title_el = element.find(["h1", "h2", "h3", "h4", "h5", "a"])
        title = title_el.get_text(strip=True) if title_el else text[:100]
        
        # Get link
        link_el = element.find("a", href=True)
        detail_url = urljoin(base_url, link_el["href"]) if link_el else None
        
        # Extract fields
        asking_price = ListingExtractor.extract_price(text)
        revenue = ListingExtractor.extract_revenue(text)
        cash_flow = ListingExtractor.extract_cash_flow(text)
        location = ListingExtractor.extract_location(text)
        vertical = ListingExtractor.classify_vertical(text)
        
        # Generate hash for deduplication
        hash_input = f"{title}|{asking_price}|{detail_url or base_url}"
        listing_hash = hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        
        now = datetime.now(timezone.utc).isoformat()
        
        return {
            "hash": listing_hash,
            "title": title[:500],
            "asking_price": asking_price,
            "revenue": revenue,
            "cash_flow": cash_flow,
            "city": location["city"] if location else None,
            "state": location["state"] if location else None,
            "vertical": vertical,
            "source_url": detail_url or base_url,
            "broker": broker_name,
            "broker_url": base_url,
            "first_seen": now,
            "last_seen": now,
            "raw_text": text[:1000],
        }


# ============================================================
# PAGE FETCHER
# ============================================================

class PageFetcher:
    """Fetch pages with requests or Playwright."""
    
    def __init__(self):
        self.session = requests.Session()
        self.playwright = None
        self.browser = None
    
    def fetch_requests(self, url, timeout=15):
        """Fetch with requests (fast, for static sites)."""
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        resp = self.session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp.text
    
    def fetch_playwright(self, url, timeout=20000):
        """Fetch with Playwright (for JS-rendered sites)."""
        if not HAS_PLAYWRIGHT:
            raise RuntimeError("Playwright not installed")
        
        if not self.browser:
            self.playwright = sync_playwright().start()
            self.browser = self.playwright.chromium.launch(headless=True)
        
        page = self.browser.new_page(user_agent=random.choice(USER_AGENTS))
        try:
            page.goto(url, timeout=timeout, wait_until="networkidle")
            page.wait_for_timeout(2000)  # Extra wait for dynamic content
            html = page.content()
            return html
        finally:
            page.close()
    
    def fetch(self, url, use_playwright=False):
        """Fetch a page, trying requests first, then Playwright."""
        try:
            html = self.fetch_requests(url)
            # Check if page has meaningful content
            if len(html) > 5000:
                return html, "requests"
        except Exception:
            pass
        
        if use_playwright or HAS_PLAYWRIGHT:
            try:
                html = self.fetch_playwright(url)
                return html, "playwright"
            except Exception:
                pass
        
        # Final attempt with requests
        html = self.fetch_requests(url, timeout=20)
        return html, "requests"
    
    def close(self):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()


# ============================================================
# PAGINATION HANDLER
# ============================================================

class PaginationHandler:
    """Detect and follow pagination."""
    
    @staticmethod
    def find_next_page(soup, current_url):
        """Find the next page URL."""
        # Common pagination patterns
        next_selectors = [
            "a.next", "a.next-page", "a[rel='next']",
            "li.next a", "a.pagination-next", "a[aria-label='Next']",
            ".pagination a.active + a",  # Next after active
        ]
        
        for selector in next_selectors:
            try:
                el = soup.select_one(selector)
                if el and el.get("href"):
                    return urljoin(current_url, el["href"])
            except:
                pass
        
        # Look for "Next" text in links
        for a in soup.find_all("a", href=True):
            text = a.get_text(strip=True).lower()
            if text in ["next", "next »", "next ›", "next page", "→", ">"]:
                return urljoin(current_url, a["href"])
        
        return None


# ============================================================
# MAIN SCRAPER
# ============================================================

class DealLedgerScraper:
    """Main scraper orchestrator."""
    
    def __init__(self, brokers_csv, output_dir="data/snapshots"):
        self.brokers = self._load_brokers(brokers_csv)
        self.output_dir = output_dir
        self.pattern_cache = PatternCache()
        self.fetcher = PageFetcher()
        self.detector = PatternDetector()
        self.extractor = ListingExtractor()
        
        # Stats
        self.stats = {
            "started": datetime.now(timezone.utc).isoformat(),
            "brokers_attempted": 0,
            "brokers_success": 0,
            "brokers_failed": 0,
            "total_listings": 0,
            "listings_with_price": 0,
            "listings_with_cashflow": 0,
            "patterns_cached": len(self.pattern_cache.patterns),
            "patterns_learned": 0,
            "verticals": Counter(),
        }
        self.all_listings = []
        self.failures = []
    
    def _load_brokers(self, csv_path):
        """Load broker list from CSV."""
        df = pd.read_csv(csv_path)
        
        # Normalize column names
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        
        # Need at minimum: name and url
        required = {"url"}
        # Try to find the URL column
        url_col = None
        for col in df.columns:
            if "url" in col or "website" in col or "link" in col:
                url_col = col
                break
        
        if not url_col:
            # Try first column that looks like URLs
            for col in df.columns:
                sample = df[col].dropna().head(5).tolist()
                if any(str(s).startswith("http") for s in sample):
                    url_col = col
                    break
        
        if not url_col:
            print(f"❌ Cannot find URL column in {csv_path}")
            print(f"   Columns: {list(df.columns)}")
            sys.exit(1)
        
        # Find name column
        name_col = None
        for col in df.columns:
            if "name" in col or "broker" in col or "company" in col:
                name_col = col
                break
        
        brokers = []
        for _, row in df.iterrows():
            url = str(row[url_col]).strip()
            if not url.startswith("http"):
                continue
            
            name = str(row[name_col]).strip() if name_col and pd.notna(row.get(name_col)) else urlparse(url).netloc
            
            brokers.append({
                "name": name,
                "url": url,
                "domain": urlparse(url).netloc,
            })
        
        print(f"📋 Loaded {len(brokers)} brokers from {csv_path}")
        return brokers
    
    def scrape_broker(self, broker):
        """Scrape a single broker site."""
        name = broker["name"]
        url = broker["url"]
        domain = broker["domain"]
        
        print(f"\n{'='*60}")
        print(f"🔍 [{self.stats['brokers_attempted']+1}] {name}")
        print(f"   {url}")
        
        self.stats["brokers_attempted"] += 1
        
        try:
            # Check pattern cache first
            cached = self.pattern_cache.get(domain)
            
            # Fetch the page
            html, method = self.fetcher.fetch(url)
            print(f"   ✅ Fetched ({method}, {len(html):,} bytes)")
            
            # Detect or use cached pattern
            if cached:
                pattern = cached
                print(f"   📦 Using cached pattern: {pattern['container_selector']}")
            else:
                pattern = self.detector.detect(html, url)
                if pattern:
                    # Try predicted pattern from similar sites
                    predicted = self.pattern_cache.predict(html, url)
                    if predicted and predicted.get("score", 0) > pattern.get("score", 0):
                        pattern = predicted
                        print(f"   🧠 ML predicted pattern: {pattern['container_selector']}")
                    else:
                        print(f"   🔎 Detected pattern: {pattern['container_selector']} ({pattern['count']} elements)")
                    
                    self.pattern_cache.store(domain, pattern)
                    self.stats["patterns_learned"] += 1
                else:
                    print(f"   ⚠️  No listing pattern detected")
                    self.failures.append({
                        "broker": name,
                        "url": url,
                        "error": "No listing pattern detected",
                        "html_size": len(html),
                    })
                    self.stats["brokers_failed"] += 1
                    return []
            
            # Extract listings from all pages
            all_page_listings = []
            current_url = url
            page_num = 1
            max_pages = 20
            
            while current_url and page_num <= max_pages:
                if page_num > 1:
                    time.sleep(random.uniform(1, 3))
                    try:
                        html, _ = self.fetcher.fetch(current_url)
                    except Exception:
                        break
                
                soup = BeautifulSoup(html, "html.parser")
                
                # Find listing elements
                try:
                    elements = soup.select(pattern["container_selector"])
                except:
                    elements = []
                
                if not elements:
                    break
                
                page_listings = []
                for el in elements:
                    listing = self.extractor.extract_listing(el, current_url, name)
                    if listing:
                        page_listings.append(listing)
                
                if not page_listings:
                    break
                
                all_page_listings.extend(page_listings)
                
                if page_num > 1:
                    print(f"   📄 Page {page_num}: {len(page_listings)} listings")
                
                # Check for next page
                current_url = PaginationHandler.find_next_page(soup, current_url)
                page_num += 1
            
            # Deduplicate by hash
            seen_hashes = set()
            unique_listings = []
            for listing in all_page_listings:
                if listing["hash"] not in seen_hashes:
                    seen_hashes.add(listing["hash"])
                    unique_listings.append(listing)
            
            if unique_listings:
                print(f"   ✅ {len(unique_listings)} listings extracted" + 
                      (f" ({page_num-1} pages)" if page_num > 2 else ""))
                
                price_count = sum(1 for l in unique_listings if l["asking_price"])
                cf_count = sum(1 for l in unique_listings if l["cash_flow"])
                print(f"   💰 {price_count} with price, {cf_count} with cash flow")
                
                self.stats["brokers_success"] += 1
            else:
                print(f"   ⚠️  Pattern matched but no listings extracted")
                self.failures.append({
                    "broker": name,
                    "url": url,
                    "error": "Pattern matched but extraction failed",
                    "pattern": pattern["container_selector"],
                })
                self.stats["brokers_failed"] += 1
            
            return unique_listings
            
        except Exception as e:
            error_msg = str(e)[:200]
            print(f"   ❌ Error: {error_msg}")
            self.failures.append({
                "broker": name,
                "url": url,
                "error": error_msg,
                "traceback": traceback.format_exc()[-500:],
            })
            self.stats["brokers_failed"] += 1
            return []
    
    def run(self, top_n=None, test=False):
        """Run the full scrape."""
        brokers = self.brokers
        
        if test:
            # In test mode, skip the first 5 franchise HQ brokers
            # and grab 5 that are more likely to be actual broker sites
            brokers = [b for b in brokers if not any(
                skip in b["name"].lower() 
                for skip in ["murphy", "transworld", "sunbelt", "vr business", "first choice", "hedgestone"]
            )][:5]
            if not brokers:
                brokers = self.brokers[:5]
            print(f"\n🧪 TEST MODE: scraping {len(brokers)} brokers")
        elif top_n:
            brokers = brokers[:top_n]
            print(f"\n📊 Scraping top {len(brokers)} brokers")
        else:
            print(f"\n🚀 Scraping ALL {len(brokers)} brokers")
        
        print(f"📦 Pattern cache: {len(self.pattern_cache.patterns)} patterns loaded")
        print()
        
        for broker in brokers:
            listings = self.scrape_broker(broker)
            self.all_listings.extend(listings)
            
            # Update vertical stats
            for listing in listings:
                self.stats["verticals"][listing.get("vertical", "other")] += 1
            
            # Random delay between brokers
            delay = random.uniform(2, 5)
            time.sleep(delay)
        
        # Final stats
        self.stats["total_listings"] = len(self.all_listings)
        self.stats["listings_with_price"] = sum(1 for l in self.all_listings if l["asking_price"])
        self.stats["listings_with_cashflow"] = sum(1 for l in self.all_listings if l["cash_flow"])
        self.stats["patterns_cached"] = len(self.pattern_cache.patterns)
        self.stats["completed"] = datetime.now(timezone.utc).isoformat()
        self.stats["verticals"] = dict(self.stats["verticals"])
        
        # Save results
        self._save_results()
        self._print_summary()
    
    def _save_results(self):
        """Save listings, failures, and stats to snapshot directory."""
        today = datetime.now().strftime("%Y-%m-%d")
        snapshot_dir = os.path.join(self.output_dir, today)
        os.makedirs(snapshot_dir, exist_ok=True)
        
        # Save listings as JSON
        listings_json = os.path.join(snapshot_dir, "listings.json")
        with open(listings_json, "w") as f:
            json.dump(self.all_listings, f, indent=2, default=str)
        
        # Save listings as CSV
        if self.all_listings:
            listings_csv = os.path.join(snapshot_dir, "listings.csv")
            df = pd.DataFrame(self.all_listings)
            df.to_csv(listings_csv, index=False)
        
        # Save failures
        failures_json = os.path.join(snapshot_dir, "failures.json")
        with open(failures_json, "w") as f:
            json.dump(self.failures, f, indent=2, default=str)
        
        # Save summary
        summary_json = os.path.join(snapshot_dir, "summary.json")
        with open(summary_json, "w") as f:
            json.dump(self.stats, f, indent=2, default=str)
        
        print(f"\n📁 Results saved to {snapshot_dir}/")
    
    def _print_summary(self):
        """Print final summary."""
        print(f"\n{'='*60}")
        print(f"DEALLEDGER SCRAPE COMPLETE")
        print(f"{'='*60}")
        print(f"Brokers attempted:   {self.stats['brokers_attempted']}")
        print(f"Brokers success:     {self.stats['brokers_success']}")
        print(f"Brokers failed:      {self.stats['brokers_failed']}")
        success_rate = (self.stats['brokers_success'] / max(self.stats['brokers_attempted'], 1)) * 100
        print(f"Success rate:        {success_rate:.0f}%")
        print(f"")
        print(f"Total listings:      {self.stats['total_listings']}")
        print(f"With price:          {self.stats['listings_with_price']}")
        print(f"With cash flow:      {self.stats['listings_with_cashflow']}")
        print(f"Patterns cached:     {self.stats['patterns_cached']}")
        print(f"Patterns learned:    {self.stats['patterns_learned']}")
        
        if self.stats["verticals"]:
            print(f"\nVertical breakdown:")
            for v, count in sorted(self.stats["verticals"].items(), key=lambda x: -x[1]):
                print(f"  {v:20s} {count}")
        
        print(f"{'='*60}")
    
    def cleanup(self):
        """Clean up resources."""
        self.fetcher.close()


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="DealLedger Scraper V5")
    parser.add_argument("--brokers", required=True, help="Path to brokers CSV")
    parser.add_argument("--test", action="store_true", help="Test mode (5 brokers)")
    parser.add_argument("--top-n", type=int, help="Scrape top N brokers")
    parser.add_argument("--all", action="store_true", help="Scrape all brokers")
    parser.add_argument("--output", default="data/snapshots", help="Output directory")
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("DEALLEDGER SCRAPER V5")
    print("=" * 60)
    
    scraper = DealLedgerScraper(args.brokers, output_dir=args.output)
    
    try:
        if args.test:
            scraper.run(test=True)
        elif args.top_n:
            scraper.run(top_n=args.top_n)
        elif args.all:
            scraper.run()
        else:
            print("Specify --test, --top-n N, or --all")
            sys.exit(1)
    finally:
        scraper.cleanup()


if __name__ == "__main__":
    main()
