"""
DealLedger Scraper: Transworld Business Advisors

Website: https://www.tworld.com
Coverage: National (US franchise network)
Verticals: General (multi-industry)

Notes:
- Transworld is one of the largest business brokerage franchises
- Listings are aggregated from individual franchise offices
- Site uses standard HTML, no heavy JavaScript rendering
"""

import re
from typing import List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scrapers.base import (
    BaseScraper,
    Listing,
    parse_price,
    parse_integer,
    normalize_state,
    classify_vertical,
)


class TransworldScraper(BaseScraper):
    """Scraper for Transworld Business Advisors."""
    
    broker_id = "transworld"
    broker_name = "Transworld Business Advisors"
    base_url = "https://www.tworld.com"
    
    # Be extra polite - large site with many listings
    request_delay = 1.5
    
    def get_listing_urls(self) -> List[str]:
        """Get all listing URLs from Transworld."""
        urls = []
        page = 1
        max_pages = 100  # Safety limit
        
        while page <= max_pages:
            # Transworld listings index URL pattern
            # Note: This is an example - actual URL structure may differ
            index_url = f"{self.base_url}/business-search/?pg={page}"
            
            soup = self.fetch_page(index_url)
            if not soup:
                break
            
            # Find listing links
            # Adjust selector based on actual site structure
            listing_links = soup.select('a.listing-card-link, a[href*="/buy-a-business/"]')
            
            if not listing_links:
                break  # No more listings
            
            for link in listing_links:
                href = link.get('href')
                if href and '/buy-a-business/' in href:
                    full_url = urljoin(self.base_url, href)
                    urls.append(full_url)
            
            page += 1
        
        return list(set(urls))  # Deduplicate
    
    def parse_listing(self, url: str, soup: BeautifulSoup) -> Optional[Listing]:
        """Parse a Transworld listing page."""
        
        # Title
        title_el = soup.select_one('h1.listing-title, h1.entry-title, h1')
        if not title_el:
            return None
        title = title_el.get_text(strip=True)
        
        # Skip if this looks like an index page, not a listing
        if 'search' in title.lower() or 'results' in title.lower():
            return None
        
        # Description
        desc_el = soup.select_one('div.listing-description, div.business-description, div.entry-content')
        description = desc_el.get_text(strip=True) if desc_el else None
        
        # Price - look for common patterns
        asking_price = None
        price_hidden = False
        
        price_patterns = [
            'span.asking-price',
            'span.price',
            'div.listing-price',
            'td:contains("Asking Price") + td',
        ]
        
        for pattern in price_patterns:
            price_el = soup.select_one(pattern)
            if price_el:
                price_text = price_el.get_text(strip=True)
                asking_price = parse_price(price_text)
                if asking_price is None and price_text:
                    price_hidden = True
                break
        
        # Location
        city = None
        state = None
        
        # Try to find location in various formats
        location_el = soup.select_one('span.location, div.listing-location, span.city-state')
        if location_el:
            location_text = location_el.get_text(strip=True)
            # Parse "City, ST" format
            match = re.match(r'([^,]+),\s*([A-Z]{2})', location_text)
            if match:
                city = match.group(1).strip()
                state = match.group(2)
            else:
                # Just state
                state_match = re.search(r'\b([A-Z]{2})\b', location_text)
                if state_match:
                    state = state_match.group(1)
        
        # Financials - look in details table or specs
        revenue = None
        cash_flow = None
        employees = None
        
        # Common pattern: definition list or table with labels
        for row in soup.select('tr, div.detail-row, dl dt'):
            text = row.get_text(strip=True).lower()
            
            # Get the value (next sibling or paired element)
            value_el = row.find_next_sibling() or row.select_one('td:last-child, dd, span.value')
            if not value_el:
                continue
            value_text = value_el.get_text(strip=True)
            
            if 'gross revenue' in text or 'annual revenue' in text:
                revenue = parse_price(value_text)
            elif 'cash flow' in text or 'sde' in text or 'seller' in text:
                cash_flow = parse_price(value_text)
            elif 'employee' in text:
                employees = parse_integer(value_text)
        
        # Category and vertical classification
        category = None
        category_el = soup.select_one('span.category, a.category-link, div.business-type')
        if category_el:
            category = category_el.get_text(strip=True)
        
        vertical = classify_vertical(title, description or "", category or "")
        
        # Source ID from URL
        source_id = None
        id_match = re.search(r'/(\d+)/?$', url)
        if id_match:
            source_id = id_match.group(1)
        
        # Build listing
        return Listing(
            source_url=url,
            source_id=source_id,
            broker_id=self.broker_id,
            broker_name=self.broker_name,
            title=title,
            description=description,
            asking_price=asking_price,
            price_hidden=price_hidden,
            vertical=vertical,
            category=category,
            city=city,
            state=state,
            revenue=revenue,
            cash_flow=cash_flow,
            employees=employees,
        )


if __name__ == "__main__":
    import json
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    scraper = TransworldScraper()
    results = scraper.run()
    
    print(f"\nScraped {len(results)} listings from Transworld\n")
    
    if results:
        print("Example listing:")
        print(json.dumps(results[0], indent=2, default=str))
