"""
DealLedger Scraper Template

Copy this file to create a new broker scraper.

Steps:
1. Copy this file to scrapers/brokers/{broker_name}.py
2. Update broker_id, broker_name, base_url
3. Implement get_listing_urls()
4. Implement parse_listing()
5. Test with: python -m scrapers.brokers.{broker_name}
6. Add tests in tests/test_{broker_name}.py
"""

from typing import List, Optional
from bs4 import BeautifulSoup

from scrapers.base import (
    BaseScraper,
    Listing,
    parse_price,
    parse_integer,
    normalize_state,
    classify_vertical,
)


class TemplateScraper(BaseScraper):
    """
    Scraper for [BROKER NAME].
    
    Website: [BROKER URL]
    Coverage: [STATES/REGIONS]
    Verticals: [VERTICALS THEY SPECIALIZE IN]
    """
    
    # === REQUIRED: Update these ===
    broker_id = "template"  # Short, lowercase, no spaces (e.g., "transworld", "sunbelt")
    broker_name = "Template Broker"  # Full company name
    base_url = "https://example.com"  # Main website URL
    
    # === OPTIONAL: Override defaults ===
    # request_delay = 1.0  # Seconds between requests (be polite)
    # timeout = 30  # Request timeout in seconds
    
    def get_listing_urls(self) -> List[str]:
        """
        Return all listing URLs from this broker.
        
        Typical approach:
        1. Fetch the listings index page
        2. Find pagination (if any)
        3. Extract listing URLs from each page
        4. Return deduplicated list
        """
        urls = []
        
        # Example: Single listings page
        # index_url = f"{self.base_url}/listings"
        # soup = self.fetch_page(index_url)
        # if soup:
        #     for link in soup.select('a.listing-link'):
        #         href = link.get('href')
        #         if href:
        #             urls.append(urljoin(self.base_url, href))
        
        # Example: Paginated listings
        # page = 1
        # while True:
        #     index_url = f"{self.base_url}/listings?page={page}"
        #     soup = self.fetch_page(index_url)
        #     if not soup:
        #         break
        #     
        #     links = soup.select('a.listing-link')
        #     if not links:
        #         break
        #     
        #     for link in links:
        #         href = link.get('href')
        #         if href:
        #             urls.append(urljoin(self.base_url, href))
        #     
        #     page += 1
        
        # TODO: Implement for this broker
        raise NotImplementedError("Implement get_listing_urls()")
        
        return list(set(urls))  # Deduplicate
    
    def parse_listing(self, url: str, soup: BeautifulSoup) -> Optional[Listing]:
        """
        Parse a single listing page.
        
        Args:
            url: The listing URL
            soup: BeautifulSoup object of the page
            
        Returns:
            Listing object with all available fields populated
        """
        
        # === Extract required fields ===
        
        # Title (required)
        title_el = soup.select_one('h1.listing-title')  # Adjust selector
        if not title_el:
            return None  # Can't proceed without title
        title = title_el.get_text(strip=True)
        
        # === Extract optional fields ===
        
        # Description
        desc_el = soup.select_one('div.listing-description')
        description = desc_el.get_text(strip=True) if desc_el else None
        
        # Price
        price_el = soup.select_one('span.price')
        asking_price = parse_price(price_el.get_text()) if price_el else None
        price_hidden = asking_price is None and price_el is not None
        
        # Location
        city_el = soup.select_one('span.city')
        city = city_el.get_text(strip=True) if city_el else None
        
        state_el = soup.select_one('span.state')
        state = normalize_state(state_el.get_text()) if state_el else None
        
        # Financials
        revenue_el = soup.select_one('span.revenue')
        revenue = parse_price(revenue_el.get_text()) if revenue_el else None
        
        cash_flow_el = soup.select_one('span.cash-flow')
        cash_flow = parse_price(cash_flow_el.get_text()) if cash_flow_el else None
        
        # Classification
        category_el = soup.select_one('span.category')
        category = category_el.get_text(strip=True) if category_el else None
        vertical = classify_vertical(title, description or "", category or "")
        
        # Source ID (broker's internal ID, if visible in URL or page)
        # source_id = url.split('/')[-1]  # Example: extract from URL
        source_id = None
        
        # === Build and return Listing ===
        
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
            # Add more fields as available...
        )


# === Entry point for direct execution ===

if __name__ == "__main__":
    import json
    import logging
    
    logging.basicConfig(level=logging.INFO)
    
    scraper = TemplateScraper()
    results = scraper.run()
    
    print(f"\nScraped {len(results)} listings\n")
    
    # Print first result as example
    if results:
        print("Example listing:")
        print(json.dumps(results[0], indent=2, default=str))
