"""
DealLedger Base Scraper

All broker scrapers inherit from this class.
"""

import hashlib
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass
class Listing:
    """Represents a single business-for-sale listing."""
    
    # Required fields
    source_url: str
    broker_id: str
    broker_name: str
    title: str
    
    # Optional identifiers
    source_id: Optional[str] = None
    
    # Pricing
    asking_price: Optional[int] = None
    price_hidden: bool = False
    
    # Classification
    vertical: str = "other"
    category: Optional[str] = None
    
    # Location
    city: Optional[str] = None
    state: Optional[str] = None
    country: str = "US"
    zip: Optional[str] = None
    region: Optional[str] = None
    location_hidden: bool = False
    
    # Business details
    description: Optional[str] = None
    revenue: Optional[int] = None
    cash_flow: Optional[int] = None
    ebitda: Optional[int] = None
    inventory: Optional[int] = None
    ffe: Optional[int] = None
    real_estate: Optional[bool] = None
    real_estate_value: Optional[int] = None
    year_established: Optional[int] = None
    employees: Optional[int] = None
    
    # Deal terms
    seller_financing: Optional[bool] = None
    sba_prequalified: Optional[bool] = None
    franchise: Optional[bool] = None
    franchise_name: Optional[str] = None
    home_based: Optional[bool] = None
    relocatable: Optional[bool] = None
    absentee_owner: Optional[bool] = None
    
    # Metadata (auto-populated)
    scraped_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    content_hash: Optional[str] = None
    raw_data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Compute content hash after initialization."""
        self.content_hash = self._compute_hash()
    
    def _compute_hash(self) -> str:
        """Compute SHA-256 hash of normalized content."""
        content = (
            f"{self.title or ''}"
            f"{self.asking_price or ''}"
            f"{self.revenue or ''}"
            f"{self.cash_flow or ''}"
            f"{self.city or ''}"
            f"{self.state or ''}"
            f"{(self.description or '')[:500]}"
        )
        return hashlib.sha256(content.encode()).hexdigest()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime to ISO format
        if self.scraped_at:
            data['scraped_at'] = self.scraped_at.isoformat()
        return data


class BaseScraper(ABC):
    """
    Base class for all broker scrapers.
    
    Subclasses must implement:
        - broker_id: str
        - broker_name: str
        - base_url: str
        - get_listing_urls() -> List[str]
        - parse_listing(url, soup) -> Listing
    """
    
    # Subclasses must override these
    broker_id: str = None
    broker_name: str = None
    base_url: str = None
    
    # Default settings (can be overridden)
    request_delay: float = 1.0  # Seconds between requests
    timeout: int = 30
    user_agent: str = "DealLedger/1.0 (https://dealledger.org)"
    
    def __init__(self):
        if not all([self.broker_id, self.broker_name, self.base_url]):
            raise ValueError("Subclass must define broker_id, broker_name, and base_url")
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
    
    @abstractmethod
    def get_listing_urls(self) -> List[str]:
        """
        Return a list of all listing URLs to scrape.
        
        This typically involves:
        1. Fetching the listings index page(s)
        2. Paginating through results
        3. Extracting individual listing URLs
        """
        pass
    
    @abstractmethod
    def parse_listing(self, url: str, soup: BeautifulSoup) -> Optional[Listing]:
        """
        Parse a single listing page and return a Listing object.
        
        Args:
            url: The listing URL
            soup: BeautifulSoup object of the page
            
        Returns:
            Listing object, or None if parsing failed
        """
        pass
    
    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a page and return BeautifulSoup object."""
        try:
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None
    
    def scrape_all(self) -> List[Listing]:
        """
        Scrape all listings from this broker.
        
        Returns:
            List of Listing objects
        """
        import time
        
        listings = []
        urls = self.get_listing_urls()
        
        logger.info(f"[{self.broker_id}] Found {len(urls)} listing URLs")
        
        for i, url in enumerate(urls):
            logger.debug(f"[{self.broker_id}] Scraping {i+1}/{len(urls)}: {url}")
            
            soup = self.fetch_page(url)
            if soup is None:
                continue
            
            try:
                listing = self.parse_listing(url, soup)
                if listing:
                    listings.append(listing)
            except Exception as e:
                logger.error(f"[{self.broker_id}] Failed to parse {url}: {e}")
            
            # Polite delay between requests
            time.sleep(self.request_delay)
        
        logger.info(f"[{self.broker_id}] Successfully scraped {len(listings)} listings")
        return listings
    
    def run(self) -> List[Dict[str, Any]]:
        """
        Main entry point. Scrape all listings and return as dicts.
        """
        listings = self.scrape_all()
        return [listing.to_dict() for listing in listings]


# --- Utility functions for parsing ---

def parse_price(text: str) -> Optional[int]:
    """
    Parse a price string into an integer.
    
    Examples:
        "$450,000" -> 450000
        "450000" -> 450000
        "$1.2M" -> 1200000
        "Contact for Price" -> None
    """
    if not text:
        return None
    
    text = text.strip().upper()
    
    # Check for hidden price indicators
    hidden_indicators = ['CONTACT', 'CALL', 'TBD', 'N/A', 'NEGOTIABLE', 'UPON REQUEST']
    if any(ind in text for ind in hidden_indicators):
        return None
    
    # Remove currency symbols and commas
    import re
    text = re.sub(r'[,$]', '', text)
    
    # Handle M/K suffixes
    multiplier = 1
    if 'M' in text:
        multiplier = 1_000_000
        text = text.replace('M', '')
    elif 'K' in text:
        multiplier = 1_000
        text = text.replace('K', '')
    
    # Extract number
    match = re.search(r'[\d.]+', text)
    if match:
        try:
            return int(float(match.group()) * multiplier)
        except ValueError:
            return None
    
    return None


def parse_integer(text: str) -> Optional[int]:
    """Parse a string into an integer, handling commas."""
    if not text:
        return None
    
    import re
    text = re.sub(r'[,$]', '', str(text).strip())
    match = re.search(r'\d+', text)
    if match:
        try:
            return int(match.group())
        except ValueError:
            return None
    return None


def normalize_state(state: str) -> Optional[str]:
    """Normalize state name to two-letter code."""
    if not state:
        return None
    
    state = state.strip().upper()
    
    # Already a code
    if len(state) == 2:
        return state
    
    # Common state names
    STATE_MAP = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY', 'DISTRICT OF COLUMBIA': 'DC',
    }
    
    return STATE_MAP.get(state)


def classify_vertical(title: str, description: str = "", category: str = "") -> str:
    """
    Classify a listing into a vertical based on text content.
    
    Returns vertical code (e.g., 'cleaning', 'laundromat', 'hvac')
    """
    text = f"{title} {description} {category}".lower()
    
    VERTICAL_KEYWORDS = {
        'cleaning': ['cleaning', 'janitorial', 'maid', 'housekeeping', 'sanitation'],
        'laundromat': ['laundromat', 'laundry', 'coin laundry', 'wash and fold'],
        'vending': ['vending', 'atm route', 'vending machine', 'amusement route'],
        'hvac': ['hvac', 'heating', 'air conditioning', 'mechanical', 'refrigeration'],
        'landscaping': ['landscaping', 'lawn care', 'lawn service', 'tree service', 'irrigation'],
        'pool': ['pool service', 'pool cleaning', 'pool maintenance', 'pool route'],
        'pest': ['pest control', 'exterminator', 'termite', 'pest management'],
        'plumbing': ['plumbing', 'plumber', 'drain', 'sewer'],
        'electrical': ['electrical', 'electrician', 'electric service'],
        'automotive': ['auto repair', 'car wash', 'auto body', 'mechanic', 'tire'],
        'restaurant': ['restaurant', 'cafe', 'bar', 'food service', 'catering', 'pizza'],
        'retail': ['retail', 'store', 'shop', 'boutique'],
        'ecommerce': ['ecommerce', 'e-commerce', 'online business', 'amazon', 'shopify'],
        'manufacturing': ['manufacturing', 'fabrication', 'production', 'factory'],
        'distribution': ['distribution', 'wholesale', 'distributor', 'logistics'],
        'professional': ['consulting', 'accounting', 'staffing', 'insurance agency'],
        'healthcare': ['medical', 'dental', 'healthcare', 'clinic', 'pharmacy', 'home health'],
    }
    
    for vertical, keywords in VERTICAL_KEYWORDS.items():
        if any(kw in text for kw in keywords):
            return vertical
    
    return 'other'
