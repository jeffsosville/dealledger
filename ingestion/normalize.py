"""
DealLedger: Normalize Scraper Output

Takes raw scraper output and normalizes it to the canonical schema.
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pathlib import Path


def generate_listing_id(source_url: str, broker_id: str) -> str:
    """Generate a stable, unique listing ID."""
    content = f"{broker_id}:{source_url}"
    hash_val = hashlib.sha256(content.encode()).hexdigest()[:12]
    return f"dl_{hash_val}"


def normalize_price(value: Any) -> Optional[int]:
    """Normalize price to integer USD."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        # Remove currency symbols, commas
        cleaned = re.sub(r'[,$]', '', value.strip())
        try:
            return int(float(cleaned))
        except ValueError:
            return None
    return None


def normalize_state(state: Any) -> Optional[str]:
    """Normalize state to 2-letter code."""
    if not state:
        return None
    
    state = str(state).strip().upper()
    
    if len(state) == 2:
        return state
    
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


def normalize_boolean(value: Any) -> Optional[bool]:
    """Normalize to boolean."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ('true', 'yes', '1', 'y')
    return bool(value)


def compute_content_hash(listing: Dict[str, Any]) -> str:
    """Compute SHA-256 hash of key listing content."""
    content = (
        f"{listing.get('title', '')}"
        f"{listing.get('asking_price', '')}"
        f"{listing.get('revenue', '')}"
        f"{listing.get('cash_flow', '')}"
        f"{listing.get('city', '')}"
        f"{listing.get('state', '')}"
        f"{str(listing.get('description', ''))[:500]}"
    )
    return hashlib.sha256(content.encode()).hexdigest()


def normalize_listing(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a single listing to canonical schema.
    
    Args:
        raw: Raw listing dict from scraper
        
    Returns:
        Normalized listing dict
    """
    # Generate ID if not present
    listing_id = raw.get('id') or generate_listing_id(
        raw.get('source_url', ''),
        raw.get('broker_id', '')
    )
    
    normalized = {
        # Identifiers
        'id': listing_id,
        'source_url': raw.get('source_url'),
        'source_id': raw.get('source_id'),
        'broker_id': raw.get('broker_id'),
        'broker_name': raw.get('broker_name'),
        
        # Timestamps
        'first_seen': raw.get('first_seen') or datetime.now(timezone.utc).isoformat(),
        'last_seen': raw.get('last_seen') or datetime.now(timezone.utc).isoformat(),
        'scraped_at': raw.get('scraped_at') or datetime.now(timezone.utc).isoformat(),
        'status': raw.get('status', 'active'),
        
        # Business details
        'title': raw.get('title'),
        'description': raw.get('description'),
        'asking_price': normalize_price(raw.get('asking_price')),
        'price_hidden': normalize_boolean(raw.get('price_hidden')) or False,
        'vertical': raw.get('vertical', 'other'),
        'category': raw.get('category'),
        
        # Location
        'city': raw.get('city'),
        'state': normalize_state(raw.get('state')),
        'country': raw.get('country', 'US'),
        'zip': raw.get('zip'),
        'region': raw.get('region'),
        'location_hidden': normalize_boolean(raw.get('location_hidden')) or False,
        
        # Financials
        'revenue': normalize_price(raw.get('revenue')),
        'cash_flow': normalize_price(raw.get('cash_flow')),
        'ebitda': normalize_price(raw.get('ebitda')),
        'inventory': normalize_price(raw.get('inventory')),
        'ffe': normalize_price(raw.get('ffe')),
        'real_estate': normalize_boolean(raw.get('real_estate')),
        'real_estate_value': normalize_price(raw.get('real_estate_value')),
        'year_established': raw.get('year_established'),
        'employees': raw.get('employees'),
        
        # Deal terms
        'seller_financing': normalize_boolean(raw.get('seller_financing')),
        'sba_prequalified': normalize_boolean(raw.get('sba_prequalified')),
        'franchise': normalize_boolean(raw.get('franchise')),
        'franchise_name': raw.get('franchise_name'),
        'home_based': normalize_boolean(raw.get('home_based')),
        'relocatable': normalize_boolean(raw.get('relocatable')),
        'absentee_owner': normalize_boolean(raw.get('absentee_owner')),
        
        # Metadata
        'confidence': raw.get('confidence', 0.5),
        'flags': raw.get('flags', []),
    }
    
    # Compute content hash
    normalized['content_hash'] = compute_content_hash(normalized)
    
    return normalized


def normalize_scraper_output(scraper_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Normalize all listings from a scraper run.
    
    Args:
        scraper_results: Full output from run_all.py
        
    Returns:
        List of normalized listings
    """
    all_listings = []
    
    for broker in scraper_results.get('brokers', []):
        if broker.get('status') != 'success':
            continue
        
        for raw_listing in broker.get('listings', []):
            try:
                normalized = normalize_listing(raw_listing)
                all_listings.append(normalized)
            except Exception as e:
                print(f"Warning: Failed to normalize listing: {e}")
                continue
    
    return all_listings


def main():
    """CLI for normalizing scraper output."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Normalize scraper output")
    parser.add_argument("input", help="Input JSON file (scraper output)")
    parser.add_argument("-o", "--output", help="Output JSON file")
    
    args = parser.parse_args()
    
    with open(args.input) as f:
        scraper_results = json.load(f)
    
    normalized = normalize_scraper_output(scraper_results)
    
    output = {
        "normalized_at": datetime.now(timezone.utc).isoformat(),
        "source_run_id": scraper_results.get('run_id'),
        "listings_count": len(normalized),
        "listings": normalized,
    }
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(output, f, indent=2)
        print(f"Wrote {len(normalized)} normalized listings to {args.output}")
    else:
        print(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
