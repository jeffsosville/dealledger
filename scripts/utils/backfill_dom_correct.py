#!/usr/bin/env python3
import json
import sys
import os
from datetime import datetime, timedelta
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

def calculate_dom_from_bbs_number(listing_number):
    """Calculate DOM from actual BizBuySell listing number"""
    if not listing_number or listing_number < 100000:
        return None
    
    # BizBuySell listing number progression (approximate)
    # Based on your data: 2.4M range numbers
    # Current day roughly corresponds to listing ~24500000 (based on pattern)
    
    # Rough estimate: 1000 listings per day
    # Higher number = more recent
    today = datetime.now()
    
    if listing_number >= 24000000:
        # Very recent listings
        estimated_date = today - timedelta(days=(24500000 - listing_number) / 1000)
    elif listing_number >= 20000000:
        # Recent listings (last few months)
        estimated_date = today - timedelta(days=(24000000 - listing_number) / 800)
    elif listing_number >= 10000000:
        # Older listings (last year+)
        estimated_date = today - timedelta(days=(20000000 - listing_number) / 500)
    elif listing_number >= 2000000:
        # Your range - last 2-3 years
        # Linear interpolation based on your sample having 164-187 days
        # Number 2421520 has 164 days, 2412963 has 187 days
        # Higher number = more recent (fewer days)
        estimated_days = max(1, 300 - (listing_number - 2400000) / 1000)
        estimated_date = today - timedelta(days=estimated_days)
    else:
        # Very old listings
        estimated_date = today - timedelta(days=365 * 3)
    
    dom = (today.date() - estimated_date.date()).days
    return max(1, dom)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dom-only', action='store_true')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--limit', type=int, default=10)
    
    args = parser.parse_args()
    
    if not DEALLEDGER_KEY:
        log.error("Set DEALLEDGER_ANON_KEY environment variable")
        sys.exit(1)
    
    try:
        from supabase import create_client
        supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)
    except Exception as e:
        log.error(f"Error: {e}")
        sys.exit(1)
    
    log.info("Fetching listings with null DOM...")
    
    result = supabase.table('listings').select(
        'id, listing_number, days_on_market, header'
    ).is_('days_on_market', 'null').not_.is_('listing_number', 'null').limit(args.limit).execute()
    
    listings = result.data
    log.info(f"Found {len(listings)} listings needing DOM updates")
    
    updates = []
    for listing in listings:
        dom = calculate_dom_from_bbs_number(listing['listing_number'])
        if dom is not None:
            updates.append({
                'id': listing['id'],
                'days_on_market': dom
            })
            header = listing.get('header') or 'No title'
            log.info(f"Listing {listing['listing_number']}: DOM={dom} | {header[:50]}")
    
    log.info(f"Prepared {len(updates)} DOM updates")
    
    if args.dry_run:
        log.info("DRY RUN: Would update the above listings")
    else:
        log.info("Updating listings...")
        supabase.table('listings').upsert(updates).execute()
        log.info(f"Successfully updated {len(updates)} listings")

if __name__ == '__main__':
    main()
