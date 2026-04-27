#!/usr/bin/env python3
"""
Backfill DOM and BizQuest views into DealLedger listings table

This script:
1. Calculates DOM from listing_number for BizBuySell listings
2. Merges BizQuest profileViews from enriched JSON files
3. Updates the listings table with both signals

Usage:
    python backfill_dom_views.py --bizquest-json bizquest_enriched.json
    python backfill_dom_views.py --dom-only  # Just backfill DOM
    python backfill_dom_views.py --dry-run   # Preview changes
"""

import json
import sys
import os
from datetime import datetime, timezone, timedelta
from supabase import create_client
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

# Supabase config
DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

def bbs_listing_number_to_date(listing_number):
    """Convert BizBuySell listing number to estimated listing date"""
    if not listing_number or listing_number < 10000000:
        return None
    
    # BizBuySell started around listing 10000000 in early 2020
    # They increment roughly 500,000 per year
    BASE_NUMBER = 10000000
    BASE_DATE = datetime(2020, 1, 1)
    LISTINGS_PER_DAY = 500000 / 365.25  # ~1370 per day
    
    days_since_base = (listing_number - BASE_NUMBER) / LISTINGS_PER_DAY
    estimated_date = BASE_DATE + timedelta(days=days_since_base)
    return estimated_date.date()

def calculate_dom(listing_number):
    """Calculate days on market from listing number"""
    if not listing_number:
        return None
    
    estimated_date = bbs_listing_number_to_date(listing_number)
    if not estimated_date:
        return None
    
    today = datetime.now().date()
    dom = (today - estimated_date).days
    return max(0, dom)

def load_bizquest_views(json_file):
    """Load BizQuest enriched JSON and extract listNumber -> profileViews mapping"""
    if not json_file or not os.path.exists(json_file):
        log.warning(f"BizQuest JSON file not found: {json_file}")
        return {}
    
    log.info(f"Loading BizQuest views from {json_file}")
    
    try:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        views_map = {}
        total_listings = len(data)
        with_views = 0
        
        for listing in data:
            list_number = listing.get('listNumber')
            profile_views = listing.get('profileViews')
            
            if list_number and profile_views is not None:
                views_map[int(list_number)] = int(profile_views)
                with_views += 1
        
        log.info(f"Loaded {with_views}/{total_listings} BizQuest listings with view counts")
        return views_map
        
    except Exception as e:
        log.error(f"Error loading BizQuest JSON: {e}")
        return {}

def fetch_listings_needing_updates(supabase, update_dom=True, update_views=True):
    """Fetch listings that need DOM or views updates"""
    
    conditions = []
    if update_dom:
        conditions.append("(days_on_market is null and listing_number is not null)")
    if update_views:
        conditions.append("(listing_views is null)")
    
    if not conditions:
        log.warning("No update flags specified")
        return []
    
    where_clause = " OR ".join(f"({c})" for c in conditions)
    
    log.info(f"Fetching listings where: {where_clause}")
    
    try:
        result = supabase.table('listings').select(
            'id, listing_number, listing_views, days_on_market, source, header'
        ).or_(where_clause).execute()
        
        listings = result.data
        log.info(f"Found {len(listings)} listings needing updates")
        return listings
        
    except Exception as e:
        log.error(f"Error fetching listings: {e}")
        return []

def update_listing_batch(supabase, updates, dry_run=False):
    """Update a batch of listings with new DOM/views data"""
    if not updates:
        return
    
    if dry_run:
        log.info(f"DRY RUN: Would update {len(updates)} listings")
        for update in updates[:5]:  # Show first 5 as preview
            log.info(f"  ID {update['id']}: DOM={update.get('days_on_market', 'unchanged')}, "
                    f"Views={update.get('listing_views', 'unchanged')}")
        if len(updates) > 5:
            log.info(f"  ... and {len(updates) - 5} more")
        return
    
    try:
        # Upsert in batches of 100
        batch_size = 100
        for i in range(0, len(updates), batch_size):
            batch = updates[i:i + batch_size]
            supabase.table('listings').upsert(batch).execute()
            log.info(f"Updated batch {i//batch_size + 1}/{(len(updates)-1)//batch_size + 1}")
        
        log.info(f"Successfully updated {len(updates)} listings")
        
    except Exception as e:
        log.error(f"Error updating listings: {e}")

def main():
    parser = argparse.ArgumentParser(description='Backfill DOM and BizQuest views into DealLedger')
    parser.add_argument('--bizquest-json', help='Path to BizQuest enriched JSON file')
    parser.add_argument('--dom-only', action='store_true', help='Only update DOM, skip views')
    parser.add_argument('--views-only', action='store_true', help='Only update views, skip DOM')
    parser.add_argument('--dry-run', action='store_true', help='Preview changes without updating')
    parser.add_argument('--limit', type=int, help='Limit number of listings to process')
    
    args = parser.parse_args()
    
    if not DEALLEDGER_KEY:
        log.error("Set DEALLEDGER_ANON_KEY environment variable")
        sys.exit(1)
    
    # Create Supabase client
    supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)
    
    # Determine what to update
    update_dom = not args.views_only
    update_views = not args.dom_only and args.bizquest_json
    
    if not update_dom and not update_views:
        log.error("Nothing to update. Specify --bizquest-json for views or allow DOM updates.")
        sys.exit(1)
    
    log.info(f"Backfill plan: DOM={update_dom}, Views={update_views}")
    
    # Load BizQuest views if provided
    bizquest_views = {}
    if update_views:
        bizquest_views = load_bizquest_views(args.bizquest_json)
        if not bizquest_views:
            log.warning("No BizQuest views loaded, skipping views update")
            update_views = False
    
    # Fetch listings needing updates
    listings = fetch_listings_needing_updates(supabase, update_dom, update_views)
    
    if args.limit:
        listings = listings[:args.limit]
        log.info(f"Limited to {len(listings)} listings")
    
    if not listings:
        log.info("No listings need updates")
        return
    
    # Prepare updates
    updates = []
    dom_updated = 0
    views_updated = 0
    
    for listing in listings:
        update = {'id': listing['id']}
        updated_fields = []
        
        # Update DOM if needed and possible
        if (update_dom and 
            listing.get('days_on_market') is None and 
            listing.get('listing_number')):
            
            dom = calculate_dom(listing['listing_number'])
            if dom is not None:
                update['days_on_market'] = dom
                updated_fields.append(f"DOM={dom}")
                dom_updated += 1
        
        # Update views if needed and available
        if (update_views and 
            listing.get('listing_views') is None and 
            listing.get('listing_number')):
            
            views = bizquest_views.get(listing['listing_number'])
            if views is not None:
                update['listing_views'] = views
                updated_fields.append(f"Views={views}")
                views_updated += 1
        
        # Only add to updates if we're changing something
        if len(update) > 1:  # More than just 'id'
            updates.append(update)
            log.debug(f"Listing {listing['id']} ({listing.get('header', '')[:50]}): {', '.join(updated_fields)}")
    
    log.info(f"Prepared {len(updates)} updates: {dom_updated} DOM, {views_updated} views")
    
    if updates:
        update_listing_batch(supabase, updates, args.dry_run)
    
    # Summary stats
    if not args.dry_run and updates:
        log.info("Backfill complete. Fetching summary stats...")
        try:
            stats = supabase.table('listings').select(
                'days_on_market, listing_views', count='exact'
            ).not_.is_('days_on_market', 'null').not_.is_('listing_views', 'null').execute()
            
            count_with_both = len(stats.data) if stats.data else 0
            log.info(f"Listings now with both DOM and Views: {count_with_both}")
            
        except Exception as e:
            log.warning(f"Could not fetch summary stats: {e}")

if __name__ == '__main__':
    main()
