#!/usr/bin/env python3
"""
DealLedger Daily Export
=======================
Exports listings from Supabase to JSON/CSV files for public release.

Usage:
    python scripts/export_daily.py              # Export today's snapshot
    python scripts/export_daily.py --days 7     # Export last 7 days
    python scripts/export_daily.py --all        # Export everything

Output:
    data/daily/YYYY-MM-DD.json
    data/daily/YYYY-MM-DD.csv
    data/latest.json (copy of most recent)
    data/latest.csv
"""

import os
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()


def get_supabase():
    """Initialize Supabase client."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("Set SUPABASE_URL and SUPABASE_KEY environment variables")
    return create_client(url, key)


def fetch_listings(supabase, since_date=None, limit=None):
    """
    Fetch listings from Supabase.
    
    Args:
        supabase: Supabase client
        since_date: Only fetch listings updated after this date
        limit: Max number of listings to fetch
    
    Returns:
        List of listing dicts
    """
    print("Fetching listings from Supabase...")
    
    all_listings = []
    page_size = 1000
    offset = 0
    
    while True:
        query = supabase.table('listings').select('*')
        
        if since_date:
            query = query.gte('scraped_at', since_date.isoformat())
        
        query = query.order('scraped_at', desc=True)
        query = query.range(offset, offset + page_size - 1)
        
        response = query.execute()
        
        if not response.data:
            break
        
        all_listings.extend(response.data)
        print(f"  Fetched {len(all_listings)} listings...")
        
        if len(response.data) < page_size:
            break
        
        offset += page_size
        
        if limit and len(all_listings) >= limit:
            all_listings = all_listings[:limit]
            break
    
    print(f"Total: {len(all_listings)} listings")
    return all_listings


def clean_for_export(listings):
    """
    Clean listings for public export.
    Remove internal fields, standardize format.
    """
    public_fields = [
        'listing_id',
        'title',
        'price',
        'price_text',
        'location',
        'city',
        'state',
        'description',
        'listing_url',
        'business_type',
        'revenue',
        'cash_flow',
        'broker_account',
        'scraped_at'
    ]
    
    cleaned = []
    for listing in listings:
        clean = {k: listing.get(k) for k in public_fields}
        cleaned.append(clean)
    
    return cleaned


def export_snapshot(listings, output_dir, date_str=None):
    """
    Export listings to JSON and CSV.
    
    Args:
        listings: List of listing dicts
        output_dir: Directory to write files
        date_str: Date string for filename (default: today)
    
    Returns:
        Tuple of (json_path, csv_path)
    """
    if date_str is None:
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    daily_dir = output_dir / 'daily'
    daily_dir.mkdir(exist_ok=True)
    
    # Export JSON
    json_path = daily_dir / f'{date_str}.json'
    with open(json_path, 'w') as f:
        json.dump(listings, f, indent=2, default=str)
    print(f"Wrote {json_path}")
    
    # Export CSV
    csv_path = daily_dir / f'{date_str}.csv'
    df = pd.DataFrame(listings)
    df.to_csv(csv_path, index=False)
    print(f"Wrote {csv_path}")
    
    # Update latest symlinks/copies
    latest_json = output_dir / 'latest.json'
    latest_csv = output_dir / 'latest.csv'
    
    with open(latest_json, 'w') as f:
        json.dump(listings, f, indent=2, default=str)
    df.to_csv(latest_csv, index=False)
    print(f"Updated latest.json and latest.csv")
    
    return json_path, csv_path


def generate_stats(listings):
    """Generate summary statistics."""
    df = pd.DataFrame(listings)
    
    stats = {
        'total_listings': len(listings),
        'with_price': df['price'].notna().sum(),
        'with_revenue': df['revenue'].notna().sum(),
        'with_cash_flow': df['cash_flow'].notna().sum(),
        'unique_brokers': df['broker_account'].nunique(),
        'states_covered': df['state'].nunique(),
    }
    
    # Price stats
    prices = df['price'].dropna()
    if len(prices) > 0:
        stats['price_median'] = prices.median()
        stats['price_mean'] = prices.mean()
        stats['price_min'] = prices.min()
        stats['price_max'] = prices.max()
    
    # By state
    state_counts = df['state'].value_counts().head(10).to_dict()
    stats['top_states'] = state_counts
    
    # By business type
    type_counts = df['business_type'].value_counts().head(10).to_dict()
    stats['top_types'] = type_counts
    
    return stats


def print_stats(stats):
    """Print summary statistics."""
    print(f"\n{'='*50}")
    print("SNAPSHOT STATISTICS")
    print('='*50)
    print(f"Total listings:     {stats['total_listings']:,}")
    print(f"With price:         {stats['with_price']:,} ({stats['with_price']/stats['total_listings']*100:.1f}%)")
    print(f"With revenue:       {stats['with_revenue']:,}")
    print(f"With cash flow:     {stats['with_cash_flow']:,}")
    print(f"Unique brokers:     {stats['unique_brokers']:,}")
    print(f"States covered:     {stats['states_covered']}")
    
    if 'price_median' in stats:
        print(f"\nPrice range: ${stats['price_min']:,.0f} - ${stats['price_max']:,.0f}")
        print(f"Median price: ${stats['price_median']:,.0f}")
    
    if stats.get('top_states'):
        print(f"\nTop states:")
        for state, count in list(stats['top_states'].items())[:5]:
            print(f"  {state}: {count:,}")
    
    print('='*50)


def main():
    parser = argparse.ArgumentParser(description="Export DealLedger listings to public files")
    parser.add_argument('--days', type=int, help="Export listings from last N days")
    parser.add_argument('--all', action='store_true', help="Export all listings")
    parser.add_argument('--output', default='data', help="Output directory (default: data)")
    parser.add_argument('--date', help="Date string for filename (default: today)")
    
    args = parser.parse_args()
    
    # Determine date filter
    since_date = None
    if args.days:
        since_date = datetime.now() - timedelta(days=args.days)
        print(f"Exporting listings since {since_date.date()}")
    elif not args.all:
        # Default: today only
        since_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        print(f"Exporting today's listings")
    else:
        print("Exporting ALL listings")
    
    # Fetch from Supabase
    supabase = get_supabase()
    listings = fetch_listings(supabase, since_date=since_date)
    
    if not listings:
        print("No listings found!")
        return
    
    # Clean for export
    cleaned = clean_for_export(listings)
    
    # Export
    date_str = args.date or datetime.now().strftime('%Y-%m-%d')
    export_snapshot(cleaned, args.output, date_str)
    
    # Stats
    stats = generate_stats(cleaned)
    print_stats(stats)
    
    print(f"\n✓ Export complete!")
    print(f"  Files: {args.output}/daily/{date_str}.json, {args.output}/daily/{date_str}.csv")
    print(f"  Latest: {args.output}/latest.json, {args.output}/latest.csv")


if __name__ == "__main__":
    main()
