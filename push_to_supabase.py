#!/usr/bin/env python3
"""
push_to_supabase.py
-------------------
Pushes a cleaned listings JSON file to the listings_direct table in Supabase.

Usage:
    python3 push_to_supabase.py --input data/snapshots/2026-03-24/listings_clean.json
    python3 push_to_supabase.py --input data/snapshots/2026-03-24/listings_clean.json --dry-run

Requires .env with SUPABASE_URL and SUPABASE_SERVICE_KEY.
"""

import json
import argparse
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

BATCH_SIZE = 200

LISTINGS_DIRECT_COLUMNS = {
    'id', 'title', 'url', 'broker_name', 'broker_domain',
    'city', 'state', 'asking_price', 'cash_flow', 'revenue',
    'category', 'vertical', 'description', 'quality_score', 'quality_tier',
    'bbs_listing_id', 'days_on_market', 'profile_views',
    'first_seen', 'last_seen', 'status', 'match_confidence', 'source',
    'created_at', 'updated_at',
}


def get_domain(url):
    try:
        return urlparse(str(url)).netloc
    except Exception:
        return None


def map_to_listings_direct(row):
    """Map a v5 snapshot record to listings_direct schema."""
    now = datetime.now(timezone.utc).isoformat()

    # Build ID from hash (already a stable 16-char hex)
    record_id = row.get('hash') or row.get('id')
    if not record_id:
        return None

    title = str(row.get('title', '') or '').strip()
    if not title:
        return None

    source_url = row.get('source_url') or row.get('url')
    broker_url = row.get('broker_url')

    return {
        'id':            record_id,
        'title':         title,
        'url':           source_url,
        'broker_name':   row.get('broker'),
        'broker_domain': get_domain(source_url),
        'city':          row.get('city'),
        'state':         row.get('state'),
        'asking_price':  row.get('asking_price'),
        'cash_flow':     row.get('cash_flow'),
        'revenue':       row.get('revenue'),
        'vertical':      row.get('vertical'),
        'description':   row.get('raw_text', '')[:2000] if row.get('raw_text') else None,
        'first_seen':    row.get('first_seen') or now,
        'last_seen':     row.get('last_seen') or now,
        'status':        'active',
        'source':        'broker_direct',
        'created_at':    now,
        'updated_at':    now,
    }


def push_listings(listings, dry_run=False):
    url  = os.environ['SUPABASE_URL']
    key  = os.environ['SUPABASE_SERVICE_KEY']
    sb   = create_client(url, key)

    mapped = []
    skipped = 0
    for row in listings:
        record = map_to_listings_direct(row)
        if record:
            mapped.append(record)
        else:
            skipped += 1

    print(f'Mapped:  {len(mapped)}')
    print(f'Skipped: {skipped}')

    if dry_run:
        print('\nDRY RUN — first 3 records that would be upserted:')
        for r in mapped[:3]:
            print(json.dumps({k: v for k, v in r.items() if k != 'description'}, indent=2, default=str))
        return

    # Push in batches
    inserted = 0
    errors   = 0
    total    = len(mapped)

    print(f'\nPushing {total} records in batches of {BATCH_SIZE}...')

    for i in range(0, total, BATCH_SIZE):
        batch = mapped[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        try:
            result = sb.table('listings_direct').upsert(
                batch,
                on_conflict='id'
            ).execute()
            inserted += len(batch)
            print(f'  Batch {batch_num}/{total_batches}: {len(batch)} records ✓')
        except Exception as e:
            errors += len(batch)
            print(f'  Batch {batch_num}/{total_batches}: ERROR — {e}')

        time.sleep(0.2)  # rate limit safety

    print(f'\n============================================================')
    print(f'PUSH COMPLETE')
    print(f'============================================================')
    print(f'Upserted: {inserted}')
    print(f'Errors:   {errors}')
    print(f'============================================================')


def main():
    parser = argparse.ArgumentParser(description='Push clean listings to Supabase listings_direct')
    parser.add_argument('--input', required=True, help='Path to listings_clean.json')
    parser.add_argument('--dry-run', action='store_true', help='Preview without writing to Supabase')
    args = parser.parse_args()

    print(f'Loading {args.input}...')
    with open(args.input) as f:
        listings = json.load(f)
    print(f'Loaded {len(listings)} listings')

    push_listings(listings, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
