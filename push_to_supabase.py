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

import hashlib
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


def stable_listing_id(title: str, url: str, base_url: str, broker_domain: str) -> str:
    """
    Same identity scheme as stable_listing_id() in scrapers/dealledger_scraper_v6.py.
    Recomputed here rather than trusted from the snapshot: older snapshots
    (pre-2026-09-02) carry a 'hash' field built from sha256(title|asking_price|url),
    which produces a new id on every price edit (CLAUDE.md operating principle 4).
    Replaying one of those snapshots through this script must not re-inject that
    scheme, so the id is always derived fresh from the URL, never taken from the
    snapshot's stored 'hash'/'id' field.
    """
    if url and url != base_url:
        key = url
    else:
        key = f"{broker_domain}|{(title or '').strip().lower()}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]

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

    title = str(row.get('title', '') or '').strip()
    if not title:
        return None

    source_url = row.get('source_url') or row.get('url')
    broker_url = row.get('broker_url')
    if not source_url:
        return None

    record_id = stable_listing_id(title, source_url, broker_url, get_domain(source_url))

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
