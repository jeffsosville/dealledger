#!/usr/bin/env python3
import os
from supabase import create_client

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)

# Get sample listings with null DOM
result = supabase.table('listings').select(
    'id, listing_number, days_on_market, header, source'
).is_('days_on_market', 'null').not_.is_('listing_number', 'null').limit(10).execute()

print("Sample listings with null DOM:")
for listing in result.data:
    print(f"  ID: {listing['id']}")
    print(f"  Number: {listing['listing_number']}")
    print(f"  Source: {listing['source']}")
    print(f"  Header: {listing.get('header', '')[:60]}")
    print()

# Also check range of listing numbers
all_result = supabase.table('listings').select('listing_number').not_.is_('listing_number', 'null').limit(100).execute()
numbers = [l['listing_number'] for l in all_result.data]
if numbers:
    print(f"Listing number range: {min(numbers):,} to {max(numbers):,}")
