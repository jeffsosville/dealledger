import os
from supabase import create_client

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)

# Get range of listing numbers
result = supabase.table('listings').select('listing_number').not_.is_('listing_number', 'null').limit(1000).execute()
numbers = [l['listing_number'] for l in result.data]
if numbers:
    print(f"Range: {min(numbers):,} to {max(numbers):,}")
    print(f"Sample numbers: {sorted(numbers)[:10]}")
