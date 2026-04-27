import os
from supabase import create_client

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)

# Check DOM status
result = supabase.table('listings').select('id', count='exact').not_.is_('days_on_market', 'null').execute()
print(f"Listings with DOM populated: {result.count}")

# Sample a few
sample = supabase.table('listings').select('listing_number, days_on_market, header').not_.is_('days_on_market', 'null').limit(5).execute()
for row in sample.data:
    print(f"  {row['listing_number']}: {row['days_on_market']} days | {(row['header'] or 'No title')[:40]}")
