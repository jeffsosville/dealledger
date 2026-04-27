import os
from supabase import create_client
from datetime import datetime, timedelta

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

def calculate_dom_fixed(listing_number):
    if not listing_number or listing_number < 100000:
        return None
    
    today = datetime.now()
    
    if listing_number >= 2000000:
        estimated_days = max(0, 300 - (listing_number - 2400000) / 1000)  # Changed to max(0, ...)
        estimated_date = today - timedelta(days=estimated_days)
    else:
        estimated_date = today - timedelta(days=365 * 3)
    
    dom = (today.date() - estimated_date.date()).days
    return max(0, dom)  # Changed from max(1, dom) to max(0, dom)

# Get listings that currently show 1 day but should be 0
supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)
result = supabase.table('listings').select(
    'id, listing_number, days_on_market'
).eq('days_on_market', 1).not_.is_('listing_number', 'null').limit(100).execute()

print("-- SQL to fix same-day listings (1d -> 0d)")
print()

updates = []
for listing in result.data:
    dom = calculate_dom_fixed(listing['listing_number'])
    if dom == 0:  # Should be same day
        updates.append(f"UPDATE listings SET days_on_market = 0 WHERE id = {listing['id']};")

for update in updates:
    print(update)

print(f"-- Total same-day fixes: {len(updates)}")
