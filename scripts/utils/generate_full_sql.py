import os
from supabase import create_client
from datetime import datetime, timedelta

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

def calculate_dom_from_bbs_number(listing_number):
    if not listing_number or listing_number < 100000:
        return None
    
    today = datetime.now()
    
    if listing_number >= 2000000:
        estimated_days = max(1, 300 - (listing_number - 2400000) / 1000)
        estimated_date = today - timedelta(days=estimated_days)
    else:
        estimated_date = today - timedelta(days=365 * 3)
    
    dom = (today.date() - estimated_date.date()).days
    return max(1, dom)

# Fetch all listings that need DOM updates
supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)
result = supabase.table('listings').select(
    'id, listing_number'
).is_('days_on_market', 'null').not_.is_('listing_number', 'null').limit(1000).execute()

print("-- SQL to update DOM for null records")
print("-- Run this in Supabase SQL Editor")
print()

sql_updates = []
for listing in result.data:
    dom = calculate_dom_from_bbs_number(listing['listing_number'])
    if dom:
        sql_updates.append(f"UPDATE listings SET days_on_market = {dom} WHERE id = {listing['id']};")

# Print in chunks
for i in range(0, len(sql_updates), 50):
    chunk = sql_updates[i:i+50]
    print(f"-- Batch {i//50 + 1} ({len(chunk)} updates)")
    print("\n".join(chunk))
    print()

print(f"-- Total: {len(sql_updates)} updates generated")
