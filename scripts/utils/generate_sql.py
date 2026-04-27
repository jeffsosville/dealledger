# Same DOM calculation logic
import json

def calculate_dom_from_bbs_number(listing_number):
    from datetime import datetime, timedelta
    
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

# Generate SQL for first 100 listings that need updates
listing_data = [
    (112114, 2445744), (40885, 2473675), (31791, 2468770), (25467, 2463457),
    (3378, 2436332), (3205, 2435378), (30290, 2467277), # ... add more as needed
]

sql_updates = []
for listing_id, listing_number in listing_data:
    dom = calculate_dom_from_bbs_number(listing_number)
    if dom:
        sql_updates.append(f"UPDATE listings SET days_on_market = {dom} WHERE id = {listing_id};")

print("-- SQL to run in Supabase SQL Editor:")
print("\n".join(sql_updates[:20]))  # First 20 updates
