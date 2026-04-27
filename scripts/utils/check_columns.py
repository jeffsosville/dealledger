import os
from supabase import create_client

DEALLEDGER_URL = 'https://kqckuedsyyosmccushyd.supabase.co'
DEALLEDGER_KEY = os.environ.get('DEALLEDGER_ANON_KEY')

supabase = create_client(DEALLEDGER_URL, DEALLEDGER_KEY)

# Check what columns exist and sample data
result = supabase.table('listings').select('*').limit(3).execute()

if result.data:
    print("Available columns:")
    for key in result.data[0].keys():
        print(f"  {key}")
    
    print("\nSample records:")
    for i, record in enumerate(result.data):
        print(f"\nRecord {i+1}:")
        print(f"  id: {record.get('id')}")
        print(f"  listing_number: {record.get('listing_number')}")
        print(f"  source: {record.get('source')}")
        print(f"  url: {record.get('url', '')[:60] if record.get('url') else 'None'}")
        print(f"  days_on_market: {record.get('days_on_market')}")
        print(f"  header: {record.get('header', '')[:50] if record.get('header') else 'None'}")
