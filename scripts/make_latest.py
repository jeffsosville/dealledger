"""
Convert V4 snapshot JSON to dealledger.org homepage format.
The homepage JS expects a flat JSON array with specific field names.

Usage:
    python3 scripts/make_latest.py data/snapshots/2026-02-19/listings_20260220_234539.json
"""
import json
import sys
from pathlib import Path

if len(sys.argv) < 2:
    print("Usage: python3 scripts/make_latest.py <snapshot_json>")
    sys.exit(1)

input_path = sys.argv[1]

with open(input_path) as f:
    data = json.load(f)

# Handle both wrapped and flat formats
if isinstance(data, dict) and 'listings' in data:
    listings = data['listings']
else:
    listings = data

# Normalize field names to match what index.html expects
normalized = []
for l in listings:
    normalized.append({
        'title': l.get('title'),
        'price': l.get('asking_price') or l.get('price'),
        'state': l.get('state'),
        'city': l.get('city'),
        'business_type': l.get('business_type') or l.get('vertical'),
        'source_url': l.get('source_url') or l.get('listing_url') or l.get('url'),
        'broker_name': l.get('broker_name'),
        'scraped_at': l.get('first_seen') or l.get('scraped_at'),
        'location': l.get('location'),
        'revenue': l.get('revenue'),
        'cash_flow': l.get('cash_flow'),
    })

# Write flat array to data/latest.json
output_path = Path("data/latest.json")
with open(output_path, 'w') as f:
    json.dump(normalized, f)

print(f"Wrote {len(normalized)} listings to {output_path}")

# Also write CSV
import csv
csv_path = Path("data/latest.csv")
if normalized:
    keys = ['title', 'price', 'state', 'city', 'business_type', 'source_url',
            'broker_name', 'scraped_at', 'location', 'revenue', 'cash_flow']
    with open(csv_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(normalized)
    print(f"Wrote {len(normalized)} listings to {csv_path}")
