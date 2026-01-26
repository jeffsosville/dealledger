"""
DealLedger: Snapshot Generation

Creates CSV and JSON snapshots of the current ledger state.
"""

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any


# Fields to include in CSV export (order matters)
CSV_FIELDS = [
    'id',
    'source_url',
    'broker_id',
    'broker_name',
    'status',
    'first_seen',
    'last_seen',
    'title',
    'asking_price',
    'price_hidden',
    'vertical',
    'category',
    'city',
    'state',
    'country',
    'revenue',
    'cash_flow',
    'employees',
    'year_established',
    'seller_financing',
    'sba_prequalified',
    'franchise',
    'confidence',
    'content_hash',
    'flags',
]


def flatten_for_csv(listing: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a listing dict for CSV export."""
    flat = {}
    
    for field in CSV_FIELDS:
        value = listing.get(field)
        
        # Handle lists (like flags)
        if isinstance(value, list):
            flat[field] = '|'.join(str(v) for v in value)
        # Handle booleans
        elif isinstance(value, bool):
            flat[field] = 'true' if value else 'false'
        # Handle None
        elif value is None:
            flat[field] = ''
        else:
            flat[field] = value
    
    return flat


def generate_snapshot_csv(
    listings: List[Dict[str, Any]], 
    output_path: Path
) -> None:
    """Generate a CSV snapshot."""
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        
        for listing in listings:
            flat = flatten_for_csv(listing)
            writer.writerow(flat)


def generate_snapshot_json(
    listings: List[Dict[str, Any]], 
    output_path: Path,
    metadata: Dict[str, Any] = None
) -> None:
    """Generate a JSON snapshot."""
    snapshot = {
        "snapshot_version": "1.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "listings_count": len(listings),
        "listings": listings,
    }
    
    if metadata:
        snapshot["metadata"] = metadata
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(snapshot, f, indent=2)


def generate_snapshot(
    listings: List[Dict[str, Any]],
    output_dir: str = "data/snapshots",
    date_str: str = None,
    formats: List[str] = None
) -> Dict[str, str]:
    """
    Generate snapshot files.
    
    Args:
        listings: List of normalized listings
        output_dir: Directory for snapshot files
        date_str: Date string for filename (default: today)
        formats: List of formats to generate (default: ['csv', 'json'])
        
    Returns:
        Dict mapping format to output path
    """
    if formats is None:
        formats = ['csv', 'json']
    
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    outputs = {}
    
    if 'csv' in formats:
        csv_path = output_path / f"{date_str}.csv"
        generate_snapshot_csv(listings, csv_path)
        outputs['csv'] = str(csv_path)
        
        # Also create 'latest.csv' symlink/copy
        latest_csv = output_path / "latest.csv"
        if latest_csv.exists():
            latest_csv.unlink()
        # Copy instead of symlink for portability
        generate_snapshot_csv(listings, latest_csv)
    
    if 'json' in formats:
        json_path = output_path / f"{date_str}.json"
        generate_snapshot_json(listings, json_path)
        outputs['json'] = str(json_path)
        
        latest_json = output_path / "latest.json"
        if latest_json.exists():
            latest_json.unlink()
        generate_snapshot_json(listings, latest_json)
    
    return outputs


def main():
    """CLI for generating snapshots."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate ledger snapshots")
    parser.add_argument("input", help="Input JSON file (normalized listings)")
    parser.add_argument(
        "-o", "--output-dir", 
        default="data/snapshots",
        help="Output directory"
    )
    parser.add_argument(
        "-d", "--date",
        help="Date string for filename (default: today)"
    )
    parser.add_argument(
        "-f", "--formats",
        default="csv,json",
        help="Comma-separated formats to generate"
    )
    
    args = parser.parse_args()
    
    with open(args.input) as f:
        data = json.load(f)
    
    listings = data.get('listings', data)
    if isinstance(listings, dict):
        listings = [listings]
    
    formats = [f.strip() for f in args.formats.split(',')]
    
    outputs = generate_snapshot(
        listings,
        output_dir=args.output_dir,
        date_str=args.date,
        formats=formats
    )
    
    print(f"Generated {len(listings)} listings to:")
    for fmt, path in outputs.items():
        print(f"  {fmt}: {path}")


if __name__ == "__main__":
    main()
