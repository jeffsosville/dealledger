#!/usr/bin/env python3
import json
import sys
import argparse

def main():
    parser = argparse.ArgumentParser(description='Generate SQL to backfill BizQuest views')
    parser.add_argument('--json', required=True, help='Path to BizQuest enriched JSON file')
    
    args = parser.parse_args()
    
    try:
        with open(args.json, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("-- SQL to update listing_views from BizQuest data")
    print("-- Run this in Supabase SQL Editor")
    print()
    
    updates = []
    view_count = 0
    
    for listing in data:
        list_number = listing.get('listNumber')
        profile_views = listing.get('profileViews')
        
        if list_number and profile_views is not None:
            # Match by listing_number and update listing_views
            updates.append(f"UPDATE listings SET listing_views = {profile_views} WHERE listing_number = {list_number};")
            view_count += 1
    
    # Print in batches of 50
    for i in range(0, len(updates), 50):
        chunk = updates[i:i+50]
        print(f"-- Batch {i//50 + 1} ({len(chunk)} updates)")
        for update in chunk:
            print(update)
        print()
    
    print(f"-- Total: {len(updates)} view updates generated")
    print(f"-- Listings with view data: {view_count}")

if __name__ == '__main__':
    main()
