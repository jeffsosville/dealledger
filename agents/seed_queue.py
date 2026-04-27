#!/usr/bin/env python3
"""
Seed scrape_jobs queue from brokers_clean.csv

Usage:
  python3 seed_queue.py data/brokers_clean.csv
  python3 seed_queue.py data/brokers_clean.csv --type scrape --priority 100
  python3 seed_queue.py data/brokers_clean.csv --skip-discovered

Env:
  SUPABASE_URL, SUPABASE_KEY
"""

import os
import csv
import sys
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse

from supabase import create_client

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description="Seed scrape_jobs queue")
    parser.add_argument("csv_path", help="Path to broker CSV (brokers_clean.csv)")
    parser.add_argument("--type", default="discover",
                        choices=["discover", "scrape", "rediscover"],
                        help="Job type to seed (default: discover)")
    parser.add_argument("--priority", type=int, default=10,
                        help="Job priority (lower = higher priority, default: 10)")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--skip-discovered", action="store_true",
                        help="Skip domains already in broker_discovery with status=ok")
    args = parser.parse_args()

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Load URLs from CSV
    urls = []
    with open(args.csv_path, newline="") as f:
        sample = f.read(500)
    
    with open(args.csv_path, newline="") as f:
        if "," in sample.split("\n")[0]:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get("listings_url") or row.get("url") or
                       row.get("URL") or row.get("website") or
                       list(row.values())[0] if row else "").strip()
                if url.startswith("http"):
                    urls.append(url)
        else:
            urls = [l.strip() for l in f if l.strip().startswith("http")]

    print(f"Loaded {len(urls)} URLs from {args.csv_path}")

    # Optionally skip already-discovered domains
    skip_domains = set()
    if args.skip_discovered:
        try:
            rows = sb.table("broker_discovery").select("domain").eq("status", "ok").execute().data
            skip_domains = {r["domain"] for r in rows}
            print(f"Skipping {len(skip_domains)} already-discovered domains")
        except Exception as e:
            print(f"Warning: could not load discovered domains: {e}")

    inserted = 0
    skipped = 0
    duplicate = 0

    for url in urls:
        domain = urlparse(url).netloc.lower().replace("www.", "")

        if domain in skip_domains:
            skipped += 1
            continue

        try:
            sb.table("scrape_jobs").insert({
                "domain": domain,
                "url": url,
                "job_type": args.type,
                "priority": args.priority,
                "status": "queued",
                "attempts": 0,
                "max_attempts": args.max_attempts,
                "run_after": now_iso(),
                "scheduled_at": now_iso(),
            }).execute()
            inserted += 1
        except Exception:
            duplicate += 1  # partial unique index blocks duplicates

    print(f"\nDone:")
    print(f"  Inserted:   {inserted}")
    print(f"  Skipped:    {skipped} (already discovered)")
    print(f"  Duplicates: {duplicate} (already queued)")
    print(f"\nNext: python3 queue_runner.py --workers 4")


if __name__ == "__main__":
    main()
