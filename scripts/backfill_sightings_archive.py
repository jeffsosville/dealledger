#!/usr/bin/env python3
"""
backfill_sightings_archive.py — one-time load of data/snapshots/ into listing_sighting.

Each data/snapshots/<YYYY-MM-DD>/listings.csv is one day's generic-crawler output.
Every (listing id, snapshot date) becomes a sighting with source='archive'.
Only ids that exist in listings_direct are loaded. Existing sightings are left
alone (insert ... on conflict do nothing), so it is safe to re-run.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (from ~/.dealledger.env).

Usage:
    python3 scripts/backfill_sightings_archive.py --dry-run
    python3 scripts/backfill_sightings_archive.py
"""
import argparse
import csv
import glob
import os
import re
import sys

import requests

csv.field_size_limit(10**9)
BATCH = 5000
DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def archive_sightings(root):
    pairs = set()
    for d in sorted(glob.glob(os.path.join(root, "*"))):
        day = os.path.basename(d)
        f = os.path.join(d, "listings.csv")
        if not DAY_RE.match(day) or not os.path.exists(f):
            continue
        with open(f, newline="", encoding="utf-8", errors="replace") as fh:
            for row in csv.DictReader(fh):
                i = (row.get("id") or "").strip()
                if len(i) == 16:  # pre-March 32-char ids don't map to listings_direct
                    pairs.add((i, day))
    return pairs


def known_ids(base, headers):
    ids, last = set(), ""
    while True:
        r = requests.get(f"{base}/rest/v1/listings_direct", headers=headers, timeout=60,
                         params={"select": "id", "id": f"gt.{last}", "order": "id.asc", "limit": "1000"})
        r.raise_for_status()
        batch = r.json()
        if not batch:
            return ids
        ids.update(x["id"] for x in batch)
        last = batch[-1]["id"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/snapshots")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    pairs = archive_sightings(args.root)
    days = sorted({d for _, d in pairs})
    print(f"archive: {len(pairs):,} sightings, {len({i for i, _ in pairs}):,} ids, "
          f"{len(days)} days ({days[0] if days else '-'} .. {days[-1] if days else '-'})")
    if len(days) < 50:
        sys.exit("Fewer than 50 snapshot days found; is this a shallow checkout? Stopping.")

    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not base or not key:
        if args.dry_run:
            return
        sys.exit("Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}

    ids = known_ids(base, headers)
    rows = [{"row_id": i, "seen_on": d, "source": "archive"} for i, d in sorted(pairs) if i in ids]
    print(f"listings_direct ids: {len(ids):,}; loadable sightings: {len(rows):,}")
    if args.dry_run:
        return

    post_headers = dict(headers, **{"Content-Type": "application/json",
                                    "Prefer": "resolution=ignore-duplicates,return=minimal"})
    for n in range(0, len(rows), BATCH):
        r = requests.post(f"{base}/rest/v1/listing_sighting?on_conflict=row_id,seen_on",
                          headers=post_headers, json=rows[n:n + BATCH], timeout=120)
        if r.status_code >= 300:
            sys.exit(f"batch {n // BATCH}: HTTP {r.status_code} {r.text[:300]}")
        print(f"  {min(n + BATCH, len(rows)):,} / {len(rows):,}")
    print("done")


if __name__ == "__main__":
    main()
