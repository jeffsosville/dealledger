#!/usr/bin/env python3
"""
first_seen_monitor.py — catches a reset first_seen the day it happens.

THE PROBLEM THIS SOLVES
`first_seen` is the days-on-market anchor — the number the whole DealLedger
DOM methodology reads. It must be set once, at true creation, and never
move later. On 26 Aug 2026 a same-day revert (see CLAUDE.md #13) deleted the
upsert guard that protected it; every successful re-scrape of an existing
specialized-scraper row after that silently overwrote its first_seen with
"now". By 2 Sep, 1,058 execbb.com rows, 803 fcbb.com rows, and 480
murphybusiness.com rows had a reset first_seen, and nothing said so for a
week — the exact silent-failure shape CLAUDE.md principle 1 warns about.

WHAT IT DOES
One invariant: for a given row id, first_seen must never be later on this
observation than it was on the last one. It has no fixed notion of "the
previous value" beyond what it itself recorded last time it ran, so it
persists a baseline (id -> first_seen) locally and diffs against it every
run. Run this after every scrape — local cron and CI both — not on a
schedule of its own; a monitor that only checks weekly can miss six days
of damage same as the source it's watching.

Usage:
    python3 scrapers/first_seen_monitor.py
    python3 scrapers/first_seen_monitor.py --baseline data/first_seen_baseline.json
"""
import argparse
import json
import os
import sys
from collections import defaultdict

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def sb_headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}


def fetch_current():
    """id -> (broker_domain, first_seen) for every active row."""
    rows, offset, page = {}, 0, 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings_direct",
            headers=sb_headers(),
            params={"select": "id,broker_domain,first_seen", "status": "eq.active",
                    "limit": str(page), "offset": str(offset)},
            timeout=90,
        )
        r.raise_for_status()
        batch = r.json()
        for row in batch:
            rows[row["id"]] = (row.get("broker_domain"), row["first_seen"])
        if len(batch) < page:
            break
        offset += page
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--baseline", default="data/first_seen_baseline.json",
                     help="local file tracking id -> first_seen from the last run")
    args = ap.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
        sys.exit(2)

    baseline = {}
    if os.path.exists(args.baseline):
        with open(args.baseline) as f:
            baseline = json.load(f)
    print(f"Baseline: {len(baseline)} ids from the previous run "
          f"({args.baseline if baseline else 'none — first run, nothing to compare yet'})")

    current = fetch_current()
    print(f"Current: {len(current)} active rows")

    violations = defaultdict(list)
    for uid, (domain, first_seen) in current.items():
        prev = baseline.get(uid)
        if prev and first_seen > prev:
            violations[domain].append((uid, prev, first_seen))

    total = sum(len(v) for v in violations.values())
    if total:
        print(f"\n{'='*70}")
        print(f"⚠️  INVARIANT VIOLATED: {total} row(s) where first_seen moved LATER")
        print(f"{'='*70}")
        for domain, rows in sorted(violations.items(), key=lambda kv: -len(kv[1])):
            print(f"\n{domain}: {len(rows)} row(s)")
            for uid, prev, now in rows[:5]:
                print(f"   {uid}  {prev} -> {now}")
            if len(rows) > 5:
                print(f"   ... and {len(rows) - 5} more")
    else:
        print("\n✅ No violations — every id present in both observations kept its first_seen.")

    with open(args.baseline, "w") as f:
        json.dump({uid: fs for uid, (_, fs) in current.items()}, f)
    print(f"\nBaseline updated: {len(current)} ids written to {args.baseline}")

    sys.exit(1 if total else 0)


if __name__ == "__main__":
    main()
