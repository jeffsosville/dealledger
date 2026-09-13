#!/usr/bin/env python3
"""
backfill_first_seen_from_snapshots.py — recover the DOM anchor from the archive.

WHY THIS EXISTS
From the 26 Aug revert until 13 Sep, every re-scrape reset
listings_direct.first_seen to "now" (see SupabaseWriter.upsert()). 46,326 rows
were repaired from listings.first_seen, which the bridge never overwrites — but
that source only goes back to 2026-03-24, and it does not cover rows that were
never bridged. ~13,000 active rows are still dated later than they were really
first seen.

data/snapshots/ is the older and better witness: every daily run has committed
the full listing set since 2026-02-18. If an id appears in the 2026-04-03
snapshot, it existed on 2026-04-03, whatever the row says today.

SAFETY
This can only move first_seen EARLIER. The database enforces that regardless of
what this script sends:

    trg_listings_direct_lock_first_seen (BEFORE UPDATE)
      new.first_seen := least(new.first_seen, old.first_seen)

So it is safe to re-run, and safe to run against a partial checkout — a missing
snapshot means a missed correction, never a wrong one.

Ids changed format in March 2026 (32-char md5 -> 16-char). Pre-March snapshots
are read but their ids simply will not match anything, which is correct.

Usage:
    python3 scrapers/backfill_first_seen_from_snapshots.py            # dry run
    python3 scrapers/backfill_first_seen_from_snapshots.py --apply
"""
import argparse
import glob
import json
import os
import sys
from collections import Counter

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def headers(extra=None):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
    if extra:
        h.update(extra)
    return h


def build_archive(root):
    """id -> earliest snapshot date (YYYY-MM-DD) across every committed run."""
    best, dates_read = {}, 0
    for date_dir in sorted(os.listdir(root)):
        path = os.path.join(root, date_dir)
        if not os.path.isdir(path) or len(date_dir) != 10:
            continue
        found = False
        for f in glob.glob(os.path.join(path, "listings*.json")):
            try:
                obj = json.load(open(f))
            except Exception as exc:
                print(f"   skip {f}: {exc}")
                continue
            rows = obj.get("listings") if isinstance(obj, dict) else obj
            if not isinstance(rows, list):
                continue
            found = True
            for r in rows:
                i = r.get("id")
                if i and (i not in best or date_dir < best[i]):
                    best[i] = date_dir
        dates_read += bool(found)
    return best, dates_read


def fetch_current():
    """id -> first_seen (ISO) for every row in listings_direct."""
    rows, offset, page = {}, 0, 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings_direct",
            headers=headers(),
            params={"select": "id,first_seen", "limit": str(page), "offset": str(offset)},
            timeout=90,
        )
        r.raise_for_status()
        batch = r.json()
        for row in batch:
            rows[row["id"]] = row["first_seen"]
        if len(batch) < page:
            break
        offset += page
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshots", default="data/snapshots")
    ap.add_argument("--apply", action="store_true",
                    help="actually write. Without it this only reports.")
    args = ap.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
        sys.exit(2)

    archive, dates_read = build_archive(args.snapshots)
    print(f"Archive: {len(archive):,} ids across {dates_read} snapshot dates "
          f"(earliest {min(archive.values()) if archive else 'n/a'})")

    # An empty or tiny archive means a shallow checkout, not a clean database.
    # Assert rather than silently report "nothing to do" (CLAUDE.md #1).
    if dates_read < 30:
        print(f"ABORT: only {dates_read} snapshot dates readable — "
              f"this looks like a partial checkout, not a healthy archive.")
        sys.exit(1)

    current = fetch_current()
    print(f"Database: {len(current):,} listings_direct rows")

    fixes = []
    for uid, fs in current.items():
        seen = archive.get(uid)
        if seen and seen < fs[:10]:
            fixes.append((uid, fs[:10], seen))

    print(f"\nRows whose archive date precedes their stored first_seen: {len(fixes):,}")
    if fixes:
        drift = sorted((int(__import__("datetime").date.fromisoformat(a).toordinal()
                            - __import__("datetime").date.fromisoformat(b).toordinal())
                        for _, a, b in fixes), reverse=True)
        print(f"   worst drift: {drift[0]} days   median drift: {drift[len(drift)//2]} days")
        by_month = Counter(b[:7] for _, _, b in fixes)
        for m in sorted(by_month):
            print(f"   restored to {m}: {by_month[m]:,}")
        print("\n   sample:")
        for uid, was, now in fixes[:5]:
            print(f"      {uid}  {was} -> {now}")

    if not args.apply:
        print("\nDry run — nothing written. Re-run with --apply.")
        return

    written = 0
    for i in range(0, len(fixes), 500):
        batch = [{"id": uid, "first_seen": f"{seen}T00:00:00+00:00"}
                 for uid, _, seen in fixes[i:i + 500]]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/listings_direct",
            headers=headers({"Content-Type": "application/json",
                             "Prefer": "resolution=merge-duplicates,return=minimal"}),
            data=json.dumps(batch), timeout=120,
        )
        if r.status_code >= 300:
            print(f"   batch at {i} failed: {r.status_code} {r.text[:300]}")
            sys.exit(1)
        written += len(batch)
        print(f"   {written:,}/{len(fixes):,}")

    # Verify against the database rather than trusting the write count.
    after = fetch_current()
    remaining = sum(1 for uid, fs in after.items()
                    if archive.get(uid) and archive[uid] < fs[:10])
    print(f"\nWrote {written:,}. Rows still later than their archive date: {remaining:,}")
    if remaining:
        print("Some rows did not move. Check that "
              "trg_listings_direct_lock_first_seen is still least(), not a hard reject.")
        sys.exit(1)
    print("✅ first_seen now matches the earliest archival observation for every id.")


if __name__ == "__main__":
    main()
