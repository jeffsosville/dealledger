#!/usr/bin/env python3
"""
staleness_monitor.py — per-source staleness, ranked by listings held.

THE PROBLEM THIS SOLVES
A source can keep reporting "success" (job exits 0, rows exist) while its
crawl has silently stopped completing. tworld.com sat at ~977 rows from 16
Jul until 2 Sep with nothing flagging it. vestedbb.com — the second-largest
source at ~3,586 listings — hasn't updated since 23 Aug. Neither failure
looked like a failure: the rows were already there from earlier runs, so
every downstream check that only asks "does this broker have listings?"
kept saying yes.

The check that catches this is different: not "does data exist" but "is the
newest row recent." A large source whose newest row is days old is not
refreshing, whether or not last night's job printed success.

WHAT IT DOES
For every broker_domain with at least one status='active' row in
listings_direct: count of active rows held, and the age in days of its
single newest row (MAX(last_seen)). Flags any source older than
--stale-days (default 3), sorted by rows held — a large stale source is a
much bigger problem than a small one.

Usage:
    python3 scrapers/staleness_monitor.py
    python3 scrapers/staleness_monitor.py --stale-days 5
    python3 scrapers/staleness_monitor.py --min-listings 10 --csv out.csv
"""
import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def sb_headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}


def fetch_active_rows():
    """broker_domain + last_seen for every active row, paginated."""
    rows, offset, page = [], 0, 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/listings_direct",
            headers=sb_headers(),
            params={
                "select": "broker_domain,last_seen",
                "status": "eq.active",
                "limit": str(page),
                "offset": str(offset),
            },
            timeout=90,
        )
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--stale-days", type=int, default=3,
                     help="flag sources whose newest row is older than this many days")
    ap.add_argument("--min-listings", type=int, default=1,
                     help="ignore sources holding fewer than this many active rows")
    ap.add_argument("--csv", help="write the full per-source table here")
    args = ap.parse_args()

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
        sys.exit(2)

    print("Fetching active listings_direct rows...")
    rows = fetch_active_rows()
    print(f"{len(rows)} active rows across all sources\n")

    per_domain = defaultdict(lambda: {"count": 0, "newest": None})
    for r in rows:
        d = per_domain[r["broker_domain"]]
        d["count"] += 1
        ls = r.get("last_seen")
        if ls and (d["newest"] is None or ls > d["newest"]):
            d["newest"] = ls

    now = datetime.now(timezone.utc)
    table = []
    for domain, d in per_domain.items():
        if d["count"] < args.min_listings:
            continue
        if d["newest"]:
            newest_dt = datetime.fromisoformat(d["newest"].replace("Z", "+00:00"))
            age_days = (now - newest_dt).total_seconds() / 86400
        else:
            age_days = float("inf")
        table.append({
            "domain": domain,
            "active_listings": d["count"],
            "newest_last_seen": d["newest"],
            "age_days": round(age_days, 1),
            "stale": age_days > args.stale_days,
        })

    table.sort(key=lambda x: -x["active_listings"])

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["domain", "active_listings",
                                               "newest_last_seen", "age_days", "stale"])
            w.writeheader()
            w.writerows(table)
        print(f"Wrote {len(table)} rows to {args.csv}\n")

    stale = [t for t in table if t["stale"]]
    stale_listings = sum(t["active_listings"] for t in stale)
    total_listings = sum(t["active_listings"] for t in table)

    print(f"{'='*70}")
    print(f"STALENESS MONITOR  (flag: newest row > {args.stale_days} days old)")
    print(f"{'='*70}")
    print(f"{len(table)} sources, {total_listings} active listings total")
    print(f"{len(stale)} sources STALE, holding {stale_listings} listings "
          f"({stale_listings/total_listings*100:.1f}% of the index)\n")

    print(f"{'domain':<38} {'active':>8} {'age(d)':>8}  newest_last_seen")
    print("-" * 80)
    for t in table[:40]:
        flag = "  ⚠️ STALE" if t["stale"] else ""
        age = "never" if t["age_days"] == float("inf") else f"{t['age_days']:.1f}"
        print(f"{t['domain']:<38} {t['active_listings']:>8} {age:>8}  {t['newest_last_seen']}{flag}")

    if len(table) > 40:
        print(f"... and {len(table)-40} more (use --csv to see all)")


if __name__ == "__main__":
    main()
