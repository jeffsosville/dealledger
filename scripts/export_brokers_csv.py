#!/usr/bin/env python3
"""
Generate the V6 broker list from broker_sources at job start.

    python3 scripts/export_brokers_csv.py --out data/brokers_clean.csv

Replaces reading the committed data/brokers_clean.csv, which drifted from
broker_sources: discovery promoted brokers in the DB that the nightly crawl
never saw because nobody re-exported the CSV.

Selection: discovery_stage in ('3_crawlable', '4_producing').
Order:     oldest-seeded first, so the newest discoveries sit at the END —
           _order_by_staleness's NEW_BROKER_QUOTA walks the list in reverse
           to pick "newest never-scraped first", same as it did with the
           append-only CSV.

Exits non-zero (and writes nothing) on any error or a suspiciously small
result, so the workflow fails instead of crawling a truncated list.
"""
import argparse
import csv
import os
import sys

import requests

STAGES = ("3_crawlable", "4_producing")
MIN_ROWS = 200          # 2026-09-24: 1,111 rows in these stages


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/brokers_clean.csv")
    ap.add_argument("--min-rows", type=int, default=MIN_ROWS)
    args = ap.parse_args()

    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("❌ SUPABASE_URL / SUPABASE_SERVICE_KEY not set", file=sys.stderr)
        return 1
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}

    rows, start, page = [], 0, 1000
    while True:
        r = requests.get(
            f"{url}/rest/v1/broker_sources",
            headers={**headers, "Range-Unit": "items", "Range": f"{start}-{start + page - 1}"},
            params={
                "select": "id,broker_name,company_name,domain,listing_url,homepage_url,discovery_stage",
                "discovery_stage": f"in.({','.join(STAGES)})",
                "order": "first_seeded_at.asc.nullsfirst,created_at.asc,id.asc",
            },
            timeout=60,
        )
        if not r.ok:
            print(f"❌ broker_sources HTTP {r.status_code}: {r.text[:300]}", file=sys.stderr)
            return 1
        batch = r.json()
        rows.extend(batch)
        if len(batch) < page:
            break
        start += page

    out, seen = [], set()
    for b in rows:
        link = (b.get("listing_url") or b.get("homepage_url") or "").strip()
        if not link.startswith("http") or link in seen:
            continue
        seen.add(link)
        name = (b.get("broker_name") or b.get("company_name") or b.get("domain") or "").strip()
        out.append({"broker_name": name, "listing_url": link, "domain": b.get("domain") or ""})

    by_stage = {s: sum(1 for b in rows if b.get("discovery_stage") == s) for s in STAGES}
    print(f"📋 broker_sources: {len(rows)} rows {by_stage} -> {len(out)} unique listing URLs")
    if len(out) < args.min_rows:
        print(f"❌ only {len(out)} brokers (< {args.min_rows}) — refusing to write a "
              f"truncated list", file=sys.stderr)
        return 1

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["broker_name", "listing_url", "domain"])
        w.writeheader()
        w.writerows(out)
    print(f"✅ wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
