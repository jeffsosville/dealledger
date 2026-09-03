#!/usr/bin/env python3
"""
export_discovered_ok.py — append broker_discovery status='ok' rows to
data/brokers_clean.csv, deduping against what's already there.

Written for the discover_backlog.py follow-up: once discovery has resolved a
domain's listings_url, the daily scraper only picks it up if it's in the CSV
brokers.csv reads from. This appends id,name,company,url in the same shape as
the existing file, skipping any URL (by domain) already present.

Usage:
  python3 agents/export_discovered_ok.py
"""
import csv
import os
import requests
from urllib.parse import urlparse

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                         "data", "brokers_clean.csv")


def sb_headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}


def fetch_ok_rows():
    rows, offset = [], 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/broker_discovery",
            headers=sb_headers(),
            params={"select": "domain,listings_url,url,notes", "status": "eq.ok",
                    "limit": "1000", "offset": str(offset)},
            timeout=60,
        )
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < 1000:
            break
        offset += 1000
    return rows


def existing_domains():
    domains = set()
    if not os.path.exists(CSV_PATH):
        return domains
    with open(CSV_PATH, newline="") as f:
        for row in csv.reader(f):
            for cell in row:
                if cell.startswith("http"):
                    d = urlparse(cell).netloc.lower()
                    bare = d[4:] if d.startswith("www.") else d
                    domains.add(bare)
    return domains


def next_id(existing_ids):
    return (max(existing_ids) + 1) if existing_ids else 1


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")

    ok_rows = fetch_ok_rows()
    print(f"{len(ok_rows)} status='ok' rows in broker_discovery")

    seen = existing_domains()
    print(f"{len(seen)} distinct domains already in {CSV_PATH}")

    # Figure out the next free numeric id by scanning the first column.
    existing_ids = []
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, newline="") as f:
            for row in csv.reader(f):
                if row and row[0].strip():
                    try:
                        existing_ids.append(int(float(row[0])))
                    except ValueError:
                        pass
    nid = next_id(existing_ids)

    to_add = []
    skipped_dupe = 0
    skipped_no_url = 0
    rescued_from_url_col = 0
    for row in ok_rows:
        domain = (row.get("domain") or "").lower()
        bare = domain[4:] if domain.startswith("www.") else domain
        if not bare or bare in seen:
            skipped_dupe += 1
            continue
        url = row.get("listings_url")
        if not url:
            # Some rows were written by a discovery path that only
            # populated `url`, never `listings_url` — status='ok' with a
            # real URL that this export silently dropped for good. Fall
            # back rather than lose them again.
            url = row.get("url")
            if url:
                rescued_from_url_col += 1
        if not url:
            skipped_no_url += 1
            continue
        name = row.get("notes") or bare
        to_add.append([nid, name, name, url])
        seen.add(bare)
        nid += 1

    print(f"{skipped_dupe} already present, {len(to_add)} new rows to append "
          f"({rescued_from_url_col} rescued from the url column), "
          f"{skipped_no_url} status='ok' with no URL in either column")

    if to_add:
        with open(CSV_PATH, "a", newline="") as f:
            w = csv.writer(f)
            w.writerows(to_add)
        print(f"Appended {len(to_add)} rows to {CSV_PATH}")
    else:
        print("Nothing to append.")


if __name__ == "__main__":
    main()
