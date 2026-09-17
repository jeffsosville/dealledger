#!/usr/bin/env python3
"""
archive_coverage.py — can the Internet Archive date our floor-dated listings?

THE PROBLEM
-----------
~6,500 live listings are floors: already on the broker's site the first day we
crawled it (2026-03-24 for most), so all we can honestly say is "at least 175
days". Their real age is unknown and almost certainly larger.

THE IDEA
--------
The Archive has been capturing broker sites for years. Where it captured one
of those exact listing URLs before our first crawl, the capture proves the
listing existed then — turning "at least 175 days" into "at least 600".

THIS SCRIPT MEASURES WHETHER THAT IS WORTH BUILDING.
It is read-only: no writes to Supabase, no changes to any listing. For the top
N broker domains by floor count it asks the Archive what it holds for that
domain before our first crawl, matches captures against our URLs, and reports
what the improvement would be. If coverage is a few percent, drop the idea.
If it is a third, it is the best day of work available.

    python3 scrapers/archive_coverage.py --domains 25
"""

import argparse
import json
import os
import re
import sys
import time
from collections import defaultdict

import requests

CDX = "https://web.archive.org/cdx/search/cdx"
HEADERS = {"User-Agent": "DealLedger coverage check (dealledger.org; info@dealledger.org)"}


def norm(u):
    """Same normalisation the lookup uses, so matches are apples to apples."""
    u = (u or "").split("#")[0]
    u = re.sub(r"^https?://(www\.)?", "", u, flags=re.I)
    return u.rstrip("/").lower()


def supabase(url, key, path, params):
    r = requests.get(f"{url.rstrip('/')}/rest/v1/{path}", params=params, timeout=120,
                     headers={"apikey": key, "Authorization": f"Bearer {key}"})
    r.raise_for_status()
    return r.json()


def floors_by_domain(url, key):
    """Floor-dated live listings, grouped by broker domain."""
    rows, start, page = [], 0, 1000
    while True:
        chunk = supabase(url, key, "mv_listings_page", {
            "select": "source_url,estimated_listed_date,dom_days_eff",
            "source": "eq.broker_direct",
            "dom_basis": "eq.floor",
            "offset": start, "limit": page,
        })
        rows.extend(chunk)
        if len(chunk) < page:
            break
        start += page
    by_domain = defaultdict(list)
    for r in rows:
        u = norm(r.get("source_url"))
        if u:
            by_domain[u.split("/")[0]].append((u, r.get("estimated_listed_date")))
    return by_domain


def archived_before(domain, cutoff, pause):
    """{normalised url: earliest capture date} for captures before cutoff."""
    params = {
        "url": f"{domain}/*",
        "matchType": "prefix",
        "output": "json",
        "fl": "original,timestamp,statuscode",
        "to": cutoff.replace("-", ""),
        "pageSize": "5",
    }
    meta = requests.get(CDX, params=dict(params, showNumPages="true"),
                        headers=HEADERS, timeout=180)
    try:
        pages = int((meta.text or "0").strip())
    except ValueError:
        pages = 0
    if pages == 0:
        return {}

    found = {}
    for p in range(min(pages, 40)):           # 40 pages is plenty to judge coverage
        try:
            r = requests.get(CDX, params=dict(params, page=str(p)),
                             headers=HEADERS, timeout=180)
            rows = json.loads(r.text) if r.text.strip() else []
        except Exception:                      # noqa: BLE001
            break
        if rows and rows[0][:1] == ["original"]:
            rows = rows[1:]
        for row in rows:
            if len(row) < 2 or (len(row) > 2 and row[2] != "200"):
                continue
            u, stamp = norm(row[0]), row[1]
            day = f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}"
            if u not in found or day < found[u]:
                found[u] = day
        time.sleep(pause)
    return found


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--domains", type=int, default=25)
    ap.add_argument("--cutoff", default="2026-03-24", help="our first crawl")
    ap.add_argument("--pause", type=float, default=8.0)
    args = ap.parse_args()

    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY")
    if not (url and key):
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY required")
        return 1

    by_domain = floors_by_domain(url, key)
    total_floors = sum(len(v) for v in by_domain.values())
    ranked = sorted(by_domain.items(), key=lambda kv: -len(kv[1]))[:args.domains]
    print(f"{total_floors:,} floor-dated listings across {len(by_domain)} domains; "
          f"checking the top {len(ranked)}\n")
    print(f"{'domain':38} {'floors':>7} {'archived':>9} {'matched':>8} {'%':>5}  oldest match")
    print("-" * 88)

    tot_f = tot_m = 0
    for domain, listings in ranked:
        captures = archived_before(domain, args.cutoff, args.pause)
        ours = dict(listings)
        matched = {u: d for u, d in captures.items() if u in ours}
        oldest = min(matched.values()) if matched else "—"
        pct = round(100 * len(matched) / max(len(listings), 1))
        tot_f += len(listings); tot_m += len(matched)
        print(f"{domain[:38]:38} {len(listings):>7} {len(captures):>9} "
              f"{len(matched):>8} {pct:>4}%  {oldest}")
        time.sleep(args.pause)

    print("-" * 88)
    print(f"{'TOTAL (sampled domains)':38} {tot_f:>7} {'':>9} {tot_m:>8} "
          f"{round(100 * tot_m / max(tot_f, 1)):>4}%")
    print("\nA few percent: not worth building. A third: build it — those listings"
          "\nstop saying 'at least 175 days' and start saying what they really are.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
