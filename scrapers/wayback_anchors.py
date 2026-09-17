#!/usr/bin/env python3
"""
wayback_anchors.py — build DOM calibration anchors from Internet Archive captures.

WHY THIS EXISTS
---------------
dom_anchors has 28 entries and starts 2024-12-12, so every listing numbered
below 2,312,165 comes back "predates our calibration". The Archive has been
capturing marketplace listing pages for years, and each capture carries both
the listing number (in the URL) and the capture date.

WHAT A CAPTURE PROVES
---------------------
That the listing EXISTED on the capture date. That is an UPPER BOUND on when
it was listed, never the listing date itself: a listing captured 2025-06-01
went up on or before that day. So these are written to wayback_anchors, not
dom_anchors, and any age computed from them is a floor — "at least this old" —
which is the same language the site already uses for listings that were
already up when we first crawled a broker.

THE MONOTONE SWEEP
------------------
Listing numbers are sequential, so if number N was live on date D, every
number below N was created before D. Sweeping from the highest number down and
carrying the running-minimum date propagates the tightest bound to every lower
number, which also cleans up captures of pages that sat archived for years.

USAGE
    python3 scrapers/wayback_anchors.py --from-year 2019 --to-year 2026
    python3 scrapers/wayback_anchors.py --dry-run          # print, write nothing
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime

import requests

CDX = "https://web.archive.org/cdx/search/cdx"

SITES = {
    # site key -> (cdx url pattern, regex capturing the listing number)
    "bizbuysell": ("bizbuysell.com/Business-Opportunity/*",
                   re.compile(r"/business-opportunity/[^/]+/(\d{6,8})/?", re.I)),
    "bizquest":   ("bizquest.com/business-for-sale/*",
                   re.compile(r"/business-for-sale/[^/]+/BW(\d{6,8})/?", re.I)),
}

HEADERS = {"User-Agent": "DealLedger anchor builder (dealledger.org; info@dealledger.org)"}


def _get(params, retries=5):
    """
    One CDX request, with the backoff the Archive expects.

    Their rate limiter answers a burst of large queries with 504s and then
    refuses connections outright (run 2026-09-17: 2019 returned 25,391 rows,
    2020 came back with 187, and by 2025 the host stopped answering). Slow and
    paged finishes; fast and greedy gets cut off with a plausible-looking but
    wrong result, which is worse than failing.
    """
    delay = 30
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(CDX, params=params, headers=HEADERS, timeout=300)
            if r.status_code == 200:
                return r.text
            print(f"      HTTP {r.status_code} (attempt {attempt}/{retries}), waiting {delay}s")
        except Exception as exc:                       # noqa: BLE001
            print(f"      {type(exc).__name__} (attempt {attempt}/{retries}), waiting {delay}s")
        time.sleep(delay)
        delay = min(delay * 2, 300)
    return None


def fetch_year(pattern, year, pause):
    """
    All captures for one year, page by page.

    CDX returns page counts for a query when asked, and serves fixed-size
    pages — the documented way to pull a large result set without asking the
    index to materialise it all at once.
    """
    base = {
        "url": pattern,
        "matchType": "prefix",
        "output": "json",
        "fl": "original,timestamp",
        "filter": "statuscode:200",
        "collapse": "urlkey",
        "from": str(year),
        "to": str(year),
        "pageSize": "5",
    }

    meta = _get(dict(base, showNumPages="true"))
    if meta is None:
        print(f"   {year}: could not read page count — skipping")
        return []
    try:
        pages = int(meta.strip() or 0)
    except ValueError:
        pages = 0
    if pages == 0:
        print(f"   {year}: no pages")
        return []

    rows = []
    for page in range(pages):
        text = _get(dict(base, page=str(page)))
        if text is None:
            print(f"   {year}: page {page + 1}/{pages} failed — keeping what we have")
            break
        text = text.strip()
        if text:
            try:
                chunk = json.loads(text)
            except json.JSONDecodeError:
                print(f"   {year}: page {page + 1} was not JSON — skipping")
                chunk = []
            if chunk and chunk[0][:1] == ["original"]:
                chunk = chunk[1:]
            rows.extend(chunk)
        print(f"   {year}: page {page + 1}/{pages}, {len(rows)} rows so far")
        time.sleep(pause)
    return rows


def collect(from_year, to_year, pause, writer=None):
    """{listing_number: {'site':…, 'earliest': 'YYYY-MM-DD', 'captures': n}}"""
    found = {}
    for site, (pattern, number_re) in SITES.items():
        for year in range(from_year, to_year + 1):
            rows = fetch_year(pattern, year, pause)
            hits = 0
            for row in rows:
                if len(row) < 2:
                    continue
                url, stamp = row[0], row[1]
                m = number_re.search(url)
                if not m or len(stamp) < 8:
                    continue
                ln = int(m.group(1))
                day = f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}"
                rec = found.get(ln)
                if rec is None:
                    found[ln] = {"site": site, "earliest": day, "captures": 1}
                else:
                    rec["captures"] += 1
                    if day < rec["earliest"]:
                        rec["earliest"] = day
                hits += 1
            print(f"{site} {year}: {len(rows)} captures, {hits} with a listing number")
            # Checkpoint after every year, so a run cut short by the rate
            # limiter still banks what it collected.
            if writer and found:
                writer(sweep(found))
            time.sleep(pause)
    return found


def sweep(found):
    """Highest number down, carrying the running-minimum date (see docstring)."""
    out = []
    running = None
    for ln in sorted(found, reverse=True):
        rec = found[ln]
        running = rec["earliest"] if running is None else min(running, rec["earliest"])
        out.append({
            "listing_number": ln,
            "site": rec["site"],
            "earliest_capture": rec["earliest"],
            "bound_date": running,
            "captures": rec["captures"],
        })
    out.reverse()
    return out


def write(rows, url, key, chunk=500, quiet=False):
    endpoint = f"{url.rstrip('/')}/rest/v1/wayback_anchors"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }
    written = 0
    for i in range(0, len(rows), chunk):
        batch = rows[i:i + chunk]
        r = requests.post(endpoint, headers=headers, data=json.dumps(batch), timeout=120)
        if r.status_code >= 300:
            print(f"❌ write failed at row {i}: HTTP {r.status_code} {r.text[:300]}")
            return written
        written += len(batch)
        if not quiet:
            print(f"   wrote {written}/{len(rows)}")
    return written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--from-year", type=int, default=2019)
    ap.add_argument("--to-year", type=int, default=datetime.now().year)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--pause", type=float, default=12.0,
                    help="Seconds between Archive requests (default 12)")
    args = ap.parse_args()

    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY")
    checkpoint = None
    if not args.dry_run and url and key:
        checkpoint = lambda rows: write(rows, url, key, quiet=True)

    print(f"Collecting Archive captures {args.from_year}–{args.to_year} "
          f"({args.pause}s between requests)…")
    found = collect(args.from_year, args.to_year, args.pause, checkpoint)
    if not found:
        print("No captures found — nothing written.")
        return 1

    rows = sweep(found)
    lo, hi = rows[0], rows[-1]
    print(f"\n{len(rows)} distinct listing numbers")
    print(f"   lowest  {lo['listing_number']} bounded at {lo['bound_date']}")
    print(f"   highest {hi['listing_number']} bounded at {hi['bound_date']}")

    if args.dry_run:
        for r in rows[:: max(1, len(rows) // 25)]:
            print(f"   {r['listing_number']:>9}  ≤ {r['bound_date']}  ({r['site']})")
        return 0

    if not (url and key):
        print("SUPABASE_URL / SUPABASE_SERVICE_KEY not set — printing only.")
        return 1
    n = write(rows, url, key)
    print(f"\n✅ {n} anchors written to wayback_anchors.")
    print("   Compare against the trusted anchors with: select * from v_anchor_check;")
    return 0


if __name__ == "__main__":
    sys.exit(main())
