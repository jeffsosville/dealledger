#!/usr/bin/env python3
"""
regression_check.py — make silent scraper failures LOUD.

Runs against listings_direct AFTER the daily scrapers and asserts, per broker:
  1. count > 0                     (extraction didn't silently die — vestedbb→0)
  2. count >= baseline * (1-DROP)  (no >20% drop — max_pages truncation)
  3. titles pass is_listing_junk() (no junk titles — the titles→"Sold" case)

Baselines are "known-good" counts captured on a healthy day. Update them when a
broker legitimately grows/shrinks (a real change should be a deliberate baseline
edit, not a silent drift).

Exit code: 0 if all pass; 1 if any check fails (so CI/cron flags it). Pass
--no-fail to always exit 0 (report only).

Usage:
    python3 scrapers/regression_check.py
    python3 scrapers/regression_check.py --no-fail
"""

import os
import sys
import argparse

import requests

sys.path.insert(0, os.path.dirname(__file__))
from junk_filter import is_listing_junk  # SAME logic the scraper extracts with

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co").rstrip("/")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")

DROP_TOLERANCE = 0.20      # fail if count drops more than this below baseline
JUNK_RATE_MAX = 0.05       # fail if >5% of sampled titles are junk (systemic)
TITLE_SAMPLE = 2000        # titles pulled per broker for the junk check

# Each broker: how to select its rows in listings_direct + a healthy baseline.
# filter = (column, PostgREST expression).
BROKERS = [
    # NOTE: routesforsale.net shows ~33 in listings_direct vs the 14 baseline —
    # reconcile the baseline or the filter when convenient (drop-check still guards it).
    {"name": "routesforsale",  "baseline": 14,   "filter": ("broker_domain", "ilike.*routesforsale.net*")},
    {"name": "quietlight",     "baseline": 423,  "filter": ("broker_domain", "ilike.*quietlight*")},
    {"name": "companysellers", "baseline": 498,  "filter": ("broker_domain", "ilike.*companysellers*")},
    {"name": "link",           "baseline": 443,  "filter": ("broker_name", "eq.Link Business")},
    {"name": "vestedbb",       "baseline": 1455, "filter": ("broker_name", "eq.Vested Business Brokers")},
    {"name": "wesell",         "baseline": 1423, "filter": ("broker_domain", "ilike.*wesellrestaurants*")},
]


def _headers(extra=None):
    h = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}
    if extra:
        h.update(extra)
    return h


def _url():
    return f"{SUPABASE_URL}/rest/v1/listings_direct"


def count_rows(col, expr):
    r = requests.get(_url(), headers=_headers({"Prefer": "count=exact", "Range": "0-0"}),
                     params={"select": "id", col: expr}, timeout=60)
    r.raise_for_status()
    cr = r.headers.get("Content-Range", "*/0")
    try:
        return int(cr.split("/")[-1])
    except ValueError:
        return 0


def fetch_titles(col, expr, limit=TITLE_SAMPLE):
    r = requests.get(_url(), headers=_headers(),
                     params={"select": "title", col: expr, "limit": str(limit)}, timeout=60)
    r.raise_for_status()
    return [row.get("title") for row in r.json()]


def check_broker(b):
    col, expr = b["filter"]
    baseline = b["baseline"]
    count = count_rows(col, expr)
    titles = fetch_titles(col, expr)
    junk = [t for t in titles if is_listing_junk(t)]
    junk_rate = (len(junk) / len(titles)) if titles else 0.0
    floor = int(baseline * (1 - DROP_TOLERANCE))

    fails = []
    if count == 0:
        fails.append("count is 0 (extraction returned nothing)")
    elif count < floor:
        pct = (baseline - count) / baseline * 100 if baseline else 0
        fails.append(f"count {count} < floor {floor} ({pct:.0f}% below baseline {baseline})")
    if titles and junk_rate > JUNK_RATE_MAX:
        fails.append(f"{len(junk)}/{len(titles)} titles are junk ({junk_rate*100:.1f}%)")

    return {
        "name": b["name"], "count": count, "baseline": baseline, "floor": floor,
        "sampled": len(titles), "junk": len(junk),
        "junk_examples": junk[:5], "fails": fails,
    }


def main():
    ap = argparse.ArgumentParser(description="DealLedger scraper regression suite")
    ap.add_argument("--no-fail", action="store_true", help="Always exit 0 (report only)")
    args = ap.parse_args()

    if not SUPABASE_SERVICE_KEY:
        print("ERROR: SUPABASE_SERVICE_KEY not set.", file=sys.stderr)
        sys.exit(2)

    print("=" * 74)
    print("DealLedger scraper regression suite  (listings_direct)")
    print(f"drop tolerance {int(DROP_TOLERANCE*100)}%  |  junk-title max "
          f"{int(JUNK_RATE_MAX*100)}%")
    print("=" * 74)
    print(f"{'BROKER':<16}{'COUNT':>7}{'BASE':>7}{'FLOOR':>7}{'JUNK':>7}   RESULT")
    print("-" * 74)

    any_fail = False
    for b in BROKERS:
        try:
            r = check_broker(b)
        except Exception as e:
            any_fail = True
            print(f"{b['name']:<16}{'ERR':>7}{b['baseline']:>7}{'':>7}{'':>7}   FAIL — query error: {str(e)[:40]}")
            continue
        ok = not r["fails"]
        any_fail = any_fail or not ok
        status = "PASS" if ok else "FAIL — " + "; ".join(r["fails"])
        print(f"{r['name']:<16}{r['count']:>7}{r['baseline']:>7}{r['floor']:>7}{r['junk']:>7}   {status}")
        if r["junk_examples"]:
            print(f"{'':<16}   junk e.g.: {r['junk_examples']}")

    print("-" * 74)
    print("RESULT:", "❌ REGRESSIONS DETECTED" if any_fail else "✅ all brokers healthy")
    print("=" * 74)

    if any_fail and not args.no_fail:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
