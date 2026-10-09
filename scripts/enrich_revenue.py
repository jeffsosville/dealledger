#!/usr/bin/env python3
"""
enrich_revenue.py - fill revenue (and missing cash flow) from listing detail pages.

WHY
  Five franchise feeds carry no revenue on their index pages or APIs, so
  ~11,500 public listings (tworld, vestedbb, execbb, hedgestone, murphy) sit
  at 0% revenue. The number is only on each listing's detail page.

HOW
  - Picks active public listings from those domains with revenue IS NULL and
    detail_checked_at IS NULL. Each listing's page is opened ONCE: success or
    "no revenue on page", it gets detail_checked_at and is never re-fetched.
    Fetch errors (403, timeouts) are NOT marked, so they retry next run.
  - One label-based extractor for every site ("Gross Revenue: $1,200,000",
    "Gross Sales $850K", "Annual Revenue ... $2.1M") instead of five
    site-specific scrapers. Run DRY_RUN=1 first: it prints the hit rate and
    sample values per domain so a bad pattern shows before anything is written.
  - Only fills NULLs. Never overwrites a revenue or cash flow already present.

Env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY   required
  DOMAINS     comma list (default: the five feeds)
  LIMIT       listings per domain this run (default 20)
  DRY_RUN     1 = fetch and report, write nothing (default 1)
  DELAY       seconds between requests (default 1.5)

  DRY_RUN=1 LIMIT=10 python3 scripts/enrich_revenue.py
  DRY_RUN=0 LIMIT=500 python3 scripts/enrich_revenue.py
"""
import os
import re
import sys
import time
from datetime import datetime, timezone

import requests

try:
    from curl_cffi import requests as curl_requests
except ImportError:
    curl_requests = None

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
DOMAINS = [d.strip() for d in os.environ.get(
    "DOMAINS", "tworld.com,vestedbb.com,execbb.com,hedgestone.com,murphybusiness.com"
).split(",") if d.strip()]
LIMIT = int(os.environ.get("LIMIT", "20"))
DRY_RUN = os.environ.get("DRY_RUN", "1") == "1"
DELAY = float(os.environ.get("DELAY", "1.5"))

# Most specific labels first; the bare "revenue"/"sales" fall back last.
REVENUE_LABELS = [
    r"gross\s+revenues?", r"gross\s+sales", r"annual\s+(?:gross\s+)?(?:revenues?|sales)",
    r"total\s+(?:revenues?|sales)", r"(?:ttm|trailing)\s+(?:revenues?|sales)",
    r"revenues?", r"sales",
]
CASH_FLOW_LABELS = [
    r"cash\s*flow", r"seller'?s?\s+discretionary\s+earnings", r"\bsde\b",
    r"owner'?s?\s+benefit", r"discretionary\s+earnings", r"adjusted\s+net",
]
_NOT_DISCLOSED = re.compile(r"not\s+disclosed|undisclosed|upon\s+request|n/a|tbd", re.I)


def _to_number(num, unit):
    try:
        v = float(num.replace(",", ""))
    except ValueError:
        return None
    unit = (unit or "").lower()
    if unit in ("k", "thousand"):
        v *= 1_000
    elif unit in ("m", "mm", "million"):
        v *= 1_000_000
    return v


def page_text(html):
    html = re.sub(r"(?is)<(script|style|noscript)[^>]*>.*?</\1>", " ", html)
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;|&#160;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    return re.sub(r"\s+", " ", text)


# Bare "revenue"/"sales" also appear in prose ("Sales tax included. Price
# $250,000"), so for those the amount must follow almost immediately.
_LOOSE = {r"revenues?", r"sales"}


def find_amount(text, labels, lo=10_000, hi=5_000_000_000):
    """First plausible dollar amount right after a label."""
    for label in labels:
        for m in re.finditer(label, text, re.I):
            window = text[m.end(): m.end() + 60]
            if _NOT_DISCLOSED.match(window.strip(" :-")):
                break
            if label in _LOOSE and not re.match(r"^[\s:\-–—|]*\$", window):
                continue
            # Require a dollar sign: "Revenue 2024: $1.2M" must not read 2024.
            mm = re.search(r"\$\s*([\d][\d,]*(?:\.\d+)?)\s*(k|m|mm|million|thousand)?\b",
                           window[:50], re.I)
            if not mm:
                continue
            v = _to_number(mm.group(1), mm.group(2))
            if v and lo <= v <= hi:
                return v
    return None


def extract(html):
    text = page_text(html)
    return {"revenue": find_amount(text, REVENUE_LABELS),
            "cash_flow": find_amount(text, CASH_FLOW_LABELS, lo=1_000)}


def sb(method, path, **kw):
    h = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
         "Content-Type": "application/json", "Prefer": "return=minimal"}
    r = requests.request(method, f"{SUPABASE_URL}/rest/v1/{path}", headers=h, timeout=60, **kw)
    r.raise_for_status()
    return r.json() if r.text else None


def candidates(domain):
    return sb("GET", "listings_direct", params={
        "select": "id,url,cash_flow,asking_price",
        "broker_domain": f"in.({domain},www.{domain})",
        "status": "eq.active", "public": "is.true",
        "revenue": "is.null", "detail_checked_at": "is.null",
        "order": "first_seen.desc", "limit": str(LIMIT)})


def session():
    if curl_requests:
        return curl_requests.Session(impersonate="chrome124")
    s = requests.Session()
    s.headers["User-Agent"] = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                               "AppleWebKit/537.36 Chrome/124.0 Safari/537.36")
    return s


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        sys.exit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    print(f"enrich_revenue  dry_run={DRY_RUN}  limit/domain={LIMIT}  domains={DOMAINS}")
    s = session()
    totals = {}
    for domain in DOMAINS:
        rows = candidates(domain)
        stats = {"fetched": 0, "errors": 0, "revenue": 0, "cash_flow": 0}
        samples = []
        for row in rows:
            try:
                r = s.get(row["url"], timeout=30)
            except Exception as e:
                stats["errors"] += 1
                print(f"  ! {domain} {row['url'][:80]}  {type(e).__name__}")
                time.sleep(DELAY)
                continue
            if r.status_code != 200:
                stats["errors"] += 1
                print(f"  ! {domain} HTTP {r.status_code}  {row['url'][:80]}")
                time.sleep(DELAY)
                continue
            stats["fetched"] += 1
            got = extract(r.text)
            patch = {"detail_checked_at": datetime.now(timezone.utc).isoformat()}
            if got["revenue"]:
                patch["revenue"] = int(got["revenue"])
                stats["revenue"] += 1
            # The cash-flow label can sit next to the asking price ("SDE 149K
            # | $199,000"): a value equal to the price is the price, not cash flow.
            price = row.get("asking_price")
            if got["cash_flow"] and price and abs(got["cash_flow"] - float(price)) < 1:
                got["cash_flow"] = None
            if got["cash_flow"] and row.get("cash_flow") is None:
                patch["cash_flow"] = int(got["cash_flow"])
                stats["cash_flow"] += 1
            if len(samples) < 5:
                samples.append((row["url"], row.get("asking_price"), got["revenue"], got["cash_flow"]))
            if not DRY_RUN:
                sb("PATCH", "listings_direct",
                   params={"id": f"eq.{row['id']}", "revenue": "is.null"}, json=patch)
            time.sleep(DELAY)

        totals[domain] = stats
        f = stats["fetched"] or 1
        print(f"\n{domain}: {len(rows)} candidates, {stats['fetched']} fetched, "
              f"{stats['errors']} errors, revenue found {stats['revenue']} "
              f"({100 * stats['revenue'] // f}%), new cash flow {stats['cash_flow']}")
        for url, price, rev, cf in samples:
            print(f"    price={price}  revenue={rev}  cash_flow={cf}  {url[:90]}")

    print("\nDRY RUN - nothing written" if DRY_RUN else "\nWritten.")


if __name__ == "__main__":
    main()
