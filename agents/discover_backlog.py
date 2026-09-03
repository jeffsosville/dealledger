#!/usr/bin/env python3
"""
discover_backlog.py — turn known-but-uncrawled broker domains into crawlable
listings URLs.

THE PROBLEM THIS SOLVES
`broker_master` holds ~1,505 distinct broker company domains, scraped from
broker profiles. Only ~334 of them appear in `listings_direct`. The other
~1,171 have never been fetched, and they carry roughly 10,000 active listings
between them — about a 25% increase on the whole index, from brokers we
already know about.

They are not in the crawl list because `broker_master.companyurl` is a
HOMEPAGE, and the scraper needs a listings INDEX page. That gap is this script.

For each domain: resolve the listings page, fingerprint it via discovery_v2,
and record the result in `broker_discovery`. Anything that resolves cleanly
can then be appended to data/brokers_clean.csv.

Discovery is a once-per-domain cost. The cache means a domain is never
re-probed unless you ask for it.

Env:
  SUPABASE_URL, SUPABASE_SERVICE_KEY   required
  ANTHROPIC_API_KEY                    optional, for the Haiku fallback
  LIMIT                                domains per run, default 100
  RETRY_FAILED=1                       re-probe domains that previously failed
  DRY_RUN=1                            probe and report, write nothing

Usage:
  LIMIT=50 python agents/discover_backlog.py
"""

import os
import signal
import sys
import time
from contextlib import contextmanager
from urllib.parse import urlparse

import requests

# Per-domain hard budget. discovery_v2's own timeouts (page.goto, networkidle
# waits) are generous and additive across several fallback steps — on 1 Sep
# 2026 a single domain (access-re.com) hung for 15+ minutes with the process
# asleep on a chromium launch under memory pressure, stalling the entire
# 200-domain batch with no error, no log line, nothing. A batch job that can
# be silently parked forever by one bad site isn't a batch job.
DOMAIN_TIMEOUT_SECS = int(os.environ.get("DOMAIN_TIMEOUT_SECS", "90"))


class DomainTimeout(Exception):
    pass


@contextmanager
def domain_watchdog(seconds):
    def _raise(signum, frame):
        raise DomainTimeout(f"exceeded {seconds}s")
    old = signal.signal(signal.SIGALRM, _raise)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
LIMIT = int(os.environ.get("LIMIT", "100"))
RETRY_FAILED = os.environ.get("RETRY_FAILED", "") == "1"
DRY_RUN = os.environ.get("DRY_RUN", "") == "1"

# Statuses that mean "don't bother again unless explicitly asked"
TERMINAL = {"ok", "dead", "auth_required", "no_listings_page"}


def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def candidates(limit: int):
    """Highest-yield uncrawled domains first.

    Pages through the WHOLE candidate view, not just the top `limit * 3` by
    priority_score. That fixed-size over-fetch used to mean: once every
    domain in the top slice had a broker_discovery row (attempted, even if
    failed), every subsequent run re-fetched the exact same top slice, found
    nothing new, and the ~445 lower-priority domains below that window were
    permanently unreachable — "run until none remain" would never converge.
    `limit` still caps how many NEW domains this run queues; it no longer
    caps how much of the backlog is even considered.
    """
    rows, offset, page = [], 0, 1000
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/v_broker_crawl_candidates",
            headers=sb_headers(),
            params={
                "select": "domain,company,active_listings,sold_last_6mo,priority_score",
                "crawl_state": "eq.never_crawled",
                "active_listings": "gt.0",
                # domain as a tiebreaker: OFFSET pagination over ties on
                # priority_score alone isn't guaranteed stable, and rows with
                # tied scores are common at the low-priority tail — without
                # this some domains were silently skipped between pages.
                "order": "priority_score.desc,domain.asc",
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


def already_done():
    """Domains we've already resolved, so we don't pay for them twice."""
    seen = {}
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/broker_discovery",
            headers=sb_headers(),
            params={"select": "domain,status", "limit": "1000",
                    "offset": str(offset)},
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for row in rows:
            seen[row["domain"]] = row.get("status")
        if len(rows) < 1000:
            break
        offset += 1000
    return seen


def save(domain, base_url, listings_url, status, method=None,
         platform=None, selector=None, listings_found=None, notes=None):
    if DRY_RUN:
        return
    payload = {
        "domain": domain,
        "base_url": base_url,
        "listings_url": listings_url,
        "status": status,
        "method": method,
        "platform": platform,
        "selector": selector,
        "listings_found": listings_found,
        "notes": notes,
        "last_attempt_at": "now()",
    }
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/broker_discovery",
        headers={**sb_headers(),
                 "Prefer": "resolution=merge-duplicates,return=minimal"},
        json=payload,
        timeout=60,
    )
    if not r.ok:
        print(f"    save failed: {r.status_code} {r.text[:200]}")


def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
        sys.exit(2)

    try:
        import discovery_v2 as dv
    except ImportError as exc:
        print(f"Could not import discovery_v2: {exc}")
        print("Run this from the agents/ directory, or with the repo root on PYTHONPATH.")
        sys.exit(2)

    seen = already_done()
    print(f"Already in broker_discovery: {len(seen)}")

    pool = candidates(LIMIT)
    queue = []
    for row in pool:
        st = seen.get(row["domain"])
        if st in TERMINAL and not RETRY_FAILED:
            continue
        queue.append(row)
        if len(queue) >= LIMIT:
            break

    print(f"Probing {len(queue)} domains"
          f"{'  (DRY RUN)' if DRY_RUN else ''}\n")

    tally = {}
    crawlable = []
    weak = []

    for i, row in enumerate(queue, 1):
        domain = row["domain"]
        base = f"https://{domain}"
        listed = row.get("active_listings") or 0
        print(f"[{i}/{len(queue)}] {domain}  ({listed} listings claimed)")

        try:
            with domain_watchdog(DOMAIN_TIMEOUT_SECS):
                listings_url = dv.find_listings_page(base, verbose=True)
        except Exception as exc:
            print(f"    probe error: {exc}")
            tally["error"] = tally.get("error", 0) + 1
            save(domain, base, None, "dead", notes=str(exc)[:300])
            continue

        if not listings_url:
            print("    no listings page found")
            tally["no_listings_page"] = tally.get("no_listings_page", 0) + 1
            save(domain, base, None, "no_listings_page")
            time.sleep(1.0)
            continue

        print(f"    listings page: {listings_url}")

        # Fingerprint it so the scraper knows how to read it.
        method = platform = selector = None
        found = None
        status = "ok"
        try:
            with domain_watchdog(DOMAIN_TIMEOUT_SECS):
                result = dv.discover(listings_url) if hasattr(dv, "discover") else None
            if isinstance(result, dict):
                method = result.get("method")
                platform = result.get("platform")
                selector = (result.get("selector") or result.get("container"))
                found = (result.get("sample_count")
                         or result.get("item_count")
                         or result.get("listings_found"))
                status = result.get("status") or "ok"
        except Exception as exc:
            print(f"    fingerprint failed (page still usable): {exc}")

        # Does the yield make sense against what this broker claims to have?
        #
        # discovery_v2 returns on the FIRST method that produces anything, so a
        # JSON-LD block holding one Organization record beats a real listing
        # grid it never got to. routeconsultant.com claims 211 listings and
        # "succeeded" with 1 item; eastcoastbusinessbrokers claims 113 and
        # "succeeded" with 2, at high confidence.
        #
        # Mark those weak rather than ok. A weak row still records what we
        # learned, but it does not go into the crawl list pretending to work.
        if status == "ok":
            got = int(found or 0)
            if got == 0:
                status = "weak"
                print(f"    weak: method returned no items")
            elif listed >= 20 and got < max(5, listed * 0.10):
                status = "weak"
                print(f"    weak: got {got} items, broker claims {listed}")
            elif got < 3:
                status = "weak"
                print(f"    weak: only {got} items found")

        save(domain, base, listings_url, status, method, platform,
             selector, found, notes=row.get("company"))
        tally[status] = tally.get(status, 0) + 1
        if status == "ok":
            crawlable.append((domain, listings_url, listed))
        elif status == "weak":
            # The listings page is real even if extraction was poor. Worth
            # crawling with the generic scraper, which may do better than
            # whatever discovery settled on first.
            weak.append((domain, listings_url, listed))

        time.sleep(1.2)   # be a decent guest

    print("\n--- results ---")
    for k, v in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:20} {v}")

    if crawlable:
        print(f"\n--- {len(crawlable)} ready for data/brokers_clean.csv ---")
        for domain, url, listed in sorted(crawlable, key=lambda x: -x[2])[:40]:
            print(f"  {listed:5}  {url}")

    if weak:
        print(f"\n--- {len(weak)} found a listings page but extracted poorly ---")
        print("    (worth crawling anyway - the generic scraper may beat discovery)")
        for domain, url, listed in sorted(weak, key=lambda x: -x[2])[:20]:
            print(f"  {listed:5}  {url}")

    print("\nExport the full crawl-ready set with:")
    print("  select domain, listings_url from broker_discovery where status='ok';")


if __name__ == "__main__":
    main()
