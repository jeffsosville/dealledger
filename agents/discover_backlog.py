#!/usr/bin/env python3
"""
discover_backlog.py — turn known-but-uncrawled broker domains into crawlable
listings URLs.

SOURCES
  SOURCE=broker_sources (default) reads v_discovery_queue: the combined broker
  universe (IBBA and other seed lists), ranked by IBBA membership and firm size, not marketplace-derived
  counts, block list honoured. Results are written to broker_discovery AND back
  onto the broker_sources row (listing_url, discovery_stage, strategy_status),
  so the stage counts in v_broker_universe_summary stay true.
  SOURCE=broker_master is the original backlog described below (legacy).

THE PROBLEM THIS SOLVED (SOURCE=broker_master)
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
  SOURCE                               broker_sources (default) | broker_master
  SEED                                 only broker_sources rows whose seed_source
                                       contains this tag, e.g. SEED=ibba
  DRY_RUN=1                            probe and report, write nothing

Usage:
  SEED=ibba LIMIT=50 DRY_RUN=1 python agents/discover_backlog.py
  SEED=ibba LIMIT=50 python agents/discover_backlog.py
"""

import os
import signal
import sys
import time
from contextlib import contextmanager
from urllib.parse import urlparse

import re

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
SOURCE = os.environ.get("SOURCE", "broker_sources").strip()
SEED = os.environ.get("SEED", "").strip()
if SOURCE not in ("broker_sources", "broker_master"):
    sys.exit(f"SOURCE must be broker_sources or broker_master, got {SOURCE!r}")

# Statuses that mean "don't bother again unless explicitly asked"
TERMINAL = {"ok", "dead", "auth_required", "no_listings_page"}


def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def candidates(limit: int):
    if SOURCE == "broker_sources":
        return candidates_from_sources()
    return candidates_from_master(limit)


def candidates_from_sources():
    """v_discovery_queue: broker_sources rows still at 1_needs_discovery.

    Ranked by IBBA membership / firm size, never by marketplace counts, so
    active_listings is always 0 here and the yield check below falls back to
    its absolute floor (fewer than 3 items = weak).
    """
    rows, offset, page = [], 0, 1000
    params = {
        "select": "domain,company_name,homepage_url,priority_score,"
                  "ibba_members,consecutive_failures,seed_source",
        "order": "priority_score.desc,domain.asc",
        "limit": str(page),
    }
    if SEED:
        params["seed_source"] = f"ilike.*{SEED}*"
    while True:
        params["offset"] = str(offset)
        r = requests.get(f"{SUPABASE_URL}/rest/v1/v_discovery_queue",
                         headers=sb_headers(), params=params, timeout=90)
        r.raise_for_status()
        batch = r.json()
        for b in batch:
            rows.append({
                "domain": b["domain"],
                "company": b.get("company_name"),
                "active_listings": 0,
                "priority_score": b.get("priority_score"),
                "consecutive_failures": b.get("consecutive_failures") or 0,
            })
        if len(batch) < page:
            break
        offset += page
    return rows


def candidates_from_master(limit: int):
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


DONE_URLS = {}


def already_done():
    """Domains we've already resolved, so we don't pay for them twice."""
    seen = {}
    offset = 0
    while True:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/broker_discovery",
            headers=sb_headers(),
            params={"select": "domain,status,listings_url,url", "limit": "1000",
                    "offset": str(offset)},
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            break
        for row in rows:
            seen[row["domain"]] = row.get("status")
            DONE_URLS[row["domain"]] = row.get("listings_url") or row.get("url")
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


# broker_discovery status -> (discovery_stage, strategy_status) on broker_sources
#
# 'ok' lands at 3_crawlable (2026-09-24). The nightly scrape builds its list
# from broker_sources stages 3_crawlable + 4_producing, so a verified result
# has to land there directly. It used to stop at 2_listings_url_known and rely
# on export_discovered_ok.py appending to data/brokers_clean.csv, a file the
# scrape no longer reads. 'weak' stays at 2: the page is real but needs work.
# promote_producing_brokers() (pg_cron, daily) moves 3 -> 4 once it produces.
STAGE_FOR = {
    "ok":               ("3_crawlable", "ready"),
    "weak":             ("2_listings_url_known", "needs_work"),
    "no_listings_page": ("0_unusable", "unusable"),
    "dead":             ("0_unusable", "unusable"),
    "auth_required":    ("0_unusable", "unusable"),
    "blocked":          ("0_blocked", "failed"),
}


# Endpoints that return a non-empty list that is not listings.
_BAD_ENDPOINT_BITS = ("/wp/v2/posts", "/wp/v2/pages", "filter", "categor",
                      "taxonom", "/tags", "menu", "/users", "comments")
# Index pages that are archives, not live inventory.
_ARCHIVE_PATH_BITS = ("previous", "past-listing", "past_listing", "sold",
                      "closed", "archive", "recently")


_PRICE = re.compile(r"\$\s?\d{1,3}(?:,\d{3})+(?:\.\d+)?|\$\s?\d+(?:\.\d+)?\s?(?:m|mm|k)\b", re.I)
_LISTING_WORDS = ("asking", "cash flow", "revenue", "sde", "ebitda",
                  "gross sales", "listing", "for sale")


def page_evidence(dv, url):
    """Distinct price strings on the listings page, or 0 if it can't be read.

    The daily scraper only takes the listings URL from brokers_clean.csv and
    works out extraction itself, so what has to be right is the URL, not
    discovery's fingerprint. A page showing several asking prices next to
    listing vocabulary is a live inventory page whatever discovery matched on.
    (2026-09-15 dry run: myexitplan, salonspaconnection and atlantic were all
    real listing pages that discovery fingerprinted wrongly.)
    """
    try:
        r = dv.get_page(url, timeout=15)
    except Exception:
        return 0
    if r.status_code != 200:
        return 0
    text = re.sub(r"<[^>]+>", " ", (r.text or "")[:800_000])
    low = text.lower()
    if sum(1 for w in _LISTING_WORDS if w in low) < 2:
        return 0
    return len(set(m.group(0).replace(" ", "").lower() for m in _PRICE.finditer(text)))


MIN_PRICES = int(os.environ.get("MIN_PRICES", "3"))


def suspicious(listings_url, method, platform, result):
    """Reason an 'ok' can't be trusted, or None.

    broker_sources rows carry no claimed listing count, so the yield check
    above can only enforce its 3-item floor. These are the specific false
    positives seen in the 2026-09-15 IBBA dry run and earlier (CLAUDE.md:
    the WordPress /posts fallback that fooled 35 of 40 human-checked domains).
    """
    endpoint = (result.get("endpoint") or result.get("api_endpoint") or "").lower()
    path = urlparse(listings_url or "").path.lower()
    for bit in _BAD_ENDPOINT_BITS:
        if bit in endpoint:
            return f"endpoint {endpoint} is not a listings feed"
    for bit in _ARCHIVE_PATH_BITS:
        if bit in path:
            return f"{path} looks like an archive of past deals"
    if (method or "") == "data_attributes":
        return "matched only on generic data-* attributes"
    return None


def update_source(row, status, listings_url=None, error=None):
    """Mirror a discovery result onto broker_sources (SOURCE=broker_sources only)."""
    if DRY_RUN or SOURCE != "broker_sources":
        return
    domain = row["domain"]
    url = f"{SUPABASE_URL}/rest/v1/broker_sources"
    hdrs = {**sb_headers(), "Prefer": "return=minimal"}
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    stage, strat = STAGE_FOR.get(
        status, ("2_listings_url_known", "needs_work") if listings_url
        else ("0_unusable", "unusable"))
    body = {"discovery_stage": stage, "strategy_status": strat,
            "last_fingerprinted_at": now}
    if stage == "0_blocked":
        body["proxy_mode"] = "required"
    if stage in ("0_unusable", "0_blocked"):
        body["last_error_type"] = "HOMEPAGE_BLOCKED" if status == "blocked" else status.upper()
        body["last_failure_at"] = now
        if error:
            body["last_error_message"] = error[:300]
    if listings_url:
        body["listing_url"] = listings_url

    r = requests.patch(url, headers=hdrs, params={"domain": f"eq.{domain}"},
                       json=body, timeout=60)
    if r.status_code == 409 and "listing_url" in body:
        # listing_url is UNIQUE: another broker row already owns this page
        # (typically a vanity domain redirecting to a franchise site).
        body = {"discovery_stage": "0_unusable", "strategy_status": "unusable",
                "last_error_type": "DUPLICATE_LISTINGS_URL",
                "last_error_message": listings_url[:300],
                "last_fingerprinted_at": now}
        r = requests.patch(url, headers=hdrs, params={"domain": f"eq.{domain}"},
                           json=body, timeout=60)
    if not r.ok:
        print(f"    broker_sources update failed: {r.status_code} {r.text[:200]}")


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
    print(f"Source: {SOURCE}{'  seed~' + SEED if SEED else ''}")
    print(f"Already in broker_discovery: {len(seen)}")

    pool = candidates(LIMIT)
    queue = []
    for row in pool:
        st = seen.get(row["domain"])
        if st in TERMINAL and not RETRY_FAILED:
            prior_url = DONE_URLS.get(row["domain"])
            if SOURCE == "broker_sources" and (st != "ok" or prior_url):
                # Resolved in an earlier run but never mirrored onto
                # broker_sources; sync it so the row leaves the queue.
                update_source(row, st, prior_url)
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
            update_source(row, "dead", error=str(exc))
            continue

        reasons = getattr(dv, "LAST_FIND", {}).get("reasons") or []
        if (not listings_url and reasons and not domain.startswith("www.")
                and all(any(e in r for e in ("SSLError", "ConnectionError",
                                             "ConnectTimeout", "Timeout"))
                        for r in reasons[:3])):
            # Certificate only covers www., or the bare host doesn't resolve.
            alt = f"https://www.{domain}"
            print(f"    connection failed on bare domain - retrying {alt}")
            try:
                with domain_watchdog(DOMAIN_TIMEOUT_SECS):
                    listings_url = dv.find_listings_page(alt, verbose=True)
                if listings_url:
                    base = alt
            except Exception as exc:
                print(f"    probe error: {exc}")

        landed = getattr(dv, "LAST_FIND", {}).get("landed") or ""
        landed_host = urlparse(landed).netloc.lower()
        landed_host = landed_host[4:] if landed_host.startswith("www.") else landed_host
        reasons = getattr(dv, "LAST_FIND", {}).get("reasons") or []
        unreachable = bool(reasons) and all(
            any(e in r for e in ("SSLError", "ConnectionError", "Timeout"))
            for r in reasons[:3])

        if not listings_url and landed_host and landed_host != domain.removeprefix("www."):
            # Vanity domain that forwards to another site (tworldhouston.com ->
            # tworld.com/locations/...). That site is crawled under its own
            # domain, if at all; don't list its pages twice.
            print(f"    redirects to {landed_host} - not a separate broker site")
            tally["redirect"] = tally.get("redirect", 0) + 1
            save(domain, base, None, "no_listings_page", notes=f"redirects to {landed}"[:300])
            update_source(row, "no_listings_page", error=f"REDIRECTS_TO {landed}")
            time.sleep(1.0)
            continue
        elif not listings_url and unreachable:
            print("    unreachable (SSL/connection) - marked dead")
            tally["dead"] = tally.get("dead", 0) + 1
            why = "; ".join(reasons[:3])
            save(domain, base, None, "dead", notes=why[:300])
            update_source(row, "dead", error=why)
            time.sleep(1.0)
            continue
        elif not listings_url:
            if getattr(dv, "LAST_FIND", {}).get("blocked"):
                # A wall, not an answer. Record it as 'blocked' (not terminal)
                # and park the broker at 0_blocked with proxy_mode=required so
                # a proxy/browser pass can pick it up instead of it being
                # written off as having no listings.
                print("    blocked - parked for a proxy pass")
                tally["blocked"] = tally.get("blocked", 0) + 1
                why = "; ".join(dv.LAST_FIND.get("reasons", [])[:4])
                save(domain, base, None, "blocked", notes=why[:300])
                update_source(row, "blocked", error=why)
            else:
                print("    no listings page found")
                tally["no_listings_page"] = tally.get("no_listings_page", 0) + 1
                save(domain, base, None, "no_listings_page")
                update_source(row, "no_listings_page")
            time.sleep(1.0)
            continue

        print(f"    listings page: {listings_url}")

        # Fingerprint it so the scraper knows how to read it.
        method = platform = selector = None
        found = None
        result = None
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

        if SOURCE == "broker_sources" and status in ("ok", "weak"):
            path = urlparse(listings_url).path.lower()
            archive = next((b for b in _ARCHIVE_PATH_BITS if b in path), None)
            prices = 0 if archive else page_evidence(dv, listings_url)
            why = suspicious(listings_url, method, platform,
                             result if isinstance(result, dict) else {})
            if archive:
                status = "weak"
                print(f"    weak: {path} looks like an archive of past deals")
            elif prices >= MIN_PRICES:
                if status != "ok" or why:
                    print(f"    ok: page shows {prices} asking prices "
                          f"(discovery fingerprint: {why or 'thin'})")
                else:
                    print(f"    ok: page shows {prices} asking prices")
                status = "ok"
            else:
                status = "weak"
                print(f"    weak: only {prices} prices on the page"
                      + (f"; {why}" if why else ""))
        elif status == "ok":
            why = suspicious(listings_url, method, platform,
                             result if isinstance(result, dict) else {})
            if why:
                status = "weak"
                print(f"    weak: {why}")

        save(domain, base, listings_url, status, method, platform,
             selector, found, notes=row.get("company"))
        update_source(row, status, listings_url)
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
        print(f"\n--- {len(crawlable)} promoted to 3_crawlable (crawled from tonight) ---")
        for domain, url, listed in sorted(crawlable, key=lambda x: -x[2])[:40]:
            print(f"  {listed:5}  {url}")

    if weak:
        print(f"\n--- {len(weak)} found a listings page but extracted poorly ---")
        print("    (worth crawling anyway - the generic scraper may beat discovery)")
        for domain, url, listed in sorted(weak, key=lambda x: -x[2])[:20]:
            print(f"  {listed:5}  {url}")



if __name__ == "__main__":
    main()
