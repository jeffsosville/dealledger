#!/usr/bin/env python3
"""
discovery_benchmark.py - how often does find_listings_page() land on the page
we already know is right?

Answer key (no hand-built list needed):
  - broker_sources at 4_producing that crawled OK in the last 14 days, with a
    listing_url on the broker's own domain (sampled, SAMPLE rows)
  - every broker_ok / broker_fixed verdict from the manual review queue
    (broker_review_applied), which is exactly the hard cases

For each broker the finder starts from the bare homepage, as it would for a
new one. Result per domain:
  exact      found the known URL (ignoring scheme, www, trailing slash)
  same_path  same path, different query string
  candidate  known URL was among the candidates but another one ranked first
  other      found a different page
  none       found nothing
  blocked    homepage walled

Writes one row per domain to discovery_benchmark (run_id groups a run) and
prints the summary. Writes nothing to broker_sources.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY, SAMPLE (default 100), RUN_ID
"""
import os
import sys
import time
from urllib.parse import urlparse

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import discovery_v2 as dv  # noqa: E402
from discover_backlog import domain_watchdog  # noqa: E402

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SAMPLE = int(os.environ.get("SAMPLE", "100"))
RUN_ID = os.environ.get("RUN_ID") or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
TIMEOUT = int(os.environ.get("DOMAIN_TIMEOUT_SECS", "150"))
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}


def bare(h):
    h = (h or "").lower().split(":")[0]
    return h[4:] if h.startswith("www.") else h


def norm(u):
    p = urlparse(u or "")
    return bare(p.netloc), p.path.rstrip("/").lower() or "/", p.query


def answer_key():
    r = requests.post(f"{SUPABASE_URL}/rest/v1/rpc/discovery_benchmark_key",
                      headers=H, json={"n": SAMPLE}, timeout=60)
    r.raise_for_status()
    return r.json()


def judge(expected, found, candidates):
    if not found:
        return "none"
    e, f = norm(expected), norm(found)
    if e == f:
        return "exact"
    if e[:2] == f[:2]:
        return "same_path"
    if any(norm(c["url"])[:2] == e[:2] for c in candidates):
        return "candidate"
    return "other"


def main():
    if not SUPABASE_URL or not KEY:
        sys.exit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    key = answer_key()
    print(f"Benchmark {RUN_ID}: {len(key)} brokers\n")
    tally = {}
    for i, row in enumerate(key, 1):
        domain, expected = row["domain"], row["expected_url"]
        t0 = time.time()
        found, err = None, None
        try:
            with domain_watchdog(TIMEOUT):
                found = dv.find_listings_page(f"https://{domain}")
        except Exception as exc:
            err = str(exc)[:200]
        lf = getattr(dv, "LAST_FIND", {})
        cands = lf.get("candidates") or []
        match = "blocked" if (not found and lf.get("blocked")) else judge(expected, found, cands)
        if err and not found:
            match = "error"
        tally[match] = tally.get(match, 0) + 1
        print(f"[{i}/{len(key)}] {match:9} {domain}\n"
              f"      expected {expected}\n      found    {found}")
        r = requests.post(f"{SUPABASE_URL}/rest/v1/discovery_benchmark",
                          headers={**H, "Prefer": "return=minimal"},
                          json={"run_id": RUN_ID, "domain": domain,
                                "key_source": row.get("key_source"),
                                "expected_url": expected, "found_url": found,
                                "match": match, "candidates": cands[:5],
                                "reasons": (lf.get("reasons") or [])[:6],
                                "error": err, "secs": round(time.time() - t0, 1)},
                          timeout=60)
        if not r.ok:
            print(f"      write failed: {r.status_code} {r.text[:200]}")
        time.sleep(0.5)

    n = len(key) or 1
    hit = tally.get("exact", 0) + tally.get("same_path", 0)
    print("\n--- benchmark ---")
    for k, v in sorted(tally.items(), key=lambda kv: -kv[1]):
        print(f"  {k:10} {v:4}  {100 * v // n}%")
    print(f"\nFound the known page: {hit}/{len(key)} ({100 * hit // n}%)"
          f"; in candidates: {hit + tally.get('candidate', 0)}")


if __name__ == "__main__":
    main()
