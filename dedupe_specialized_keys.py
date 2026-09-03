#!/usr/bin/env python3
"""
dedupe_specialized_keys.py  —  ONE-TIME migration (execbb + vested ONLY)

Collapses the duplicate rows created by the old raw-URL upsert key for the two
brokers that actually produce them — execbb (trailing ID letters) and vested
(volatile slug) — and re-keys the survivor to the new stable-key scheme so the
patched scraper matches it going forward. Keeps the EARLIEST first_seen in each
group so days-on-market is preserved.

SCOPE — read this:
    This ONLY touches rows whose URL is on execbb.com or vestedbb.com AND whose
    listing_key() resolves to a stable 'execbb:<id>' / 'vestedbb:<id>' key.
    Every other row is left completely alone. That is deliberate:
      - The patched scraper returns the raw URL verbatim for all other brokers,
        so their ids are UNCHANGED and need no migration.
      - Index / placeholder URLs ('javascript:void(0)', '/listings/', etc.)
        are many DISTINCT businesses sharing one URL — collapsing them would be
        data loss, so they are never processed here.
    (An earlier draft scoped to all source='broker_direct' rows and would have
    deleted ~9,657 legitimate index-page rows. This version does not.)

ORDER OF OPERATIONS:
    1. python3 dedupe_specialized_keys.py            # dry run — shows dupe groups
    2. python3 dedupe_specialized_keys.py --apply     # collapse + re-key
    3. deploy is already done; next 8am cron picks up the patched scraper

SAFETY:
    - Dry run by default; nothing writes without --apply.
    - Assumes nothing FOREIGN-KEYs listings_direct.id. If a saved/favorites
      table references it, say so before --apply and we switch approaches.
    - Take a Supabase snapshot before --apply (it deletes rows).

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
Imports the single source of truth for the key from run_specialized.py.
"""

import os, sys, hashlib, time, argparse
import requests as http

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scrapers"))
from run_specialized import listing_key, derive_broker_domain  # noqa: E402

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
ENDPOINT = f"{SUPABASE_URL}/rest/v1/listings_direct"
HEADERS = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}

# Only these two brokers produced URL-variance dupes. Add a broker here only
# after listing_key() gains a stable per-broker rule for it.
TARGET_DOMAINS = ["execbb.com", "vestedbb.com"]


def stable_key(url: str):
    """Return the new key ONLY if it is a stable per-broker key (prefixed).
    Returns None for anything that would fall through to the raw-URL default —
    those rows are out of scope and must not be touched."""
    k = listing_key(url, derive_broker_domain(url))
    return k if (k.startswith("execbb:") or k.startswith("vestedbb:")) else None


def new_id(key: str) -> str:
    return f"spec:{hashlib.md5(key.encode()).hexdigest()[:16]}"


def fetch_target_rows():
    rows = []
    for dom in TARGET_DOMAINS:
        offset, page = 0, 1000
        while True:
            r = http.get(
                ENDPOINT,
                headers={**HEADERS, "Range-Unit": "items", "Range": f"{offset}-{offset+page-1}"},
                params={"url": f"ilike.*{dom}*", "select": "id,url,first_seen,last_seen"},
                timeout=60,
            )
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            rows.extend(batch)
            if len(batch) < page:
                break
            offset += page
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="actually write changes")
    args = ap.parse_args()
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    rows = fetch_target_rows()
    print(f"Fetched {len(rows)} execbb/vested rows")

    # Group by the new stable id; skip anything that isn't a stable key.
    groups = {}
    skipped = 0
    for row in rows:
        k = stable_key(row.get("url") or "")
        if not k:
            skipped += 1
            continue
        groups.setdefault(new_id(k), []).append(row)
    if skipped:
        print(f"Skipped {skipped} row(s) with no stable listing-id in the URL (left untouched)")

    dup_groups = {k: g for k, g in groups.items() if len(g) > 1}
    rekey_only = {k: g for k, g in groups.items() if len(g) == 1 and g[0]["id"] != k}
    n_delete = sum(len(g) - 1 for g in dup_groups.values())

    print(f"Duplicate groups: {len(dup_groups)}  (rows to delete: {n_delete})")
    print(f"Singletons needing re-key: {len(rekey_only)}")

    if not args.apply:
        for k, g in sorted(dup_groups.items(), key=lambda kv: -len(kv[1]))[:15]:
            print(f"  {k}  x{len(g)}  e.g. {g[0]['url'][:90]}")
        print("\nDRY RUN — re-run with --apply to execute.")
        return

    deleted = rekeyed = 0
    for k, g in dup_groups.items():
        g.sort(key=lambda r: (r.get("first_seen") or "9999", r["id"]))
        survivor, losers = g[0], [x["id"] for x in g[1:]]
        for i in range(0, len(losers), 100):
            idl = ",".join(f'"{x}"' for x in losers[i:i+100])
            dr = http.delete(ENDPOINT, headers=HEADERS, params={"id": f"in.({idl})"}, timeout=60)
            if dr.status_code in (200, 204):
                deleted += len(losers[i:i+100])
            else:
                print(f"  delete err {dr.status_code}: {dr.text[:160]}")
        if survivor["id"] != k:
            pr = http.patch(ENDPOINT, headers=HEADERS,
                            params={"id": f"eq.{survivor['id']}"}, json={"id": k}, timeout=60)
            rekeyed += 1 if pr.status_code in (200, 204) else 0
            if pr.status_code not in (200, 204):
                print(f"  rekey err {pr.status_code}: {pr.text[:160]}")
        time.sleep(0.02)

    for k, g in rekey_only.items():
        pr = http.patch(ENDPOINT, headers=HEADERS,
                        params={"id": f"eq.{g[0]['id']}"}, json={"id": k}, timeout=60)
        rekeyed += 1 if pr.status_code in (200, 204) else 0
        if pr.status_code not in (200, 204):
            print(f"  rekey err {pr.status_code}: {pr.text[:160]}")
        time.sleep(0.02)

    print(f"\nDONE — deleted {deleted} duplicate rows, re-keyed {rekeyed} survivors.")


if __name__ == "__main__":
    main()
