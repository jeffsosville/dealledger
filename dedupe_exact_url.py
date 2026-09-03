#!/usr/bin/env python3
"""
dedupe_exact_url.py  —  safe, table-wide cleanup of EXACT-URL duplicates

Targets exactly what the broker_domain detector measured: rows that share an
identical URL string (rows - distinct_urls). Those arise when the same listing
was inserted more than once under different historical id schemes.

WHAT IT DOES (and refuses to do):
    - Groups broker_direct rows by EXACT url.
    - Collapses ONLY small groups (size 2..--max-group, default 4) whose URL is
      a real per-listing page. Keeps the row with the freshest last_seen, resets
      its first_seen to the EARLIEST in the group (preserves days-on-market),
      deletes the rest.
    - SKIPS and REPORTS every large group and every index/placeholder URL. These
      are the `distinct_urls=1` brokers (e.g. reputedbrokerage 329 rows/1 URL) —
      hundreds of DISTINCT businesses sharing one useless URL. Collapsing them
      would be data loss, so this script never touches them. Fixing those is a
      scraper-coverage job, not a dedup.

This does NOT address slug-churn dupes (same listing, changing URL — vested-style
/ WP permalink churn). Those show as DISTINCT urls, so they aren't in this
metric; they need stable-id keying (already done for execbb/vested; WP post-id
is a separate future task).

ORDER:
    1. python3 dedupe_exact_url.py               # dry run — full report
    2. eyeball the SKIPPED-large-group list (make sure nothing real is there)
    3. python3 dedupe_exact_url.py --apply

SAFETY: dry run by default; snapshot before --apply; assumes nothing FKs
listings_direct.id. Env: SUPABASE_URL, SUPABASE_SERVICE_KEY.
"""

import os, sys, re, argparse
import requests as http

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scrapers"))
from run_specialized import detect_index_page_url  # single source of truth  # noqa: E402

# Index/search/category/facet URLs that detect_index_page_url() misses.
# Any match here is treated as NON-collapsible at ANY group size.
_INDEX_PAT = re.compile(
    r"[?&](page|paged|_sft|fwp|wpv|_sft_status|_paged)"   # paginated / faceted (WP, FacetWP)
    r"|/search\b|/author/|/status/|/find-a-business"       # search / archive / category
    r"|-for-sale-near-me|/businesses-for-sale-\d",         # generic index slugs
    re.I,
)


def looks_like_index(u: str) -> bool:
    """True if the URL is an index/search/category page (never collapse it)."""
    return (not detect_index_page_url(u)) or bool(_INDEX_PAT.search(u))

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
ENDPOINT = f"{SUPABASE_URL}/rest/v1/listings_direct"
HEADERS = {"apikey": KEY, "Authorization": f"Bearer {KEY}", "Content-Type": "application/json"}


def fetch_all():
    rows, offset, page = [], 0, 1000
    while True:
        r = http.get(
            ENDPOINT,
            headers={**HEADERS, "Range-Unit": "items", "Range": f"{offset}-{offset+page-1}"},
            params={"source": "eq.broker_direct",
                    "select": "id,url,first_seen,last_seen,url_is_listing_specific"},
            timeout=90,
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
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--max-group", type=int, default=4,
                    help="max group size treated as a real dupe (bigger = index page, skipped)")
    args = ap.parse_args()
    if not KEY:
        sys.exit("Set SUPABASE_SERVICE_KEY")

    rows = fetch_all()
    print(f"Fetched {len(rows)} broker_direct rows")

    groups = {}
    for row in rows:
        u = (row.get("url") or "").strip()
        if u:
            groups.setdefault(u, []).append(row)

    to_delete, big_skips, junk_skips = [], [], []
    rekey_first_seen = []  # (survivor_id, earliest_first_seen)
    collapsed_groups = 0

    for u, g in groups.items():
        if len(g) < 2:
            continue
        # Guard 1: never collapse index / search / category / faceted URLs,
        # nor rows the scraper already flagged as non-listing-specific.
        if looks_like_index(u) or g[0].get("url_is_listing_specific") is False:
            junk_skips.append((u, len(g)))
            continue
        # Guard 2: never collapse suspiciously large groups (shared/placeholder URL).
        if len(g) > args.max_group:
            big_skips.append((u, len(g)))
            continue
        # Safe: same listing re-inserted. Keep freshest last_seen, preserve
        # earliest first_seen, delete the rest.
        g.sort(key=lambda r: (r.get("last_seen") or ""), reverse=True)
        survivor, losers = g[0], g[1:]
        earliest = min((r.get("first_seen") or "9999") for r in g)
        to_delete.extend(r["id"] for r in losers)
        collapsed_groups += 1
        if (survivor.get("first_seen") or "9999") != earliest:
            rekey_first_seen.append((survivor["id"], earliest))

    print(f"\nReal exact-URL dupes to delete : {len(to_delete)}  "
          f"(across {collapsed_groups} groups, size 2..{args.max_group})")
    print(f"first_seen to correct on survivors: {len(rekey_first_seen)}")
    print(f"SKIPPED index/placeholder URLs    : {len(junk_skips)}")
    print(f"SKIPPED large groups (>{args.max_group}, likely index): {len(big_skips)}")

    if big_skips:
        print("\n  Largest skipped groups (verify these are NOT real listings):")
        for u, n in sorted(big_skips, key=lambda x: -x[1])[:15]:
            print(f"    x{n:>4}  {u[:88]}")

    if not args.apply:
        print("\nDRY RUN — re-run with --apply to delete the real dupes only.")
        return

    deleted = 0
    for i in range(0, len(to_delete), 100):
        chunk = to_delete[i:i+100]
        idl = ",".join(f'"{x}"' for x in chunk)
        dr = http.delete(ENDPOINT, headers=HEADERS, params={"id": f"in.({idl})"}, timeout=60)
        if dr.status_code in (200, 204):
            deleted += len(chunk)
        else:
            print(f"  delete err {dr.status_code}: {dr.text[:160]}")
    fixed = 0
    for sid, fs in rekey_first_seen:
        pr = http.patch(ENDPOINT, headers=HEADERS,
                        params={"id": f"eq.{sid}"}, json={"first_seen": fs}, timeout=60)
        fixed += 1 if pr.status_code in (200, 204) else 0

    print(f"\nDONE — deleted {deleted} exact-URL dupes, corrected first_seen on {fixed} survivors.")
    print("Untouched: all index-page / large-group rows (separate coverage fix).")


if __name__ == "__main__":
    main()
