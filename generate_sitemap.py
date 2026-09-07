#!/usr/bin/env python3
"""Generate sitemap.xml for dealledger.org from Supabase.

Only emits URLs for pages that actually exist: active listings that have
a title, and broker firms that currently have active listings. Dead
listings are deliberately excluded — submitting URLs that 404 or render
empty wastes crawl budget and can suppress the rest of the site.

Writes into public/:
    sitemap.xml              <- index pointing at the parts below
    sitemap-static.xml
    sitemap-brokers.xml
    sitemap-listings-N.xml

Usage:
    SUPABASE_SERVICE_KEY=... python3 scripts/generate_sitemap.py --dry-run
    SUPABASE_SERVICE_KEY=... python3 scripts/generate_sitemap.py
"""

import os
import sys
import math
import argparse
from datetime import date
from xml.sax.saxutils import escape

# One canonical host. Everything here must use it — mixing www and
# non-www in a sitemap makes the duplicate-host problem worse.
BASE = os.environ.get("SITE_BASE", "https://dealledger.org").rstrip("/")

OUT_DIR  = os.environ.get("SITEMAP_OUT", "public")
PER_FILE = 25000     # comfortably under Google's 50k / 50MB limit
PAGE     = 1000      # PostgREST caps a single response at 1000 rows

STATIC_PAGES = [
    ("/",                 "daily",   "1.0"),
    ("/brokers",          "daily",   "0.9"),
    ("/methodology.html", "monthly", "0.6"),
    ("/pulse.html",       "weekly",  "0.8"),
    ("/hot.html",         "daily",   "0.7"),
]


def client():
    from supabase import create_client
    url = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not key:
        sys.exit("[-] Set SUPABASE_SERVICE_KEY (or SUPABASE_ANON_KEY)")
    return create_client(url, key)


def fetch_all(sb, table, columns, key_col, label="rows", **eq):
    """Page through a table using keyset pagination on key_col.

    Offset paging with .range() is unreliable here: without a stable sort
    PostgREST can return overlapping or skipped rows between requests,
    which silently drops a chunk of the table. Walking forward on a
    unique ascending key can't drift.
    """
    out, cursor = [], None
    while True:
        q = sb.table(table).select(columns)
        for col, val in eq.items():
            q = q.eq(col, val)
        if cursor is not None:
            q = q.gt(key_col, cursor)
        rows = q.order(key_col).limit(PAGE).execute().data or []
        if not rows:
            break
        out.extend(rows)
        cursor = rows[-1][key_col]
        print(f"    ...{len(out):,} {label}", end="\r")
        if len(rows) < PAGE:
            break
    print(" " * 44, end="\r")
    return out


def url_entry(loc, lastmod=None, changefreq=None, priority=None):
    bits = [f"    <loc>{escape(loc)}</loc>"]
    if lastmod:
        bits.append(f"    <lastmod>{lastmod}</lastmod>")
    if changefreq:
        bits.append(f"    <changefreq>{changefreq}</changefreq>")
    if priority:
        bits.append(f"    <priority>{priority}</priority>")
    return "  <url>\n" + "\n".join(bits) + "\n  </url>"


def write_urlset(path, entries):
    xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
           + "\n".join(entries) + "\n</urlset>\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"[+] {path}  ({len(entries):,} urls, "
          f"{os.path.getsize(path) / 1024:,.0f} KB)")


def write_index(path, files, today):
    parts = ["  <sitemap>\n"
             f"    <loc>{BASE}/{name}</loc>\n"
             f"    <lastmod>{today}</lastmod>\n"
             "  </sitemap>" for name in files]
    xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
           + "\n".join(parts) + "\n</sitemapindex>\n")
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"[+] {path}  (index of {len(files)} sitemaps)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="count URLs without writing any files")
    ap.add_argument("--no-brokers", action="store_true",
                    help="skip broker pages entirely")
    args = ap.parse_args()

    today = date.today().isoformat()
    sb = client()

    # ---- active listings -------------------------------------------------
    print("[*] Fetching active listings...")
    rows = fetch_all(sb, "listings",
                     "listing_number,last_seen,estimated_listed_date,header",
                     key_col="listing_number", label="listings",
                     is_active=True)

    listings, seen_numbers = [], set()
    for r in rows:
        ln = r.get("listing_number")
        if not ln or ln in seen_numbers:
            continue
        if not (r.get("header") or "").strip():
            continue
        seen_numbers.add(ln)
        listings.append(r)
    print(f"[+] {len(listings):,} active listings with titles")

    # ---- broker firms ----------------------------------------------------
    # Slugs live in broker_firms, not listings. sum_active_per_agent > 0
    # is the "this firm currently has inventory" test.
    brokers = {}
    if not args.no_brokers:
        print("[*] Fetching broker firms...")
        firm_rows = fetch_all(sb, "broker_firms", "slug,sum_active_per_agent",
                              key_col="slug", label="firms")
        for r in firm_rows:
            slug = (r.get("slug") or "").strip()
            if not slug:
                continue
            if (r.get("sum_active_per_agent") or 0) <= 0:
                continue
            brokers[slug] = today
        print(f"[+] {len(brokers):,} broker firms with active listings")

    total = len(STATIC_PAGES) + len(brokers) + len(listings)
    files_needed = 1 + (1 if brokers else 0) + max(1, math.ceil(len(listings) / PER_FILE))

    if args.dry_run:
        print(f"\n[dry run] {total:,} URLs across {files_needed} files")
        print(f"[dry run] canonical host: {BASE}")
        print(f"[dry run] would write into: {OUT_DIR}/")
        return

    os.makedirs(OUT_DIR, exist_ok=True)
    written = []

    # static
    write_urlset(os.path.join(OUT_DIR, "sitemap-static.xml"),
                 [url_entry(BASE + p, today, cf, pr) for p, cf, pr in STATIC_PAGES])
    written.append("sitemap-static.xml")

    # brokers
    if brokers:
        write_urlset(
            os.path.join(OUT_DIR, "sitemap-brokers.xml"),
            [url_entry(f"{BASE}/broker/{slug}", stamp, "weekly", "0.7")
             for slug, stamp in sorted(brokers.items())])
        written.append("sitemap-brokers.xml")

    # listings, newest first, chunked
    listings.sort(key=lambda l: l["listing_number"], reverse=True)
    chunks = max(1, math.ceil(len(listings) / PER_FILE))
    for i in range(chunks):
        part = listings[i * PER_FILE:(i + 1) * PER_FILE]
        entries = [
            url_entry(f"{BASE}/listing/{l['listing_number']}",
                      l.get("last_seen") or l.get("estimated_listed_date") or today,
                      "weekly", "0.6")
            for l in part
        ]
        name = f"sitemap-listings-{i + 1}.xml"
        write_urlset(os.path.join(OUT_DIR, name), entries)
        written.append(name)

    write_index(os.path.join(OUT_DIR, "sitemap.xml"), written, today)

    print(f"\n[+] Done — {total:,} URLs.")
    print(f"[i] Canonical host: {BASE}")
    print(f"[i] Submit {BASE}/sitemap.xml in Search Console.")


if __name__ == "__main__":
    main()