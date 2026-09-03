#!/usr/bin/env python3
"""Print row count, columns, and one sample row for the tables that matter
to the BizQuest-views / matching question. Read-only."""
import os, json
import requests as http

URL = os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")
KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
H = {"apikey": KEY, "Authorization": f"Bearer {KEY}"}

TABLES = [
    "listing_matches", "unmatched_listings", "unified_listings",
    "listings_direct_live", "dealledger_listings", "listings",
    "listing_history", "vending_listings_merge",
]

if not KEY:
    raise SystemExit("SUPABASE_SERVICE_KEY not set")

for t in TABLES:
    try:
        r = http.get(f"{URL}/rest/v1/{t}",
                     headers={**H, "Prefer": "count=exact", "Range": "0-0"},
                     params={"select": "*"}, timeout=30)
        cnt = r.headers.get("content-range", "?/?").split("/")[-1]
        rows = r.json() if r.ok else []
        cols = list(rows[0].keys()) if rows else []
        print(f"\n=== {t}   (rows: {cnt}) ===")
        print("cols:", ", ".join(cols) if cols else "(empty / none)")
        if rows:
            samp = {k: (str(v)[:50] if v is not None else None) for k, v in rows[0].items()}
            print("sample:", json.dumps(samp)[:700])
    except Exception as e:
        print(f"\n=== {t} ===  ERROR: {e}")

# Does anything still carry view counts?
print("\n--- listing_views coverage where present ---")
for t in ("listings", "listings_direct", "listings_direct_live", "unified_listings", "dealledger_listings"):
    try:
        r = http.get(f"{URL}/rest/v1/{t}",
                     headers={**H, "Prefer": "count=exact", "Range": "0-0"},
                     params={"select": "*", "listing_views": "gt.0"}, timeout=30)
        if r.ok:
            cnt = r.headers.get("content-range", "?/?").split("/")[-1]
            print(f"  {t}: rows with listing_views>0 = {cnt}")
        else:
            print(f"  {t}: (no listing_views column or not queryable)")
    except Exception as e:
        print(f"  {t}: err {e}")
