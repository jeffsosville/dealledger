#!/usr/bin/env python3
"""
export_public_listings.py — the public download: every published, active,
broker-direct listing, written to data/public/ as CSV and JSON.

Stable URLs (overwritten daily):
    https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.csv
    https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.json
Field definitions: data/public/DATA_DICTIONARY.md

Rows: listings_direct where status='active', published=true,
source='broker_direct', and the listing URL is not on a marketplace domain.
No days-on-market field is exported until the DOM definition is settled.

Env: SUPABASE_URL, and SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY.

Usage:
    python3 scripts/export_public_listings.py
    python3 scripts/export_public_listings.py --out /tmp/check
"""

import argparse
import csv
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

FIELDS = [
    "id", "title", "asking_price", "cash_flow", "revenue",
    "city", "state", "vertical", "broker_name", "broker_domain",
    "url", "first_seen", "last_seen", "description",
]
MARKETPLACE_RE = re.compile(r"(bizbuysell|bizquest|businessesforsale|loopnet)\.com", re.I)
PAGE = 1000
DESC_MAX = 1000


def fetch_all(base, key):
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    params = {
        "select": ",".join(FIELDS),
        "status": "eq.active",
        "published": "is.true",
        "source": "eq.broker_direct",
        "order": "id.asc",
        "limit": str(PAGE),
    }
    rows, last_id = [], None
    while True:
        p = dict(params)
        if last_id is not None:
            p["id"] = f"gt.{last_id}"
        r = requests.get(f"{base}/rest/v1/listings_direct", headers=headers, params=p, timeout=60)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        last_id = batch[-1]["id"]
        if len(batch) < PAGE:
            break
    return rows


def clean(row):
    out = {k: row.get(k) for k in FIELDS}
    d = out.get("description")
    if d:
        d = re.sub(r"\s+", " ", d).strip()
        out["description"] = d[:DESC_MAX]
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/public")
    args = ap.parse_args()

    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY") or os.environ.get("SUPABASE_ANON_KEY")
    if not base or not key:
        sys.exit("Set SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_ANON_KEY).")

    raw = fetch_all(base, key)
    rows = [clean(r) for r in raw if r.get("url") and not MARKETPLACE_RE.search(r["url"])]
    rows.sort(key=lambda r: (r.get("state") or "~", r.get("broker_domain") or "", r["id"]))

    if len(rows) < 5000:
        sys.exit(f"Only {len(rows)} rows; refusing to overwrite the public file (expected ~20K+).")

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    with open(out / "listings.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    with open(out / "listings.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, separators=(",", ":"), default=str)

    def pct(field):
        return round(100 * sum(1 for r in rows if r.get(field) not in (None, "")) / len(rows))

    meta = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": len(rows),
        "broker_domains": len({r.get("broker_domain") for r in rows if r.get("broker_domain")}),
        "coverage_pct": {f: pct(f) for f in
                         ["asking_price", "cash_flow", "revenue", "city", "state", "vertical"]},
        "license": "CC0-1.0",
        "dictionary": "DATA_DICTIONARY.md",
    }
    with open(out / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
