#!/usr/bin/env python3
"""
export_public_listings.py — the public download: every published, active,
broker-direct listing, written to data/public/ as CSV and JSON, plus
listings_left_observation.csv (listings absent from 3 complete crawls of their
broker's site, with the date they left observation — never labelled sold).

Stable URLs (overwritten daily, only after scripts/verify_public_export.py passes):
    https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.csv
    https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.json
Field definitions: data/public/DATA_DICTIONARY.md

Rows: listings_direct where status='active', published=true,
source='broker_direct', the listing URL's host is not a marketplace,
aggregator, digital-business platform, real-estate portal or broker MLS
(scripts/public_scope.py), and the broker is not on broker_block.

Listing age comes from listing_age_v2, the same table the website reads
(mv_listings_page_v2), so the file and the site show the same evidence:
  age_basis  window    listed between listed_after and age_date (complete crawls)
             observed  first dated sighting in a DealLedger crawl (a minimum)
             in_db     date the record was first written to our database (a minimum)
No days-on-market field is exported (DOM hold). Rows created after the day's
listing_age_v2 build have no age yet and wait for tomorrow's file.

Copies of the same listing on a broker's site (one listing_dates_v2 key) are
exported once.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (listing_dates_v2 is not readable with the anon key).

Usage:
    python3 scripts/export_public_listings.py
    python3 scripts/export_public_listings.py --out /tmp/check
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_scope import AGE_BASES, bare_domain, host_denied, host_of, listing_number  # noqa: E402

SCHEMA_VERSION = 2
SITE = "https://dealledger.org/listing/"

SOURCE_FIELDS = [
    "id", "title", "asking_price", "cash_flow", "revenue",
    "city", "state", "vertical", "broker_name", "broker_domain", "url",
]
AGE_FIELDS = ["age_basis", "age_date", "listed_after"]
FIELDS = ["listing_number", "dealledger_url"] + SOURCE_FIELDS + AGE_FIELDS + ["last_seen"]
PAGE = 1000


def _paged(base, key, table, params, order_col):
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    params = dict(params, order=f"{order_col}.asc", limit=str(PAGE))
    rows, last_id = [], None
    while True:
        p = dict(params)
        if last_id is not None:
            quoted = str(last_id).replace("\\", "\\\\").replace('"', '\\"')
            p[order_col] = f'gt."{quoted}"'  # keys contain | : / and can't go unquoted
        r = requests.get(f"{base}/rest/v1/{table}", headers=headers, params=p, timeout=60)
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        last_id = batch[-1][order_col]
        if len(batch) < PAGE:
            break
    return rows


def fetch_all(base, key):
    return _paged(base, key, "listings_direct", {
        "select": ",".join(SOURCE_FIELDS),
        "status": "eq.active",
        "published": "is.true",
        "source": "eq.broker_direct",
    }, "id")


def fetch_blocked(base, key):
    return {bare_domain(r["broker_domain"])
            for r in _paged(base, key, "broker_block", {"select": "broker_domain"}, "broker_domain")}


def fetch_dates(base, key, published_only=True):
    """{row_id: dates record} from listing_dates_v2 (identity + last_seen)."""
    params = {"select": "listing_key,row_ids,last_seen"}
    if published_only:
        params["published"] = "is.true"
    by_row = {}
    for rec in _paged(base, key, "listing_dates_v2", params, "listing_key"):
        for rid in rec.get("row_ids") or []:
            by_row[rid] = rec
    return by_row


def fetch_ages(base, key):
    """{listing_number: age row} from listing_age_v2 — what the website shows."""
    rows = _paged(base, key, "listing_age_v2",
                  {"select": "listing_number,age_basis,age_date,listed_after,built_at"}, "listing_number")
    return {int(r["listing_number"]): r for r in rows}


def permitted(row, blocked):
    url = row.get("url")
    if not url:
        return False
    if host_denied(host_of(url), blocked):
        return False
    return bare_domain(row.get("broker_domain")) not in blocked


def apply_age(row, ages):
    """Attach the public listing number and listing-age evidence. Returns False
    if listing_age_v2 has no usable age for the row yet."""
    n = listing_number(row["id"])
    a = ages.get(n)
    if not a or a.get("age_basis") not in AGE_BASES or not a.get("age_date"):
        return False
    row["listing_number"] = n
    row["dealledger_url"] = SITE + str(n)
    row["age_basis"] = a["age_basis"]
    row["age_date"] = a["age_date"]
    row["listed_after"] = a.get("listed_after") if a["age_basis"] == "window" else None
    return True


def dedupe(rows, by_row):
    """One output row per listing_dates_v2 key; last_seen from that key."""
    out, seen_keys = [], set()
    for r in rows:
        rec = by_row.get(r["id"])
        if rec:
            if rec["listing_key"] in seen_keys:
                continue
            seen_keys.add(rec["listing_key"])
            r["last_seen"] = rec.get("last_seen")
        else:
            r["last_seen"] = None
        out.append(r)
    return out


LEFT_FIELDS = ["listing_number", "dealledger_url"] + SOURCE_FIELDS + AGE_FIELDS + [
    "last_seen", "left_observation_on", "left_observation_reason",
]


def build_left_observation(base, key, ages, blocked):
    """Listings that left observation (absent from 3 complete crawls of the
    broker's site). Kept in the open record; not a claim the business sold."""
    rows = _paged(base, key, "v_listings_left_observation", {"select": "*"}, "id")
    out, no_age = [], 0
    for r in rows:
        if not permitted(r, blocked):
            continue
        if not apply_age(r, ages):
            no_age += 1
            continue
        out.append({k: r.get(k) for k in LEFT_FIELDS})
    out.sort(key=lambda r: (r.get("left_observation_on") or "", r["id"]))
    return out, no_age


def write_csv(path, fields, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/public")
    args = ap.parse_args()

    base = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_KEY")
    if not base or not key:
        sys.exit("Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")

    blocked = fetch_blocked(base, key)
    ages = fetch_ages(base, key)
    if len(ages) < 5000:
        sys.exit(f"listing_age_v2 returned {len(ages)} rows; refusing to export without ages.")
    by_row = fetch_dates(base, key)

    raw = fetch_all(base, key)
    excluded_scope = sum(1 for r in raw if not permitted(r, blocked))
    rows = [{k: r.get(k) for k in SOURCE_FIELDS} for r in raw if permitted(r, blocked)]
    rows.sort(key=lambda r: r["id"])
    rows = dedupe(rows, by_row)
    held_no_age = 0
    kept = []
    for r in rows:
        if apply_age(r, ages):
            kept.append({k: r.get(k) for k in FIELDS})
        else:
            held_no_age += 1
    rows = kept
    rows.sort(key=lambda r: (r.get("state") or "~", r.get("broker_domain") or "", r["id"]))

    if len(rows) < 5000:
        sys.exit(f"Only {len(rows)} rows; refusing to write the public file (expected ~20K+).")

    left, left_no_age = build_left_observation(base, key, ages, blocked)

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "listings.csv", FIELDS, rows)
    with open(out / "listings.json", "w", encoding="utf-8") as f:
        json.dump(rows, f, ensure_ascii=False, separators=(",", ":"), default=str)
    write_csv(out / "listings_left_observation.csv", LEFT_FIELDS, left)

    def pct(field):
        return round(100 * sum(1 for r in rows if r.get(field) not in (None, "")) / len(rows))

    built = max((a.get("built_at") or "" for a in ages.values()), default=None)
    meta = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "listing_age_built_at": built,
        "rows": len(rows),
        "broker_domains": len({bare_domain(r.get("broker_domain")) for r in rows if r.get("broker_domain")}),
        "coverage_pct": {f: pct(f) for f in
                         ["asking_price", "cash_flow", "revenue", "city", "state", "vertical"]},
        "age_basis": {b: sum(1 for r in rows if r["age_basis"] == b) for b in AGE_BASES},
        "excluded_out_of_scope": excluded_scope,
        "held_until_next_age_build": held_no_age,
        "left_observation_rows": len(left),
        "left_observation_held_no_age": left_no_age,
        "license": "CC0-1.0",
        "dictionary": "DATA_DICTIONARY.md",
        "verification": "verification.json",
    }
    with open(out / "meta.json", "w") as f:
        json.dump(meta, f, indent=2)

    print(json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
