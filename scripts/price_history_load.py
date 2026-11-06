#!/usr/bin/env python3
"""
price_history_load.py — load the files from price_history_build.py into the
private price-history tables via public.price_history_load() (service key only).

Idempotent: re-running loads nothing new; only hand-review fields on backfill
changes are refreshed. Order: source files, identity, evidence, changes.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY (~/.dealledger.env).

Usage:
    python3 scripts/price_history_load.py --dir /tmp/ph --dry-run
    python3 scripts/price_history_load.py --dir /tmp/ph
"""
import argparse
import csv
import os
import sys
import time

import requests

csv.field_size_limit(10**9)
BATCH = {"file": 500, "identity": 5000, "evidence": 3000, "change": 2000}
FILES = [("file", "source_files.csv"), ("identity", "listing_identity.csv"), ("evidence", "price_evidence.csv"),
         ("change", "price_changes.csv")]
NUMERIC = {"asking_price", "cash_flow", "revenue", "prev_price", "next_price", "pct_change"}
INTEGER = {"row_index", "ids_seen", "file_id", "row_count"}


def typed(r):
    out = {}
    for k, v in r.items():
        if k in NUMERIC:
            out[k] = float(v) if v not in ("", None) else None
        elif k in INTEGER:
            out[k] = int(v)
        else:
            out[k] = v
    return out


def batches(path, size):
    with open(path, newline="") as fh:
        buf = []
        for r in csv.DictReader(fh):
            buf.append(typed(r))
            if len(buf) >= size:
                yield buf
                buf = []
        if buf:
            yield buf


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_SERVICE_KEY")
    if not a.dry_run and not (url and key):
        sys.exit("Set SUPABASE_URL and SUPABASE_SERVICE_KEY")
    s = requests.Session()
    s.headers.update({"apikey": key or "", "Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    for kind, fname in FILES:
        total_rows = total_new = 0
        for b in batches(os.path.join(a.dir, fname), BATCH[kind]):
            total_rows += len(b)
            if a.dry_run:
                continue
            for attempt in range(4):
                r = s.post(f"{url}/rest/v1/rpc/price_history_load", json={"p_kind": kind, "p_rows": b}, timeout=300)
                if r.status_code == 200:
                    total_new += int(r.json() or 0)
                    break
                if attempt == 3:
                    sys.exit(f"{kind}: HTTP {r.status_code} {r.text[:300]}")
                time.sleep(5 * (attempt + 1))
        print(f"{kind:<9} rows read {total_rows:>9,}   inserted/updated {total_new:>9,}" + ("   (dry run)" if a.dry_run else ""))


if __name__ == "__main__":
    main()
