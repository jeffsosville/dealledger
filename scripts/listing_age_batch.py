"""Batch listing-age check: the homepage lookup, for a whole list of URLs.

Input: a text or CSV file, one listing per line. The first column is the URL
(or bare listing number). An optional second column is who listed it, as you
recorded it while collecting ("owner", "FSBO", or the broker's name); it is
passed through untouched, because the stored data can't tell owner ads from
broker ads reliably.

    https://www.bizbuysell.com/business-opportunity/.../2469691/,owner
    https://www.bizquest.com/business-for-sale/.../BW2200000/,Sunbelt Atlanta

Output: CSV with, per listing,
    provable_listed_by / provable_days   USE THESE IN PRINT. The next Internet
        Archive anchor above this listing number was captured by that date, and
        numbers are assigned in order, so this listing existed by then too.
    estimated_listed_date / estimated_days   interpolated between anchors; a
        better guess, but a guess. basis says how it was derived.
    anchor_below_* / anchor_above_*   the evidence either side
    archive_url   the Wayback history of the URL itself. A listing number dates
        when the listing was created, not that it was advertised continuously;
        a reactivated listing keeps its old number. Check this link for any
        listing you name.

This only calls DealLedger's own database (the public dl_lookup_evidence
function). It never contacts BizBuySell, BizQuest or the Internet Archive.

    python3 scripts/listing_age_batch.py urls.txt                # -> urls_ages.csv
    python3 scripts/listing_age_batch.py urls.txt -o out.csv
"""
import argparse
import csv
import json
import os
import sys
import time
import urllib.request

# The same public, read-only key the homepage uses (lib/home.html).
SUPABASE_URL = "https://kqckuedsyyosmccushyd.supabase.co"
ANON = ("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtxY2t1ZWRzeXlvc21jY3VzaHlkIiwicm9sZSI6"
        "ImFub24iLCJpYXQiOjE3NzM2NzYwMjAsImV4cCI6MjA4OTI1MjAyMH0.UJA9n-Rc9dMDq6Rv608sSHDKykrIurSrWGiSxCUxCYg")

COLUMNS = [
    "input", "listed_by", "kind", "listing_number",
    "provable_listed_by", "provable_days",
    "estimated_listed_date", "estimated_days", "basis",
    "anchor_below_number", "anchor_below_site", "anchor_below_captured",
    "anchor_above_number", "anchor_above_site", "anchor_above_captured",
    "archive_url", "note",
]


def lookup(q):
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/rpc/dl_lookup_evidence",
        data=json.dumps({"q": q}).encode(),
        headers={"apikey": ANON, "Authorization": f"Bearer {ANON}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode() or "{}") or {}


def read_inputs(path):
    rows = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for rec in csv.reader(f):
            if not rec or not rec[0].strip() or rec[0].strip().startswith("#"):
                continue
            if rec[0].strip().lower() in ("url", "input", "listing"):
                continue  # header row
            rows.append((rec[0].strip(), rec[1].strip() if len(rec) > 1 else ""))
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("infile")
    ap.add_argument("-o", "--out")
    args = ap.parse_args()
    out = args.out or os.path.splitext(args.infile)[0] + "_ages.csv"

    inputs = read_inputs(args.infile)
    if not inputs:
        sys.exit("no URLs found in " + args.infile)

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for i, (q, listed_by) in enumerate(inputs, 1):
            try:
                d = lookup(q)
            except Exception as e:  # keep going; one bad row shouldn't sink the batch
                d = {"kind": "error", "note": str(e)[:200]}
            lo, hi = d.get("anchor_below") or {}, d.get("anchor_above") or {}
            w.writerow({
                "input": q, "listed_by": listed_by, "kind": d.get("kind"),
                "listing_number": d.get("listing_number"),
                "provable_listed_by": d.get("provable_listed_by"), "provable_days": d.get("provable_days"),
                "estimated_listed_date": d.get("estimated_listed_date") or d.get("first_seen"),
                "estimated_days": d.get("days"), "basis": d.get("basis"),
                "anchor_below_number": lo.get("listing_number"), "anchor_below_site": lo.get("site"),
                "anchor_below_captured": lo.get("earliest_capture"),
                "anchor_above_number": hi.get("listing_number"), "anchor_above_site": hi.get("site"),
                "anchor_above_captured": hi.get("earliest_capture"),
                "archive_url": d.get("archive_url"), "note": d.get("note"),
            })
            print(f"[{i}/{len(inputs)}] {d.get('provable_days') or d.get('days') or '—':>6}  {q[:90]}")
            time.sleep(0.2)
    print(f"[+] wrote {out}")


if __name__ == "__main__":
    main()
