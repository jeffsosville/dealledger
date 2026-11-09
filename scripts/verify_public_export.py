#!/usr/bin/env python3
"""
verify_public_export.py — gate between the daily export and the commit.

Reads the files scripts/export_public_listings.py just wrote, re-reads the
database independently, and fails (exit 1, nothing gets committed) unless:

  Provenance
    P1  columns are exactly the published schema; no broker text, contact,
        marketplace or days-on-market columns
    P2  every listings.csv row is, right now, status='active', published=true,
        source='broker_direct' in listings_direct, with the same url and domain
    P3  no row's URL host is a marketplace, aggregator, digital platform,
        real-estate portal or broker MLS, and no row's broker is on broker_block
    P4  listing numbers are unique and match the database's formula for the row
  Listing age (must match what the website shows)
    A1  every row has age_basis in (window, observed, in_db) and the same
        age_basis / age_date / listed_after as listing_age_v2
    A2  the evidence is internally consistent: a window has listed_after <
        age_date and is <= 45 days; other levels have no listed_after; dates
        fall between the record start (2026-02-18) and today
    A3  where the listing is on the site, mv_listings_page_v2 shows the same
        evidence
  Left observation
    L1  every listings_left_observation.csv row is inactive in listings_direct,
        currently in v_listings_left_observation (three-crawl evidence), passes
        P3, matches listing_age_v2, and never says "sold"
  Sanity
    S1  row count is at least 15,000 and within 25% of the site's listing count
        (public_stats.listings_on_site)
    S2  meta.json agrees with the files

Writes data/public/verification.json (committed with the files) either way.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY
Usage: python3 scripts/verify_public_export.py [--dir data/public]
"""

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from public_scope import (  # noqa: E402
    AGE_BASES, FORBIDDEN_COLUMNS, RECORD_START, SOLD_WORDS, WINDOW_MAX_DAYS,
    bare_domain, host_denied, host_of, listing_number,
)
from export_public_listings import FIELDS, LEFT_FIELDS  # noqa: E402

MIN_ROWS = 15000
SITE_TOLERANCE = 0.25
MAX_EXAMPLES = 5


# ---------------------------------------------------------------- data access
class Db:
    def __init__(self, base, key):
        self.base, self.h = base.rstrip("/"), {"apikey": key, "Authorization": f"Bearer {key}"}

    def get(self, table, params):
        r = requests.get(f"{self.base}/rest/v1/{table}", headers=self.h, params=params, timeout=90)
        r.raise_for_status()
        return r.json()

    def by_values(self, table, col, values, select, chunk=150):
        out = []
        values = list(values)
        for i in range(0, len(values), chunk):
            part = values[i:i + chunk]
            inner = ",".join('"' + str(v).replace('"', '\\"') + '"' for v in part)
            out.extend(self.get(table, {"select": select, col: f"in.({inner})"}))
        return out

    def all(self, table, select, order):
        rows, last = [], None
        while True:
            p = {"select": select, "order": f"{order}.asc", "limit": "1000"}
            if last is not None:
                p[order] = f'gt."{last}"'
            batch = self.get(table, p)
            rows.extend(batch)
            if len(batch) < 1000:
                return rows
            last = batch[-1][order]


# ---------------------------------------------------------------- checks
class Report:
    def __init__(self):
        self.checks = []

    def add(self, code, name, failures, checked):
        self.checks.append({
            "check": code, "name": name, "checked": checked,
            "failures": len(failures), "passed": not failures,
            "examples": failures[:MAX_EXAMPLES],
        })

    @property
    def passed(self):
        return all(c["passed"] for c in self.checks)


def _d(s):
    return date.fromisoformat(str(s)[:10]) if s else None


def check_columns(header, expected):
    fails = []
    if header != expected:
        fails.append(f"columns {header} != expected {expected}")
    bad = sorted(set(header) & FORBIDDEN_COLUMNS)
    if bad:
        fails.append(f"forbidden columns present: {bad}")
    return fails


def check_scope(rows, blocked):
    fails = []
    for r in rows:
        host = host_of(r.get("url") or "")
        if host_denied(host, blocked):
            fails.append(f"{r['id']}: url host {host or '(none)'} is not a permitted source")
        elif bare_domain(r.get("broker_domain")) in blocked:
            fails.append(f"{r['id']}: broker {r.get('broker_domain')} is on broker_block")
    return fails


def check_numbers(rows):
    fails, seen = [], set()
    for r in rows:
        try:
            n = int(r["listing_number"])
        except (TypeError, ValueError):
            fails.append(f"{r['id']}: listing_number missing")
            continue
        if n != listing_number(r["id"]):
            fails.append(f"{r['id']}: listing_number {n} != {listing_number(r['id'])}")
        if n in seen:
            fails.append(f"{r['id']}: duplicate listing_number {n}")
        seen.add(n)
        if r.get("dealledger_url") != f"https://dealledger.org/listing/{n}":
            fails.append(f"{r['id']}: dealledger_url {r.get('dealledger_url')}")
    return fails


def check_age_consistency(rows, today):
    fails, start = [], _d(RECORD_START)
    for r in rows:
        b, ad, la = r.get("age_basis"), _d(r.get("age_date")), _d(r.get("listed_after"))
        tag = r["id"]
        if b not in AGE_BASES:
            fails.append(f"{tag}: age_basis {b!r}")
            continue
        if not ad:
            fails.append(f"{tag}: no age_date")
            continue
        if not (start <= ad <= today):
            fails.append(f"{tag}: age_date {ad} outside {start}..{today}")
        if b == "window":
            if not la or not (la < ad) or (ad - la).days > WINDOW_MAX_DAYS:
                fails.append(f"{tag}: bad window {la}..{ad}")
        elif la:
            fails.append(f"{tag}: listed_after set on a {b} row")
        ls = _d(r.get("last_seen"))
        if ls and ls < ad:
            fails.append(f"{tag}: last_seen {ls} before age_date {ad}")
    return fails


def check_age_matches(rows, ages, label):
    fails = []
    for r in rows:
        a = ages.get(int(r["listing_number"]))
        if not a:
            fails.append(f"{r['id']}: no {label} row for {r['listing_number']}")
            continue
        want = (a.get("age_basis"), str(a.get("age_date") or "")[:10], str(a.get("listed_after") or "")[:10])
        got = (r.get("age_basis"), str(r.get("age_date") or "")[:10], str(r.get("listed_after") or "")[:10])
        if want != got:
            fails.append(f"{r['id']}: export {got} != {label} {want}")
    return fails


def check_source_rows(rows, current, want_status, require_published):
    fails = []
    for r in rows:
        c = current.get(r["id"])
        if not c:
            fails.append(f"{r['id']}: not in listings_direct")
            continue
        if c["status"] != want_status or c["source"] != "broker_direct":
            fails.append(f"{r['id']}: status={c['status']} source={c['source']}")
        if require_published and c["published"] is not True:
            fails.append(f"{r['id']}: published={c['published']}")
        if c["url"] != r.get("url") or (c.get("broker_domain") or "") != (r.get("broker_domain") or ""):
            fails.append(f"{r['id']}: url/domain differ from listings_direct")
    return fails


# ---------------------------------------------------------------- main
def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        rows = [{k: (v if v != "" else None) for k, v in row.items()} for row in rd]
        return rd.fieldnames or [], rows


def verify(folder, db, today=None):
    today = today or datetime.now(timezone.utc).date()
    rep = Report()
    folder = Path(folder)

    header, rows = read_csv(folder / "listings.csv")
    lheader, left = read_csv(folder / "listings_left_observation.csv")
    meta = json.loads((folder / "meta.json").read_text())
    with open(folder / "listings.json", encoding="utf-8") as f:
        jrows = json.load(f)

    blocked = {bare_domain(r["broker_domain"]) for r in db.all("broker_block", "broker_domain", "broker_domain")}
    ids = [r["id"] for r in rows] + [r["id"] for r in left]
    current = {c["id"]: c for c in db.by_values(
        "listings_direct", "id", ids, "id,status,published,source,url,broker_domain")}
    numbers = [int(r["listing_number"]) for r in rows + left if r.get("listing_number")]
    ages = {int(a["listing_number"]): a for a in db.by_values(
        "listing_age_v2", "listing_number", numbers, "listing_number,age_basis,age_date,listed_after", chunk=300)}
    page = {int(p["listing_number"]): {"age_basis": p["dom_basis"], "age_date": p["estimated_listed_date"],
                                       "listed_after": p["listed_after"]}
            for p in db.by_values("mv_listings_page_v2", "listing_number", [int(r["listing_number"]) for r in rows
                                  if r.get("listing_number")],
                                  "listing_number,dom_basis,estimated_listed_date,listed_after", chunk=300)}
    exits = {r["id"] for r in db.by_values("v_listings_left_observation", "id", [r["id"] for r in left], "id")}
    site = db.get("public_stats", {"select": "listings_on_site", "limit": "1"})[0]["listings_on_site"]

    rep.add("P1", "listings.csv columns are the published schema", check_columns(header, FIELDS), 1)
    rep.add("P1b", "left-observation columns are the published schema", check_columns(lheader, LEFT_FIELDS), 1)
    j_fail = [] if len(jrows) == len(rows) and (not jrows or list(jrows[0].keys()) == FIELDS) else \
        [f"listings.json has {len(jrows)} rows / keys differ from CSV"]
    rep.add("P1c", "listings.json matches listings.csv", j_fail, 1)
    rep.add("P2", "every row is active, published, broker-direct in listings_direct now",
            check_source_rows(rows, current, "active", True), len(rows))
    rep.add("P3", "only permitted sources (no marketplace/aggregator/digital/real-estate/MLS host, no blocked broker)",
            check_scope(rows, blocked), len(rows))
    rep.add("P4", "listing numbers unique and match the database formula", check_numbers(rows), len(rows))
    rep.add("A1", "age evidence equals listing_age_v2", check_age_matches(rows, ages, "listing_age_v2"), len(rows))
    rep.add("A2", "age evidence internally consistent", check_age_consistency(rows, today), len(rows))
    on_site = [r for r in rows if int(r["listing_number"]) in page]
    rep.add("A3", "age evidence equals what the website shows (mv_listings_page_v2)",
            check_age_matches(on_site, page, "mv_listings_page_v2"), len(on_site))

    lf = check_source_rows(left, current, "inactive", False)
    lf += [f"{r['id']}: not in v_listings_left_observation (no three-crawl evidence)" for r in left if r["id"] not in exits]
    lf += check_scope(left, blocked)
    lf += check_numbers(left)
    lf += check_age_matches(left, ages, "listing_age_v2")
    lf += [f"{r['id']}: reason says sold" for r in left if SOLD_WORDS.search(r.get("left_observation_reason") or "")]
    rep.add("L1", "left-observation rows carry three-crawl evidence and permitted sources", lf, len(left))

    s1 = []
    if len(rows) < MIN_ROWS:
        s1.append(f"only {len(rows)} rows (minimum {MIN_ROWS})")
    if site and abs(len(rows) - site) / site > SITE_TOLERANCE:
        s1.append(f"{len(rows)} rows vs {site} listings on the site (> {int(SITE_TOLERANCE * 100)}% apart)")
    rep.add("S1", "row count plausible against the site", s1, 1)
    s2 = []
    if meta.get("rows") != len(rows):
        s2.append(f"meta rows {meta.get('rows')} != {len(rows)}")
    if meta.get("left_observation_rows") != len(left):
        s2.append(f"meta left rows {meta.get('left_observation_rows')} != {len(left)}")
    if _d(meta.get("generated_at")) != today:
        s2.append(f"meta generated_at {meta.get('generated_at')} is not today")
    rep.add("S2", "meta.json agrees with the files", s2, 1)

    return rep, {"rows": len(rows), "left_observation_rows": len(left), "site_listings": site,
                 "rows_on_site": len(on_site)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default="data/public")
    args = ap.parse_args()
    base, key = os.environ.get("SUPABASE_URL", ""), os.environ.get("SUPABASE_SERVICE_KEY")
    if not base or not key:
        sys.exit("Set SUPABASE_URL and SUPABASE_SERVICE_KEY.")

    rep, counts = verify(args.dir, Db(base, key))
    out = {
        "verified_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "passed": rep.passed, **counts, "checks": rep.checks,
    }
    Path(args.dir, "verification.json").write_text(json.dumps(out, indent=2) + "\n")
    for c in rep.checks:
        mark = "PASS" if c["passed"] else "FAIL"
        print(f"{mark} {c['check']:4} {c['name']} ({c['failures']} of {c['checked']})")
        for e in c["examples"]:
            print(f"       {e}")
    if not rep.passed:
        print("::error::Public export failed verification; nothing will be committed.")
        sys.exit(1)
    print("Public export verified.")


if __name__ == "__main__":
    main()
