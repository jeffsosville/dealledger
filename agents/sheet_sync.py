#!/usr/bin/env python3
"""
sheet_sync.py - feed the team's broker review sheet into broker_sources.

The team works a public Google Sheet (one row per broker, columns as exported by
the broker-pages-to-find report). This reads each tab as CSV and applies ONLY the
found_listings_url column (N), so the next broker_scrape run crawls the page:

  stage 2_listings_url_known  -> 3_crawlable, listing_url = found URL
  stage 3_crawlable/4_producing -> listing_url = found URL (stage unchanged)

Never applied (written to the report for Jeff instead):
  - rows at 0_blocked / 0_unusable / 1_needs_discovery, or domains in broker_block
  - a found URL on another domain (off-domain is never broker-direct by itself)
  - a cell that isn't a URL ("No listings", "Site not working", "NA" ...)
  - real-estate-looking URLs or domains (hard rule; a human decides)
  - team_notes (BizBuySell only, real estate, no listings ...) - report only

Every applied row gets decided_by='human' (the team made the call) and
decision_reason 'team sheet <date>: ...'. Re-running is a no-op for rows whose
listing_url already matches. Writes sheet_sync_<date>.csv with every decision.

  python3 agents/sheet_sync.py             # dry run: report only
  python3 agents/sheet_sync.py --write     # apply

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY
     SHEET_ID   (default: the broker-pages-to-find sheet)
     SHEET_GIDS (comma-separated tab gids, default 1410634222)
The sheet must be shared "Anyone with the link can view".
"""
import argparse
import csv
import io
import os
import re
import sys
from datetime import date, datetime, timezone
from urllib.parse import urlparse

import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SHEET_ID = os.environ.get("SHEET_ID", "1N2V6WB-Knr2VhFFQL5CTa3eFQ7iB_6iU653ItBccmFA")
SHEET_GIDS = [g.strip() for g in os.environ.get("SHEET_GIDS", "1410634222").split(",") if g.strip()]

RE_WORDS = re.compile(r"real[- ]?estate|realty|realtor|propert|apartment|\bcre\b|-cre\.|"
                      r"commercial[- ]?lease|\bidx\b|homes-for-sale|mls", re.I)
URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.I)
HOLD = set()
REPORT_FIELDS = ["tab", "row", "domain", "stage", "action", "listing_url_before",
                 "listing_url_after", "why", "team_notes"]


HOLD_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "sheet_sync_hold.txt")


def load_hold():
    """Domains Jeff is deciding by hand - never auto-applied (one per line, # comments)."""
    try:
        with open(HOLD_FILE) as f:
            return {bare(l.split("#")[0].strip()) for l in f if l.split("#")[0].strip()}
    except FileNotFoundError:
        return set()


def norm(u):
    """Compare URLs without scheme, www, fragment or trailing slash."""
    u = (u or "").strip().split("#", 1)[0].lower()
    u = re.sub(r"^https?://(www\.)?", "", u)
    return u.rstrip("/")


def bare(host):
    return (host or "").lower().split(":")[0].removeprefix("www.")


def sb_headers():
    return {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"}


def read_tab(gid):
    url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={gid}"
    r = requests.get(url, timeout=60, allow_redirects=True)
    if r.status_code != 200 or "text/csv" not in r.headers.get("content-type", ""):
        sys.exit(f"Sheet tab {gid} not readable as CSV (HTTP {r.status_code}). "
                 "Is it shared 'Anyone with the link can view'?")
    return list(csv.DictReader(io.StringIO(r.content.decode("utf-8-sig"))))


def clean_url(cell):
    """First URL in the cell, trimmed; fragments dropped (#listings is the same page)."""
    m = URL_RE.search(cell or "")
    if not m:
        return None
    u = m.group(0).strip().rstrip(".,;)")
    return u.split("#", 1)[0]


def load_sources(domains):
    out = {}
    doms = sorted(domains)
    for i in range(0, len(doms), 80):
        chunk = doms[i:i + 80]
        variants = chunk + [f"www.{d}" for d in chunk]
        r = requests.get(f"{SUPABASE_URL}/rest/v1/broker_sources", headers=sb_headers(),
                         params={"select": "id,domain,listing_url,discovery_stage",
                                 "domain": f"in.({','.join(variants)})"}, timeout=60)
        r.raise_for_status()
        for row in r.json():
            out.setdefault(bare(row["domain"]), []).append(row)
    return out


def load_blocked(domains):
    doms = sorted(domains)
    blocked = set()
    for i in range(0, len(doms), 80):
        chunk = doms[i:i + 80]
        r = requests.get(f"{SUPABASE_URL}/rest/v1/broker_block", headers=sb_headers(),
                         params={"select": "broker_domain",
                                 "broker_domain": f"in.({','.join(chunk)})"}, timeout=60)
        r.raise_for_status()
        blocked |= {bare(x["broker_domain"]) for x in r.json()}
    return blocked


def decide(row, sources, blocked):
    """Return (action, new_url, why). action in apply_promote, apply_url, skip, report."""
    domain = bare(row.get("domain"))
    cell = (row.get("found_listings_url") or "").strip()
    notes = (row.get("team_notes") or "").strip()
    if not domain or not (cell or notes):
        return None
    if not cell:
        return ("report", None, "team note only")
    url = clean_url(cell)
    if not url:
        return ("report", None, f"not a URL: {cell[:60]}")
    host = bare(urlparse(url).hostname)
    if host != domain and not host.endswith("." + domain):   # subdomains are the same broker
        return ("report", None, f"off-domain: {urlparse(url).hostname}")
    if domain in blocked:
        return ("report", None, "domain is on broker_block")
    if domain in HOLD:
        return ("report", None, "on data/sheet_sync_hold.txt - Jeff decides")
    if RE_WORDS.search(f"{domain} {url}"):
        return ("report", None, "reads like real estate - human decides")
    rows = sources.get(domain)
    if not rows:
        return ("report", None, "domain not in broker_sources")
    stage = rows[0]["discovery_stage"]
    same = all(norm(r.get("listing_url")) == norm(url) for r in rows)
    if stage == "2_listings_url_known":
        return ("apply_promote", url, "team verified listings page")
    if stage in ("3_crawlable", "4_producing"):
        if same:
            return ("skip", None, "listing_url already matches")
        return ("apply_url", url, "team corrected listings URL")
    return ("report", None, f"stage {stage} - not changed automatically")


def apply(row_ids, body):
    r = requests.patch(f"{SUPABASE_URL}/rest/v1/broker_sources",
                       headers={**sb_headers(), "Prefer": "return=minimal"},
                       params={"id": f"in.({','.join(row_ids)})"}, json=body, timeout=60)
    r.raise_for_status()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--out", default=f"sheet_sync_{date.today()}.csv")
    a = ap.parse_args()
    if not (SUPABASE_URL and SUPABASE_KEY):
        sys.exit("SUPABASE_URL / SUPABASE_SERVICE_KEY not set")

    HOLD.update(load_hold())
    tabs = {gid: read_tab(gid) for gid in SHEET_GIDS}
    domains = {bare(r.get("domain")) for rows in tabs.values() for r in rows if r.get("domain")}
    sources, blocked = load_sources(domains), load_blocked(domains)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    report, counts, seen = [], {}, set()
    for gid, rows in tabs.items():
        for n, row in enumerate(rows, 2):
            d = decide(row, sources, blocked)
            if not d:
                continue
            action, url, why = d
            domain = bare(row.get("domain"))
            if action.startswith("apply") and domain in seen:
                action, why = "skip", "already applied from another row/tab"
            src = sources.get(domain) or [{}]
            if action.startswith("apply"):
                seen.add(domain)
                body = {"listing_url": url, "decided_by": "human", "decided_at": ts,
                        "updated_at": ts, "decision_reason": f"team sheet {date.today()}: {why}"}
                if action == "apply_promote":
                    body.update(discovery_stage="3_crawlable", strategy_status="ready")
                if a.write:
                    apply([s["id"] for s in src], body)
            counts[action] = counts.get(action, 0) + 1
            report.append({"tab": gid, "row": n, "domain": domain,
                           "stage": src[0].get("discovery_stage", ""), "action": action,
                           "listing_url_before": src[0].get("listing_url", ""),
                           "listing_url_after": url or "", "why": why,
                           "team_notes": (row.get("team_notes") or "")[:200]})

    with open(a.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=REPORT_FIELDS)
        w.writeheader()
        w.writerows(report)
    mode = "APPLIED" if a.write else "DRY RUN"
    print(f"{mode}: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())) + f" -> {a.out}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(f"### Team sheet sync ({mode})\n\n| action | rows |\n|---|---|\n")
            f.writelines(f"| {k} | {v} |\n" for k, v in sorted(counts.items()))
            held = [r for r in report if r["action"] == "report" and r["why"] != "team note only"]
            if held:
                f.write("\n**Held for a human:**\n\n")
                f.writelines(f"- {r['domain']}: {r['why']}\n" for r in held[:60])


if __name__ == "__main__":
    main()
