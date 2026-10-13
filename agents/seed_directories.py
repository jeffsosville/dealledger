#!/usr/bin/env python3
"""
seed_directories.py - pull broker websites out of association directories and
load the new ones into broker_sources at 1_needs_discovery.

Why: stage 1 is empty. Every existing seed list is used up: of the IBBA
directory's 1,386 domains only ~130 were not already in broker_sources (almost
all realtors, banks, lawyers and franchise office pages), and the broker
profile list and Data Axle file are fully loaded. New brokers have to come from
new lists. Association and member directories are discovery seeds ONLY - never a
listing source (hard rule).

Input: data/seed_directories.csv  (tag,url,notes). One row per directory page:
a state page (masource.org/state/texas/) or an association home or
find-a-broker page. For each start URL the script:
  1. fetches it and, if it links to a "find a broker / member directory" page
     on the same site, follows that one hop;
  2. follows same-site pagination (?page=N, /page/N/) up to MAX_PAGES;
  3. follows same-site member/profile links (up to MAX_PROFILES per start
     URL) and reads the firm website from each profile;
  4. keeps off-site links that look like a firm website - not social, maps,
     marketplaces, franchise corporate sites, .gov/.edu - and that are not
     already in broker_sources or broker_block.

  python agents/seed_directories.py                    # -> seed_candidates_<date>.csv
  python agents/seed_directories.py --only masource_tx # one directory, to test
  python agents/seed_directories.py --load seed_candidates_2026-10-13.csv

--load inserts rows with keep != n as 1_needs_discovery, seed_source
'assoc_<tag>_<yyyy_mm>'. The twice-daily discovery job picks them up and the
gate decides 3_crawlable vs proposed as usual.

Env: SUPABASE_URL, SUPABASE_SERVICE_KEY. Run from the Mac.
"""

import argparse
import csv
import os
import re
import sys
import time
from datetime import date
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import discovery_v2 as dv  # noqa: E402

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
MAX_PAGES = int(os.environ.get("MAX_PAGES", "25"))
MAX_PROFILES = int(os.environ.get("MAX_PROFILES", "400"))
DELAY = float(os.environ.get("DELAY", "0.7"))  # be polite to association sites
HERE = os.path.dirname(os.path.abspath(__file__))
DIRECTORIES = os.path.join(HERE, "..", "data", "seed_directories.csv")

FIELDS = ["domain", "company", "tag", "found_on", "keep"]

NOT_FIRM = re.compile(
    r"(facebook|linkedin|twitter|x\.com$|instagram|youtube|youtu\.be|tiktok|pinterest|"
    r"google|goo\.gl|bit\.ly|tinyurl|apple\.com|microsoft|zoom\.us|calendly|"
    r"wildapricot|squarespace|wix|godaddy|wordpress\.com|mailchimp|constantcontact|"
    r"eventbrite|paypal|stripe|bizbuysell|bizquest|businessesforsale|businessbroker\.net|"
    r"dealstream|loopnet|crexi|axial|businessmart|bizben|flippa|acquire\.com|empireflippers|"
    r"ibba\.org|masource\.org|tabb\.org|mbbi\.org|gabb\.org|cabb\.org|bbfmls\.com|"
    r"realtor|zillow|redfin|exprealty|kw\.com|remax|coldwell|century21|sothebys|"
    r"\.gov$|\.edu$|\.mil$)", re.I)
FRANCHISE_CORP = ("tworld.com", "transworldba.com", "sunbeltnetwork.com", "fcbb.com",
                  "murphybusiness.com", "vrbusinessbrokers.com", "hedgestone.com",
                  "linkbusiness.com", "linkbusiness.us")
DIRECTORY_LINK = re.compile(r"(find[-_ ]?(a|an)?[-_ ]?(broker|advisor|member|intermediar)|"
                            r"member[-_ ]?(directory|search|list)|directory|our[-_ ]members|"
                            r"broker[-_ ]?(search|directory|list)|membership[-_ ]?list)", re.I)
PROFILE_LINK = re.compile(r"/(members?|profiles?|brokers?|broker-profile|advisors?|"
                          r"directory|sys/publicprofile|people|agent)s?/[\w%.-]{2,}", re.I)
PAGE_LINK = re.compile(r"([?&](page|pg|p|paged)=\d+|/page/\d+/?$)", re.I)


def bare(host):
    return (host or "").lower().strip(".").removeprefix("www.")


def get(url):
    time.sleep(DELAY)
    try:
        r = dv.get_page(url, timeout=25)
        if r.status_code >= 400:
            return None, str(r.url), f"HTTP {r.status_code}"
        return r.text, str(r.url), None
    except Exception as e:  # noqa: BLE001
        return None, url, f"{type(e).__name__}"


def links_of(html, base):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base, a["href"].strip()).split("#")[0]
        if href.startswith("http"):
            out.append((" ".join(a.get_text(" ").split())[:80], href))
    return out


def firm_links(links, site):
    for label, href in links:
        host = bare(urlparse(href).hostname)
        if not host or "." not in host or host == site or host.endswith("." + site):
            continue
        if NOT_FIRM.search(host) or any(host == c or host.endswith("." + c) for c in FRANCHISE_CORP):
            continue
        yield host, label


def existing_domains():
    out = set()
    for table, col in (("broker_sources", "domain"), ("broker_block", "broker_domain")):
        offset = 0
        while True:
            r = requests.get(f"{SUPABASE_URL}/rest/v1/{table}",
                             headers={"apikey": SUPABASE_KEY,
                                      "Authorization": f"Bearer {SUPABASE_KEY}"},
                             params={"select": col, "limit": "1000", "offset": str(offset)},
                             timeout=60)
            r.raise_for_status()
            rows = r.json()
            out |= {bare(x[col]) for x in rows if x.get(col)}
            if len(rows) < 1000:
                break
            offset += 1000
    return out


def crawl_directory(tag, start):
    site = bare(urlparse(start).hostname)
    found, errors = {}, []
    html, final, err = get(start)
    if not html:
        return found, [f"{start}: {err}"]
    links = links_of(html, final)
    pages = [(final, links)]

    # one hop to a find-a-broker / member directory page, if the start page isn't one
    if not any(PROFILE_LINK.search(u) for _, u in links if bare(urlparse(u).hostname) == site):
        for label, u in links:
            if bare(urlparse(u).hostname) == site and DIRECTORY_LINK.search(f"{label} {u}"):
                h, f2, e = get(u)
                if h:
                    pages.append((f2, links_of(h, f2)))
                    break

    # pagination
    seen_pages = {p for p, _ in pages}
    i = 0
    while i < len(pages) and len(pages) < MAX_PAGES:
        _, lk = pages[i]
        for _, u in lk:
            if bare(urlparse(u).hostname) == site and PAGE_LINK.search(u) and u not in seen_pages:
                seen_pages.add(u)
                h, f2, e = get(u)
                if h:
                    pages.append((f2, links_of(h, f2)))
                if len(pages) >= MAX_PAGES:
                    break
        i += 1

    profiles = []
    for page_url, lk in pages:
        for host, label in firm_links(lk, site):
            found.setdefault(host, (label, page_url))
        for label, u in lk:
            if bare(urlparse(u).hostname) == site and PROFILE_LINK.search(urlparse(u).path) \
                    and u not in profiles and u not in seen_pages:
                profiles.append(u)

    for u in profiles[:MAX_PROFILES]:
        h, f2, e = get(u)
        if not h:
            errors.append(f"{u}: {e}")
            continue
        title = BeautifulSoup(h, "html.parser").title
        title = " ".join(title.get_text().split())[:80] if title else ""
        for host, label in firm_links(links_of(h, f2), site):
            found.setdefault(host, (title or label, u))
    print(f"  {tag}: {len(pages)} pages, {min(len(profiles), MAX_PROFILES)} profiles, "
          f"{len(found)} off-site firm domains")
    return found, errors


def run(out_path, only):
    with open(DIRECTORIES) as f:
        dirs = [r for r in csv.DictReader(f) if r.get("url") and not r["tag"].startswith("#")]
    if only:
        dirs = [d for d in dirs if d["tag"] in only]
    known = existing_domains()
    print(f"{len(dirs)} directories; {len(known)} domains already known")
    rows, errors = {}, []
    for d in dirs:
        found, errs = crawl_directory(d["tag"], d["url"])
        errors += errs
        for host, (label, page) in found.items():
            if host in known or host in rows:
                continue
            rows[host] = {"domain": host, "company": label, "tag": d["tag"],
                          "found_on": page, "keep": ""}
    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in sorted(rows.values(), key=lambda r: (r["tag"], r["domain"])):
            w.writerow(r)
    print(f"\n{len(rows)} new candidate domains -> {out_path}")
    if errors:
        print(f"{len(errors)} fetch errors, first few:")
        for e in errors[:10]:
            print("  ", e)


def load(path):
    with open(path) as f:
        rows = [r for r in csv.DictReader(f) if (r.get("keep") or "").strip().lower() != "n"]
    known = existing_domains()
    stamp = date.today().strftime("%Y_%m")
    payload = [{
        "domain": r["domain"], "company_name": r["company"] or None,
        "listing_url": f"https://{r['domain']}/", "homepage_url": f"https://{r['domain']}/",
        "seed_source": f"assoc_{r['tag']}_{stamp}", "discovery_stage": "1_needs_discovery",
        "strategy_status": "pending", "notes": f"seed from {r['found_on']}"[:300],
    } for r in rows if bare(r["domain"]) not in known]
    print(f"Loading {len(payload)} new domains into stage 1 ({len(rows) - len(payload)} already known)")
    for i in range(0, len(payload), 200):
        r = requests.post(f"{SUPABASE_URL}/rest/v1/broker_sources",
                          headers={"apikey": SUPABASE_KEY,
                                   "Authorization": f"Bearer {SUPABASE_KEY}",
                                   "Content-Type": "application/json",
                                   "Prefer": "return=minimal"},
                          json=payload[i:i + 200], timeout=60)
        if not r.ok:
            sys.exit(f"insert failed: {r.status_code} {r.text[:300]}")
    print("done")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", default=f"seed_candidates_{date.today()}.csv")
    ap.add_argument("--only", default="", help="comma-separated tags from seed_directories.csv")
    ap.add_argument("--load", metavar="CSV")
    a = ap.parse_args()
    if not SUPABASE_URL or not SUPABASE_KEY:
        sys.exit("Missing SUPABASE_URL / SUPABASE_SERVICE_KEY")
    if a.load:
        load(a.load)
    else:
        run(a.out, {t.strip() for t in a.only.split(",") if t.strip()})


if __name__ == "__main__":
    main()
