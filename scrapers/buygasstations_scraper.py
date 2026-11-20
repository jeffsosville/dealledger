"""
buygasstations_scraper.py — buygasstations.com (Midtown Realty International,
Albert / Alex Diaz, "Gas Station Specialist", South Florida).

WHY A SPECIALIZED SCRAPER
GoDaddy Website Builder. Two pages of text blocks, one per property:
  /for-sale-business-only   (business-only sales)
  /for-sale-with-property   (station + real estate)
Each block is written the same way, after a few slideshow caption lines that
repeat OTHER blocks' headings with their prices:

    MARATHON GAS STATION BUSINESS ONLY  FOR SALE     <- heading, no "$"
    $1,300,000.00                                    <- price (or "$$ PRICE UPON REQUEST")
    Food-Mart/Carwash
    ID# Alh 669b2                                    <- broker ref, ends the block
    Located in Hialeah, FL

The V6 generic crawl crawled only the business-only page and took the
slideshow captions as cards: 3 rows / 2 URLs (one at /registration-forms),
priced from the wrong caption ($3,450,000 on a $1,000,000 Mobil), against 8
for sale on that page alone (2026-11-20).

WHAT THIS EMITS (one row per block whose heading is a gas station)
  url          <page>#dl-<slug(ID# ref)>   (broker ref = identity, principle 4)
  title        the heading
  price        the first "$" line after it (None for "PRICE UPON REQUEST")
  city/state   from "Located in <City>, FL"
  status       a "SOLD ..." line in the block -> sold, else active
NOT EMITTED (logged): lease offers, lots / land, and non-gas real estate
(strip center, free-standing building) - out of scope.
"""

import re
import time
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "buygasstations.com"
PAGES = (
    "https://buygasstations.com/for-sale-business-only",
    "https://buygasstations.com/for-sale-with-property",
)

_ZW = re.compile(r"[​‌‍﻿\xa0]")
_REF = re.compile(r"^ID#\s*(.+?)\s*$", re.I)
_HEADING = re.compile(r"^[A-Z0-9][A-Z0-9 ,.&/'\-]*\bFOR\s+(?:SALE|LEASE)\b[A-Z ]*$")
_PRICE = re.compile(r"^\$\s*(\d{1,3}(?:,\d{3})+(?:\.\d{2})?)")
_ON_REQUEST = re.compile(r"PRICE\s+UPON\s+REQUEST", re.I)
_SOLD = re.compile(r"^SOLD\b", re.I)
_LOCATED = re.compile(r"^Located in\s+(.+?),\s*(?:[A-Za-z .]*?)\b(FL)\b", re.I)
_GAS = re.compile(r"\bGAS\s+STATION\b|\b7-ELEVEN\b", re.I)
_LEASE_OR_LAND = re.compile(r"\bLEASE\b|\bLOT\b|\bACRE\b|\bLAND\b", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", _ZW.sub(" ", s or "")).strip()


def _slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


def page_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style", "noscript"]):
        t.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    return [l for l in (_clean(x) for x in soup.get_text("\n").split("\n")) if l]


def parse_blocks(lines: List[str]) -> List[Dict]:
    """Lines -> one dict per 'ID#' block: heading, price, ref, city, sold."""
    out, start = [], 0
    for i, line in enumerate(lines):
        m = _REF.match(line)
        if not m:
            continue
        block = lines[start:i]
        start = i + 1
        # heading: the LAST no-"$" FOR SALE / FOR LEASE line of the block
        # (the slideshow captions above it all carry a "$" price)
        h_idx = max((j for j, l in enumerate(block) if _HEADING.match(l)), default=None)
        heading = block[h_idx] if h_idx is not None else None
        # "PACKAGE OF 3 MOBIL /" + "7-ELEVEN GAS STATION ... FOR SALE": a
        # heading the page wraps after a trailing "/" is one heading
        if heading and h_idx and block[h_idx - 1].endswith("/") and "$" not in block[h_idx - 1]:
            heading = f"{block[h_idx - 1]} {heading}"
        after = block[h_idx + 1:] if h_idx is not None else block
        price, on_request = None, False
        for l in after:
            pm = _PRICE.match(l)
            if pm:
                price = float(pm.group(1).replace(",", ""))
                break
            if _ON_REQUEST.search(l):
                on_request = True
                break
        city = state = None
        for l in lines[i + 1:i + 3]:
            lm = _LOCATED.match(l)
            if lm:
                city, state = lm.group(1).strip().title(), lm.group(2).upper()
                break
        out.append({
            "ref": m.group(1).strip(),
            "heading": heading,
            "price": price,
            "price_on_request": on_request,
            "city": city,
            "state": state,
            "sold": any(_SOLD.match(l) for l in block),
            "facts": [l for l in after if not _PRICE.match(l)][:4],
        })
    return out


def to_rows(blocks: List[Dict], page_url: str, account: str = "buygasstations",
            log=print) -> List[Dict]:
    rows, seen = [], set()
    for b in blocks:
        h = b["heading"] or ""
        if not h:
            log(f"[buygasstations] no heading for ID# {b['ref']} - not emitted")
            continue
        if _LEASE_OR_LAND.search(h):
            log(f"[buygasstations] not emitted (lease / land): {h}")
            continue
        if not _GAS.search(h):
            log(f"[buygasstations] not emitted (not a gas station): {h}")
            continue
        key = _slug(b["ref"])
        if key in seen:
            continue
        seen.add(key)
        title = re.sub(r"\bPROPRTY\b", "PROPERTY", h)
        rows.append(format_listing(
            f"{page_url}#dl-{key}",
            account,
            title=title,
            price=b["price"],
            price_text="Price upon request" if b["price_on_request"] else None,
            city=b["city"],
            state=b["state"] or "FL",
            location=", ".join(x for x in (b["city"], b["state"] or "FL") if x),
            description="; ".join(b["facts"] + [f"ID# {b['ref']}"]) or None,
            status="sold" if b["sold"] else "active",
        ))
    return rows


def parse_page(html: str, page_url: str, account: str = "buygasstations", log=print) -> List[Dict]:
    return to_rows(parse_blocks(page_lines(html)), page_url, account, log)


class BuyGasStationsScraper:
    """run_specialized entry: BuyGasStationsScraper().scrape(account)."""

    def fetch(self, url: str) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(url, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "buygasstations") -> List[Dict]:
        rows = []
        for i, url in enumerate(PAGES):
            if i:
                time.sleep(2)
            got = parse_page(self.fetch(url), url, account)
            print(f"[buygasstations] {url}: {len(got)} emitted")
            rows.extend(got)
        print(f"[buygasstations] {len(rows)} listings "
              f"({sum(1 for r in rows if r['status'] == 'active')} active)")
        return rows
