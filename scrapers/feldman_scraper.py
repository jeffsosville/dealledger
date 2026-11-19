"""
feldman_scraper.py — feldmanbrokerage.com (Feldman Brokerage and Consulting, LLC;
Eric Feldman, laundromat specialist, NY/NJ/PA and national).

WHY A SPECIALIZED SCRAPER
The site is Weebly. Its one listings page (/buy-a-laundromat.html) is a single
column of text blocks, one per laundromat, each written the same way:

    Hi producing coin laundromat (NY)        <- title, state in parentheses
    5,200 sq ft / 126 machines / ...         <- free-form facts
    Revenue.   610,600
    Net             231,048
    Town.        BROOKLYN                    (often blank)
    County.        KINGS
    State.              NY                   (blank for D.C.)
    Selling price.   $1,039,000              <- ends the block ("$" optional)

The V6 generic crawl read each text line as a card, so DealLedger held 57
'active' rows titled "Net 96,305", "Selling price. $1,299,000" and the like,
none with a usable price or state. On 2026-11-19 the page held 64 listings.

WHAT THIS EMITS (format_listing shape, one row per block)
  title        the title line ("Hi producing coin laundromat (NY)")
  price        Selling price
  revenue      Revenue
  cash_flow    Net (the broker's net to owner), None when blank
  city/state   Town (title-cased) and State; state falls back to the title's
               "(XX)", "(WASH DC)" -> DC
  location     "Town, ST" or "<County> County, ST"
  status       active: the page shows no sold/pending markers
  description  the facts lines, then County

IDENTITY
No listing numbers or links, so the key is a slug of the title plus county:
  https://feldmanbrokerage.com/buy-a-laundromat.html#fb-<title>-<county>
A changed price keeps the same row (price_history records it); a retitled
listing becomes a new row. Retire the V6 rows once with
sql/2026-11-19_feldman_supersede.sql.
"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "feldmanbrokerage.com"
INDEX_URL = "https://feldmanbrokerage.com/buy-a-laundromat.html"

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
    "VT", "VA", "WA", "WV", "WI", "WY",
}

_ZW = re.compile(r"[​‌‍﻿\xa0]")
_TITLE = re.compile(r"\(\s*(wash\.?\s*d\.?c\.?|d\.?c\.?|[a-z]{2})\s*\)\s*$", re.I)
_FIELD = re.compile(r"^(revenue|net|town|county|state|selling\s*price)\b\s*\.?\s*(.*)$", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", _ZW.sub(" ", s)).strip()


def _money(s: str) -> Optional[float]:
    s = (s or "").replace(" ", "")
    m = re.search(r"\$?(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)([kKmM])?", s)
    if not m:
        return None
    v = float(m.group(1).replace(",", ""))
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


def _title_state(title: str) -> Optional[str]:
    m = _TITLE.search(title)
    if not m:
        return None
    code = re.sub(r"[^a-z]", "", m.group(1).lower())
    if code.endswith("dc"):
        return "DC"
    code = code.upper()
    return code if code in US_STATES else None


def _slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


def parse_lines(lines: List[str]) -> List[Dict]:
    """Page text lines -> one dict per listing block, in page order."""
    lines = [_clean(l) for l in lines]
    lines = [l for l in lines if l]
    out, block = [], []
    for line in lines:
        block.append(line)
        if re.match(r"^selling\s*price\b", line, re.I):
            parsed = _parse_block(block)
            if parsed:
                out.append(parsed)
            block = []
    return out


def _parse_block(block: List[str]) -> Optional[Dict]:
    # The title is the first "(XX)" line; anything above it (site nav, phone
    # number, stray text) is not part of the listing.
    start = next((i for i, l in enumerate(block) if _TITLE.search(l) and not _FIELD.match(l)), None)
    if start is None:
        return None
    title = block[start]
    fields, facts = {}, []
    for line in block[start + 1:]:
        m = _FIELD.match(line)
        if m:
            key = re.sub(r"\s+", " ", m.group(1).lower())
            fields[key] = m.group(2).strip(" .")
        else:
            facts.append(line)
    state_field = (fields.get("state") or "").upper().replace(".", "").strip()
    state = state_field if state_field in US_STATES else _title_state(title)
    town = fields.get("town") or ""
    if town.replace(".", "").upper() == "DC":
        town, state = "Washington", state or "DC"
    city = town.title() if town else None
    county = fields.get("county") or ""
    county = re.sub(r"\s+county$", "", county, flags=re.I).title() or None
    return {
        "title": title,
        "price": _money(fields.get("selling price")),
        "revenue": _money(fields.get("revenue")),
        "net": _money(fields.get("net")),
        "city": city,
        "county": county,
        "state": state,
        "facts": facts,
    }


def page_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("#wsite-content") or soup.body or soup
    for br in root.find_all("br"):
        br.replace_with("\n")
    return root.get_text("\n").split("\n")


def to_rows(parsed: List[Dict], account: str = "feldman") -> List[Dict]:
    rows, seen = [], {}
    for p in parsed:
        key = _slug(p["title"]) + ("-" + _slug(p["county"]) if p["county"] else "")
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:                                # same title + county twice
            key += f"-{seen[key]}"
        place = p["city"] or (f"{p['county']} County" if p["county"] else None)
        desc = "; ".join(p["facts"] + ([f"County: {p['county']}"] if p["county"] else []))
        rows.append(format_listing(
            f"{INDEX_URL}#fb-{key}",
            account,
            title=p["title"],
            price=p["price"],
            city=p["city"],
            state=p["state"],
            location=", ".join(x for x in (place, p["state"]) if x) or None,
            description=desc or None,
            business_type="Laundromat",
            revenue=p["revenue"],
            cash_flow=p["net"],
            status="active",
        ))
    return rows


def parse_page(html: str, account: str = "feldman") -> List[Dict]:
    return to_rows(parse_lines(page_lines(html)), account)


class FeldmanScraper:
    """run_specialized entry: FeldmanScraper().scrape(account)."""

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "feldman") -> List[Dict]:
        rows = parse_page(self.fetch(), account)
        print(f"[feldman] {len(rows)} listings, "
              f"{sum(1 for r in rows if r.get('price'))} priced, "
              f"{sum(1 for r in rows if r.get('state'))} with state")
        return rows
