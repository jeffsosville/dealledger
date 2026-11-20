"""
petroleumrealty_scraper.py — petroleumrealty.net (Petroleum Realty, Gus Otaki;
gas stations and c-stores, Inland Empire / Southern California).

WHY A SPECIALIZED SCRAPER
The site is Weebly. Around 2026-11-13 it was rebuilt: the per-listing pages
(/arco-am-pm-with-real-estate.html, /mobil-station-business-only.html, ...)
went away and /listings.html became one page of blocks, one per property:

    <h2 class="wsite-content-title">RIVERSIDE PRIME FREEWAY GAS STATION</h2>
    <div class="paragraph">
      Asking Price: $6.1M — With existing 76 fuel supply agreement
      Alternative Price: ...      Offer Status: ... (optional)
      Location: 1395 W Blaine Street, Riverside, CA 92507
      Property Size / Building Size / Property Type / Development Status
      Property Highlights: <ul><li>...</li></ul>
    </div>

The V6 generic crawl changed extraction pattern with the redesign and wrote
each block's LAST bullet as a listing title ("Potential rebranding and
additional revenue opportunities", "Approximately $950,000 seller loan to be
assumed by buyer"), saw 4 of 6 blocks, and left the 8 rows of the old per-page
layout active (crawls closed 'partial', so the absence rule never retired
them): 12 active rows / 8 URLs for a broker showing 6. That is the "two
writers": one writer (V6), two extraction patterns, two identity schemes.

WHAT THIS EMITS (format_listing shape, one row per block)
  title       the <h2> heading, as printed
  price       Asking Price ($6.1M -> 6,100,000); the first figure only
  city/state  from Location ("..., Riverside, CA 92507" -> Riverside, CA)
  status      active; "Offer Status:" is kept verbatim in broker_status_text
  description Property Type / Size lines + the highlights
  vertical    not set here (scrapers/broker_vertical.py decides, one rule)

NOT EMITTED: a block with a "Development Status:" field or "DEVELOPMENT" in
its heading (ADELANTO COMMERCIAL DEVELOPMENT: entitled land, no operating
business) - land-only is out of scope (Nov 11 rule). Logged, not silent.

IDENTITY
No listing numbers or detail links, so the key is a slug of the heading:
  https://petroleumrealty.net/listings.html#pr-<slug(heading)>
Never the price (principle 4). A retitled block becomes a new row.
"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "petroleumrealty.net"
INDEX_URL = "https://petroleumrealty.net/listings.html"

_ZW = re.compile(r"[​‌‍﻿\xa0]")
_FIELD = re.compile(r"^([A-Z][A-Za-z ]{2,30}):\s*(.*)$")
_STATE_ZIP = re.compile(r",\s*([^,]+?),\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?\s*$")
_NOT_A_BUSINESS = re.compile(r"\bdevelopment\b", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", _ZW.sub(" ", s or "")).strip()


def _slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


def _money(s: str) -> Optional[float]:
    m = re.search(r"\$\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*([kKmM])?\b", s or "")
    if not m:
        return None
    v = float(m.group(1).replace(",", ""))
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


def _location(loc: str):
    """'1395 W Blaine Street, Riverside, CA 92507' -> ('Riverside', 'CA');
    'Hemet, CA' -> ('Hemet', 'CA')."""
    loc = _clean(loc)
    m = _STATE_ZIP.search(loc)
    if m:
        return m.group(1).strip(), m.group(2)
    m = re.match(r"^([^,]+),\s*([A-Z]{2})\b", loc)
    if m:
        return m.group(1).strip(), m.group(2)
    return None, None


def parse_blocks(html: str) -> List[Dict]:
    """Page HTML -> one dict per <h2> block, in page order (all blocks,
    including the ones to_rows() does not emit)."""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for h2 in soup.select("h2.wsite-content-title"):
        title = _clean(h2.get_text(" "))
        # the block's text: every element up to the next h2 heading
        fields, highlights = {}, []
        for el in h2.find_all_next():
            if el.name == "h2" and "wsite-content-title" in (el.get("class") or []):
                break
            if el.name != "div" or "paragraph" not in (el.get("class") or []):
                continue
            for li in el.find_all("li"):
                t = _clean(li.get_text(" "))
                t = re.sub(r"\s*Contact Petroleum Realty for additional information\.?$", "", t).strip()
                if t:
                    highlights.append(t)
                li.decompose()
            for br in el.find_all("br"):
                br.replace_with("\n")
            for line in el.get_text("").split("\n"):
                m = _FIELD.match(_clean(line))
                if m:
                    fields[m.group(1).strip().lower()] = m.group(2).strip()
        if "asking price" not in fields:
            continue                             # page intro heading, not a property
        out.append({"title": title, "fields": fields, "highlights": highlights})
    return out


def to_rows(blocks: List[Dict], account: str = "petroleumrealty", log=print) -> List[Dict]:
    rows, seen = [], {}
    for b in blocks:
        f = b["fields"]
        if "development status" in f or _NOT_A_BUSINESS.search(b["title"]):
            log(f"[petroleumrealty] not emitted (development / land): {b['title']}")
            continue
        key = _slug(b["title"])
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:
            key += f"-{seen[key]}"
        city, state = _location(f.get("location", ""))
        facts = [f"{k.title()}: {v}" for k, v in f.items()
                 if k not in ("asking price", "location", "property highlights", "offer status") and v]
        row = format_listing(
            f"{INDEX_URL}#pr-{key}",
            account,
            title=b["title"],
            price=_money(f.get("asking price")),
            price_text=f.get("asking price") or None,
            city=city,
            state=state,
            location=_clean(f.get("location", "")) or None,
            description="; ".join(facts + b["highlights"]) or None,
            status="active",
        )
        if f.get("offer status"):
            row["broker_status_text"] = f["offer status"]
        rows.append(row)
    return rows


def parse_page(html: str, account: str = "petroleumrealty", log=print) -> List[Dict]:
    return to_rows(parse_blocks(html), account, log)


class PetroleumRealtyScraper:
    """run_specialized entry: PetroleumRealtyScraper().scrape(account)."""

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "petroleumrealty") -> List[Dict]:
        html = self.fetch()
        blocks = parse_blocks(html)
        rows = to_rows(blocks, account)
        print(f"[petroleumrealty] {len(blocks)} property blocks on the page, {len(rows)} emitted, "
              f"{sum(1 for r in rows if r.get('price'))} priced, "
              f"{sum(1 for r in rows if r.get('state'))} with state")
        return rows
