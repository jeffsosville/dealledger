"""
carwashking_scraper.py — CarWashKing.com (Vincent Carfora Broker, Inc.,
Hallandale Beach FL; car wash specialist, national).

WHY A SPECIALIZED SCRAPER
Discovery marked the site NO_LISTINGS_PAGE on 2026-11-03 ("/carwashes: soft
404"). Fetched with a browser fingerprint on 2026-11-20 the pages load fine.
The site has two listing surfaces, and they disagree:

  /carwashes?state=&page=N   "All Car Washes", 10 per page, newest id first.
                             On 2026-11-20 it walked 60 pages / 599 entries,
                             ids 1028 down to 1. It is the whole archive: it
                             has no status field, and only 4 of the entries
                             above id 900 carry any marker ("SOLD: ...",
                             "... For Lease ..."); older ones are marked
                             "SOLD..." / "NOT AVAILABLE..." or not at all.
                             Unmarked is NOT evidence of active.
  / (homepage)               the broker's current set: a featured slider
                             (li.item#itemid-N) and a latest-listings block
                             (div.item#itemid-N). 13 distinct ids on
                             2026-11-20: 1028 1027 1026 1025 1022 1021 1020
                             1012 1011 1010 1009 1008 1003.

The homepage also shows a counter "87 active listings" next to "50 in all
states", "44795 prospective buyers" and "1856 deals closed". It cannot be
reproduced from the index (367 unmarked entries, not 87), so it is not used
as the target count. See claude/staging/2026-11-20/t2-notes.md.

WHAT THIS EMITS
One row per homepage card: id, title, price ("Call for Price" -> None),
state from the location tag ("GA1028" -> GA; "AL & TN1021" -> AL, the rest
kept in location), status active. Titles starting "SOLD" / "NOT AVAILABLE",
and "For Lease" titles, are skipped (out of scope).

IDENTITY
The detail URL needs its slug (/carwash/1027 alone is a 404) and the slug is
not stable: the homepage writes .../<Title>/GA, the index .../<Title>-GA.
run_specialized.listing_key() keys carwashking rows on the numeric id
("carwashking:<id>"), so a retitled or re-slugged listing keeps its row.
"""

import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "carwashking.com"
BASE = "https://carwashking.com/"
INDEX_URL = BASE

US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI", "ID", "IL", "IN",
    "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH",
    "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT",
    "VT", "VA", "WA", "WV", "WI", "WY",
}

HREF_RE = re.compile(r"carwash/(\d+)/[^'\"]*")
SKIP_TITLE = re.compile(r"^\s*(sold|not\s+available)\b|\bfor\s+lease\b", re.I)


def _clean(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def money(text: Optional[str]) -> Optional[float]:
    m = re.search(r"\$\s*(\d[\d,]*(?:\.\d+)?)", text or "")
    if not m:
        return None
    v = float(m.group(1).replace(",", ""))
    return v if v > 0 else None


def place(loc: str, lid: str) -> Tuple[Optional[str], Optional[str]]:
    """'GA1028' -> (None, 'GA'); 'AL & TN1021' -> ('AL & TN', 'AL');
    'KY 421011025' -> ('KY 42101', 'KY'); 'Georgia948' -> ('Georgia', None)."""
    loc = _clean(loc)
    if lid and loc.endswith(lid):
        loc = loc[: -len(lid)].strip()
    m = re.match(r"^([A-Z]{2})\b", loc)
    st = m.group(1) if m and m.group(1) in US_STATES else None
    return loc or None, st


def parse_home(html: str, account: str = "carwashking") -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()
    for item in soup.select(".item[id^=itemid-]"):
        lid = item["id"].split("-", 1)[1]
        if not lid.isdigit() or lid in seen:
            continue
        a = item.select_one(".feature-title a, .listing-title a")
        href = a.get("href", "") if a else ""
        if not HREF_RE.search(href):
            fig = item.select_one("[onclick*='carwash/']")
            m = HREF_RE.search(fig.get("onclick", "")) if fig else None
            href = m.group(0) if m else ""
        if not href:
            continue
        title = _clean(a.get_text(" ")) if a else ""
        if not title or SKIP_TITLE.search(title):
            continue
        seen.add(lid)
        price_el = item.select_one(".feature-meta-cat a, .listing-meta-cat a")
        price_text = _clean(price_el.get_text(" ")) if price_el else None
        loc_el = item.select_one(".feature-location, .listing-location")
        loc, st = place(loc_el.get_text(" ") if loc_el else "", lid)
        url = BASE + href.lstrip("./").replace(" ", "%20")
        rows.append(format_listing(
            url, account,
            title=title,
            price=money(price_text),
            price_text=price_text,
            state=st,
            location=loc,
            business_type="Car Wash",
            status="active",
        ))
    return rows


class CarWashKingScraper:
    """run_specialized entry: CarWashKingScraper().scrape(account)."""

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "carwashking") -> List[Dict]:
        rows = parse_home(self.fetch(), account)
        print(f"[carwashking] {len(rows)} listings, "
              f"{sum(1 for r in rows if r.get('price'))} priced, "
              f"{sum(1 for r in rows if r.get('state'))} with state")
        return rows
