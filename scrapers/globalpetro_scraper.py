"""
globalpetro_scraper.py — Global Petro Advisors (globalpetroadvisors.com, Aventura FL).

WHY A SPECIALIZED SCRAPER
The broker's own index, https://globalpetroadvisors.com/listings/ (nav: "Businesses
for sale"), is plain server-rendered HTML: one <ul class="g-lcards"> per section,
one card per listing:

    <li><a href="/listings/<slug>-<id>/">
          <em class="g-badge uc">Under Contract</em>        (optional)
          <b>Gas station with property, Cleveland area</b>
          <span>East Cleveland, Ohio · $920,000</span>
          <small>Fee-simple station on a main urban corridor. ...</small></a></li>

The index mixes businesses with real estate the broker also sells: a land
development site, a highway fuel site (land), new-construction office condos,
and a fuel terminal property. Those are not businesses for sale (scope rule,
Nov 11/20: land-only, lease-only, For Lease, Coming Soon -> out), so V6's
generic card pattern would publish them. Discovery never found the page
(no_listings_page, 2026-09-02), and the broker had been seen only through
crexi, which is a marketplace and never a source.

WHAT IS NOT EMITTED (counted in stats['out_of_scope'])
  - cards under a section heading about condos / lease / coming soon
  - cards whose badge says For Lease / Coming Soon
  - land: "N-acre ...", "development site", "fuel site", "land", "lot", "parcel"
  - "Terminal property" (real estate only; the detail page's Type is
    "Terminal property", no operating business)

STATUS
  badge "Under Contract" / "Pending" -> active, broker_status_text verbatim
  badge "Sold"                       -> sold
  none                               -> active

IDENTITY
The detail URL (/listings/<slug>-<id>/). Never the price or title.
"""

import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

BASE = "https://globalpetroadvisors.com"
INDEX_URL = BASE + "/listings/"
DETAIL_RE = re.compile(r"^https://globalpetroadvisors\.com/listings/[a-z0-9][a-z0-9-]*/$")

STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI",
    "wyoming": "WY",
}

OUT_SECTION = re.compile(r"condo|lease|coming soon", re.I)
OUT_BADGE = re.compile(r"for lease|lease only|coming soon", re.I)
OUT_TITLE = re.compile(
    r"\b\d+(?:\.\d+)?[- ]acres?\b|\bdevelopment site\b|\bfuel site\b|\bland\b|\blots?\b|"
    r"\bparcels?\b|\bcondos?\b|\bfor lease\b|\bcoming soon\b|^terminal property\b",
    re.I)

_MONEY = re.compile(r"\$\s*(\d[\d,]*(?:\.\d+)?)\s*([kKmM])?")


def money(text: Optional[str]) -> Optional[float]:
    m = _MONEY.search(text or "")
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


def _clean(s: str) -> str:
    # the site serves some "·" double-encoded as "Â·"
    return re.sub(r"\s+", " ", (s or "").replace("Â·", "·").replace("Â", "")).strip()


def place(loc: str) -> Tuple[Optional[str], Optional[str]]:
    """'East Cleveland, Ohio' -> ('East Cleveland', 'OH');
    'Southwest Virginia (Central Appalachia)' -> (None, 'VA'); 'Eastern Kentucky' -> (None, 'KY')."""
    low = re.sub(r"\(.*?\)", "", loc).lower()
    for name in sorted(STATE_NAMES, key=len, reverse=True):
        m = re.search(r"\b" + name + r"\b", low)
        if not m:
            continue
        if name == "virginia" and re.search(r"\bwest virginia\b", low):
            continue
        city = loc[:m.start()].strip(" ,-") or None
        if city and re.fullmatch(r"(?:(?:south|north)?(?:east|west)|south|north)(?:ern)?|central",
                                 city, re.I):
            city = None                                  # "Southwest Virginia" is a region
        return city, STATE_NAMES[name]
    return None, None


def business_type(title: str) -> Optional[str]:
    t = title.lower()
    if re.search(r"gas station|fuel station|service station|truck stop|travel cent", t):
        return "Gas Station"
    if re.search(r"convenience store|c-store", t):
        return "Convenience Store"
    if re.search(r"grocery|supermarket", t):
        return "Grocery Store"
    if re.search(r"car wash", t):
        return "Car Wash"
    return None


def parse_index(html: str, account: str = "globalpetro") -> Tuple[List[Dict], Dict[str, int]]:
    """(rows, stats). One row per in-scope card; out-of-scope cards only counted."""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup
    rows: List[Dict] = []
    stats: Dict[str, int] = {"cards": 0}
    seen = set()
    for ul in main.select("ul.g-lcards"):
        head = ul.find_previous_sibling(["h2", "h1", "header"])
        section = _clean(head.get_text(" ")) if head and head.name == "h2" else ""
        for a in ul.select("li > a[href]"):
            url = urljoin(BASE, a["href"].split("#")[0].split("?")[0])
            if not url.endswith("/"):
                url += "/"
            if not DETAIL_RE.match(url) or url in seen:
                continue
            seen.add(url)
            stats["cards"] += 1
            b = a.find("b")
            title = _clean(b.get_text(" ")) if b else ""
            badge_el = a.find("em")
            badge = _clean(badge_el.get_text(" ")) if badge_el else ""
            span = a.find("span")
            line = _clean(span.get_text(" ")) if span else ""
            small = a.find("small")
            blurb = _clean(small.get_text(" ")) if small else ""

            why = None
            if not title:
                why = "no_title"
            elif section and OUT_SECTION.search(section):
                why = "out_of_scope"
            elif badge and OUT_BADGE.search(badge):
                why = "out_of_scope"
            elif OUT_TITLE.search(title):
                why = "out_of_scope"
            if why:
                stats[why] = stats.get(why, 0) + 1
                continue

            loc, _, price_txt = line.partition("·")
            city, state = place(loc.strip())
            status, label = "active", None
            if badge:
                label = badge
                status = "sold" if re.search(r"\bsold\b", badge, re.I) else "active"
            elif re.search(r"\bunder contract\b|\bpending\b", price_txt, re.I):
                label = "Under Contract"
            row = format_listing(
                url, account,
                title=title,
                price=money(price_txt),
                price_text=price_txt.strip() or None,
                city=city, state=state,
                location=loc.strip() or None,
                description=blurb or None,
                business_type=business_type(title),
                status=status,
            )
            if label:
                row["broker_status_text"] = label
            rows.append(row)
            stats[status] = stats.get(status, 0) + 1
    return rows, stats


class GlobalPetroScraper:
    """run_specialized entry: GlobalPetroScraper().scrape(account). One request."""

    def __init__(self):
        self.stats: Dict[str, int] = {}

    def _get(self, url: str) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(url, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.content.decode("utf-8", errors="replace")

    def scrape(self, account: str = "globalpetro") -> List[Dict]:
        rows, self.stats = parse_index(self._get(INDEX_URL), account)
        print(f"[globalpetro] {len(rows)} rows: {self.stats}")
        return rows
