"""
laundromat_broker_scraper.py — laundromat-broker.com (John M. Bedosky).

WHY A SPECIALIZED SCRAPER
The site is Weebly. Its one listings page (/listings.html) is a single long
list: each listing is an <h2 class="wsite-content-title"> heading followed by
<div class="paragraph"> body text, until the next heading. The broker writes
everything into the heading, status included:

    "1. Laundromat - Woodbridge, CT - GREAT DEAL - $55,000 - SOLD 2021"
    "137. Profitable, Absentee & Attended Laundromat-Northeast Houston, TX $550K- Under Deposit 2026"
    "146. Absentee-Unattended Laundromat- Franklin County, PA - $140,000"

The V6 generic crawl read this page as cards and took the first body line as
the title ("26 washers & 22 dryers approx. 20 years old..."), so 25 of 35
'active' rows were misclassified, most had no state, and it could not read
the status suffix: on 2026-11-19 the page held 154 headings, 87+ of them SOLD,
while DealLedger showed 35 active.

WHAT THIS EMITS (format_listing shape, one row per heading)
  title        the heading without its number, price and status
               ("Laundromat - Woodbridge, CT - GREAT DEAL")
  price        the last dollar amount in the heading (asking, never sale price)
  city/state   from the heading; a town table covers the broker's recurring
               towns that he writes without a state (Danbury, Bridgeport ...)
  status       from the heading's status suffix, via scrapers/listing_status
               (decisions 5/6), with this site's extra phrases below:
                 SOLD 2021 / Sold by 3rd Party 2025       -> sold
                 Has Accepted Offer 2024 / Under Deposit 2026 -> active + label
                 On Hold / Inactive / Not Available /
                 No Longer Available / Listing Cancelled /
                 Temporarily Unavailable / Off Market (any case, with or
                 without "Temporarily") -> inactive
               No suffix -> active.
  broker_status_text  the suffix verbatim, so "SOLD 2021" keeps its year
  business_type  from the heading (Laundromat, Dry Cleaner, Restaurant ...)
  cash_flow    only from an explicit "cash flow / net income / net profit of $X"
               in the body, within a sane range; otherwise None
  description  the body text

IDENTITY
  url = https://laundromat-broker.com/listings.html#lb-<heading number>
The broker numbers listings in the order he adds them and never reuses a
number (1..155 on 2026-11-19, #7 absent), so the number is the stable key.
Rows the V6 crawl wrote for this domain used #dl-<slug> URLs; retire them
once (sql/2026-11-19_laundromat_broker_supersede.sql).
"""

import re
from html import unescape
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

try:
    from listing_status import lookup_status as _shared_lookup
except Exception:
    _shared_lookup = None

DOMAIN = "laundromat-broker.com"
INDEX_URL = "https://laundromat-broker.com/listings.html"

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------
# The suffix after the price: everything from the first status word to the end.
_STATUS_START = re.compile(
    r"(?:\b|^)(sold\b|has accepted offer|accepted offer|under deposit|under contract|"
    r"temporarily\b|on hold\b|off\s*market|inactive\b|not available|no longer available|"
    r"listing cancell?ed|cancell?ed\b|withdrawn|pending\b)", re.I)

# Phrases this broker uses that scrapers/listing_status.py does not list (yet).
_SITE_PHRASES = [
    (re.compile(r"\bsold\b", re.I), "sold"),                       # SOLD 2021, Sold by 3rd Party 2025
    (re.compile(r"under deposit", re.I), "active"),                # like Under Contract (decision 6)
    (re.compile(r"accepted offer", re.I), "active"),
    (re.compile(r"temporarily", re.I), "inactive"),                # Temporarily Off Market / Unavailable
    (re.compile(r"listing cancell?ed|cancell?ed", re.I), "inactive"),
    (re.compile(r"on hold|inactive|not available|no longer available|withdrawn", re.I), "inactive"),
    (re.compile(r"off\s*market", re.I), "inactive"),               # Jeff 2026-11-23: any "Off Market", with or without "Temporarily"
    (re.compile(r"under contract|pending", re.I), "active"),
]


def split_status(heading: str) -> Tuple[str, Optional[str]]:
    """('<heading before the status>', '<status suffix verbatim>' or None)."""
    m = _STATUS_START.search(heading)
    if not m:
        return heading, None
    head = heading[:m.start()]
    suffix = heading[m.start():]
    suffix = re.sub(r"^[\s\-–—:]+|[\s\-–—:.]+$", "", suffix)
    return head, (suffix or None)


def status_of(suffix: Optional[str]) -> str:
    """'sold' | 'active' | 'inactive' for a heading's status suffix."""
    if not suffix:
        return "active"
    if re.search(r"off[\s-]*market", suffix, re.I):
        # The shared vocabulary keeps "Off Market" active (decision 6); on this
        # broker it means withdrawn (Jeff 2026-11-23), so it is decided here.
        return "inactive"
    if _shared_lookup is not None:
        hit = _shared_lookup(re.sub(r"\b(19|20)\d\d\b", "", suffix).strip(), DOMAIN)
        if hit and hit[0] in ("sold", "active", "inactive") and not re.search(r"temporarily", suffix, re.I):
            return hit[0]
    for rx, st in _SITE_PHRASES:
        if rx.search(suffix):
            return st
    return "active"


# ---------------------------------------------------------------------------
# price
# ---------------------------------------------------------------------------
_MONEY = re.compile(r"\$\s*(\d{1,3}(?:,\d{2,3})+|\d+(?:\.\d+)?)\s*([kKmM])?(?![\w])")
_BARE_MONEY = re.compile(r"(?<![\w$.])(\d{2,3},\d{3})(?![\d,])")   # "Bridgeport-235,000" (no $)


def parse_price(text: str) -> Tuple[Optional[float], Optional[Tuple[int, int]]]:
    """Last dollar amount in text -> (value, (start, end) span)."""
    hits = list(_MONEY.finditer(text))
    if hits:
        m = hits[-1]
        raw, suf = m.group(1), (m.group(2) or "").lower()
        parts = raw.split(",")
        # "$110,00" (a typo for $110,000): a short last group means thousands.
        if len(parts) > 1 and len(parts[-1]) == 2:
            raw = "".join(parts[:-1]) + parts[-1] + "0"
        try:
            v = float(raw.replace(",", ""))
        except ValueError:
            return None, None
        if suf == "k":
            v *= 1_000
        elif suf == "m":
            v *= 1_000_000
        return (v if v > 0 else None), m.span()
    m = _BARE_MONEY.search(text)
    if m:
        return float(m.group(1).replace(",", "")), m.span()
    return None, None


# ---------------------------------------------------------------------------
# location
# ---------------------------------------------------------------------------
STATE_NAMES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ",
    "new mexico": "NM", "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI",
    "south carolina": "SC", "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}
STATE_CODES = set(STATE_NAMES.values())

# Places the broker writes without a state. Only places that are unambiguous
# for this Connecticut/New York broker; anything else is left without a state.
PLACE_STATE = {
    # New York City and around
    "brooklyn": "NY", "bklyn": "NY", "bushwick": "NY", "williamsburg": "NY", "queens": "NY",
    "ridgewood": "NY", "astoria": "NY", "woodside": "NY", "maspeth": "NY", "flushing": "NY",
    "bayside": "NY", "forest hills": "NY", "bronx": "NY", "staten island": "NY",
    "hell's kitchen": "NY", "nyc": "NY", "yonkers": "NY", "westchester": "NY",
    "putnam county": "NY", "columbia county": "NY", "ulster county": "NY", "selden": "NY",
    "holbrook": "NY", "bellmore": "NY", "scarsdale": "NY", "hartsdale": "NY", "tarrytown": "NY",
    "ardsley": "NY", "goshen": "NY", "poughkeepsie": "NY", "corning": "NY",
    # Connecticut
    "danbury": "CT", "bridgeport": "CT", "new haven": "CT", "east haven": "CT",
    "north haven": "CT", "hartford": "CT", "west hartford": "CT", "east hartford": "CT",
    "brookfield": "CT", "middletown": "CT", "southbury": "CT", "oxford": "CT",
    "old lyme": "CT", "seymour": "CT", "naugatuck": "CT", "waterbury": "CT",
    "fairfield county": "CT", "litchfield county": "CT", "plainville": "CT", "bristol": "CT",
    "new london": "CT", "windham": "CT", "cheshire": "CT", "suffield": "CT", "newtown": "CT",
    "prospect": "CT", "easton": "CT", "woodbury": "CT", "milford": "CT", "cromwell": "CT",
    "bloomfield": "CT", "woodbridge": "CT", "ashford": None,      # Ashford, AL in #113
    # Massachusetts
    "boston": "MA", "lowell": "MA", "new bedford": "MA", "fall river": "MA", "watertown": "MA",
    "gloucester": "MA", "amesbury": "MA", "lawrence": "MA",
    # New Jersey
    "bayonne": "NJ", "jersey city": "NJ", "paterson": "NJ", "east orange": "NJ",
    "edison": "NJ", "passaic": "NJ", "new brunswick": "NJ",
}

_STATE_CODE_RE = re.compile(r"(?:,|\s|-|^)\s*(" + "|".join(sorted(STATE_CODES)) + r")\b\.?(?=\s|$|-|,|\$|\d)")
_STATE_NAME_RE = re.compile(r"\b(" + "|".join(sorted(STATE_NAMES, key=len, reverse=True)) + r")\b", re.I)
_NEAR = re.compile(r"^(?:near|neal|nr\.?)\s+", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip(" -–—,.:/")


def find_location(head: str) -> Tuple[Optional[str], Optional[str]]:
    """(city, state) from the heading's text before the price/status."""
    state = None
    city = None
    # 1) an explicit two-letter code ("Woodbridge, CT", "Seymour CT", "BKLYN")
    m = None
    for m in _STATE_CODE_RE.finditer(head):
        pass
    if m:
        state = m.group(1)
        before = head[:m.start(1)]
        seg = re.split(r"\s[-–—]\s|-(?=[A-Z ])|:|,(?=\s*[A-Za-z]+\s*,?\s*$)", before)[-1]
        seg = seg.split(",")[-1] if seg.count(",") and not seg.strip().endswith(",") else seg
        seg = re.split(r"\b(?:in|near|nr\.?)\s+", seg, flags=re.I)[-1]
        city = _clean(_NEAR.sub("", _clean(seg))) or None
    else:
        # 2) a state name ("Baldwin County, Alabama", "Near Jenks, OKLAHOMA")
        n = None
        for n in _STATE_NAME_RE.finditer(head):
            pass
        if n:
            state = STATE_NAMES[n.group(1).lower()]
            seg = re.split(r"\s[-–—]\s|-(?=[A-Z ])", head[:n.start()])[-1]
            city = _clean(_NEAR.sub("", _clean(seg))) or None
    # 3) a known place ("Near Danbury", "Astoria Queens", "Bronx NY")
    low = head.lower()
    place_hit = None
    for place in sorted(PLACE_STATE, key=len, reverse=True):
        if re.search(r"(?<![a-z])" + re.escape(place) + r"(?![a-z])", low):
            place_hit = place
            break
    if place_hit and PLACE_STATE[place_hit]:
        if not state:
            state = PLACE_STATE[place_hit]
        if not city or len(city) > 40 or city.lower() in ("near", ""):
            city = {"nyc": "New York", "bklyn": "Brooklyn", "hell's kitchen": "Hell's Kitchen"}.get(
                place_hit, place_hit.title())
    if city and (len(city) > 40 or not re.search(r"[A-Za-z]", city)):
        city = None
    if city and city.upper() in STATE_CODES:
        city = None
    return city, state


# ---------------------------------------------------------------------------
# type, cash flow
# ---------------------------------------------------------------------------
_TYPES = [
    (r"laundr|wash\s*(?:&|and)?\s*fold", "Laundromat"),
    (r"dry\s*clean", "Dry Cleaner"),
    (r"pizz|restaurant|deli\b|caf[eé]|bagel|burger|take-?out|dining|grill|bakery", "Restaurant"),
    (r"liquor", "Liquor Store"),
    (r"gas station", "Gas Station"),
    (r"garage door", "Garage Door Company"),
    (r"tree cutting", "Tree Service"),
    (r"martial arts", "Martial Arts Studio"),
]


def business_type(title: str) -> Optional[str]:
    for rx, label in _TYPES:
        if re.search(rx, title, re.I):
            return label
    return None


_CF = re.compile(
    r"(?:cash\s*flow|net\s*(?:income|profit)|owner'?s?\s*(?:benefit|earnings)|sde)\b[^$\d]{0,30}"
    r"\$\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*([kK])?", re.I)


def cash_flow(body: str) -> Optional[float]:
    m = _CF.search(body or "")
    if not m:
        return None
    try:
        v = float(m.group(1).replace(",", ""))
    except ValueError:
        return None
    if (m.group(2) or "").lower() == "k":
        v *= 1_000
    return v if 5_000 <= v <= 5_000_000 else None


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------
_NUM = re.compile(r"^\s*-?\s*(\d{1,3})\s*\.\s*")


def parse_heading(heading: str) -> Optional[Dict]:
    """One heading -> dict of the fields above, or None when it isn't a listing."""
    h = re.sub(r"\s+", " ", unescape(heading).replace("​", "")).strip()
    m = _NUM.match(h)
    if not m:
        return None
    number = int(m.group(1))
    rest = h[m.end():]
    head, suffix = split_status(rest)
    price, span = parse_price(head)
    if price is None and suffix:                       # "$450K- Under Deposit": price sits before the suffix
        price, span = parse_price(rest)
    title_src = head[:span[0]] + " " + head[span[1]:] if span and span[1] <= len(head) else head
    title_src = re.sub(r"\b(?:only|reduced to|price reduced|firm)\b", " ", _clean(title_src), flags=re.I)
    title = _clean(re.sub(r"\s*[-–—]\s*[-–—]\s*", " - ", title_src))
    city, state = find_location(head)
    return {
        "number": number,
        "title": title or None,
        "price": price,
        "status_text": suffix,
        "status": status_of(suffix),
        "city": city,
        "state": state,
        "business_type": business_type(title),
    }


def parse_page(html: str) -> List[Dict]:
    """All listings on the page, in page order, as format_listing rows."""
    soup = BeautifulSoup(html, "html.parser")
    rows, seen = [], set()
    for h2 in soup.select("h2.wsite-content-title"):
        parsed = parse_heading(h2.get_text(" "))
        if not parsed or parsed["number"] in seen:
            continue
        seen.add(parsed["number"])
        body = []
        for sib in h2.find_next_siblings():
            if sib.name == "h2" and "wsite-content-title" in (sib.get("class") or []):
                break
            t = sib.get_text(" ", strip=True)
            if t:
                body.append(t)
        description = re.sub(r"\s+", " ", " ".join(body).replace("​", "")).strip() or None
        row = format_listing(
            f"{INDEX_URL}#lb-{parsed['number']}",
            "laundromat-broker",
            title=parsed["title"],
            price=parsed["price"],
            city=parsed["city"],
            state=parsed["state"],
            location=", ".join(x for x in (parsed["city"], parsed["state"]) if x) or None,
            description=description,
            business_type=parsed["business_type"],
            cash_flow=cash_flow(description or ""),
            status=parsed["status"],
        )
        if parsed["status_text"]:
            row["broker_status_text"] = parsed["status_text"]
        rows.append(row)
    return rows


class LaundromatBrokerScraper:
    """run_specialized entry: LaundromatBrokerScraper().scrape(account)."""

    def __init__(self):
        self.stats: Dict[str, int] = {}

    def fetch(self) -> str:
        # The site's nginx resets requests whose TLS/UA isn't a current Chrome
        # (see dealledger_scraper_v6.py, chrome145 note), so use curl_cffi.
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "laundromat-broker") -> List[Dict]:
        rows = parse_page(self.fetch())
        for r in rows:
            r["broker_account"] = account
            self.stats[r["status"]] = self.stats.get(r["status"], 0) + 1
        print(f"[laundromat-broker] {len(rows)} headings: {self.stats}")
        return rows
