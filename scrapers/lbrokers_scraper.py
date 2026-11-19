"""
lbrokers_scraper.py — Laundry Brokers, Inc. (www.lbrokers.com).

WHY A SPECIALIZED SCRAPER
/stores-for-sale is a Leaflet map plus a card list; the V6 generic crawl found
no listing pattern on it (NO_PATTERN on all 48 runs to 2026-11-17, one row).
Each card links to a detail page:

    /coin-laundry-stores-for-sale/<slug>     (one older page: /coin-laundry-store-for-sale/)

A detail page has
  <h1 class="section-title">  "PENDING- Richmond, VA Laundry For Sale <strong>$799,000.00</strong>"
  .stats-row .js-stat-col     one column per figure: "$850,000" + .stat-title "Asking Price"
  "Location: Burlington, 30338, NC" on some pages
  a description, then a long Disclaimer.

WHAT IS NOT EMITTED
Laundry Brokers is also a Speed Queen distributor, and some "listings" are new
build-out / equipment packages, not businesses. Their own pages say so
("This is not a business sale. No existing business, revenue, or customer base
is being transferred."; "new vended laundry project package"; a stat titled
"Estimated Total Equipment/ Renovation" or "Equipment Package"). Those are not
businesses for sale (same rule as real estate and leases): not emitted, counted
in stats['new_build'].

STATUS (from the h1 prefix/suffix, via the shared policy)
  SOLD               -> sold, label kept ("SOLD")
  PENDING / Sale Pending -> active, label kept (decision 6)
  none               -> active

FIGURES (labels as the broker writes them; projected figures are his)
  asking_price  Asking Price / Sale Price / List / Total Price, else the h1 price
  cash_flow     Cash Flow / Net Income / Net (projected) / EBITA ("$45,000-50,000" -> 45,000)
  revenue       Gross Revenue / Revenue / Annual Gross Income
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

BASE = "https://www.lbrokers.com"
INDEX_URL = BASE + "/stores-for-sale"
DETAIL_RE = re.compile(r"/coin-laundry-stores?-for-sale/[a-z0-9][\w-]*$", re.I)

STATE_NAMES = {
    "alabama": "AL", "arizona": "AZ", "arkansas": "AR", "california": "CA", "colorado": "CO",
    "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA", "illinois": "IL",
    "indiana": "IN", "kentucky": "KY", "louisiana": "LA", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "mississippi": "MS", "missouri": "MO", "new jersey": "NJ", "new york": "NY",
    "north carolina": "NC", "ohio": "OH", "oklahoma": "OK", "pennsylvania": "PA",
    "south carolina": "SC", "tennessee": "TN", "texas": "TX", "virginia": "VA",
    "west virginia": "WV",
}
STATE_CODES = set(STATE_NAMES.values()) | {"AK", "HI", "ID", "IA", "KS", "ME", "MN", "MT", "NE",
                                           "NV", "NH", "NM", "ND", "OR", "RI", "SD", "UT", "VT",
                                           "WA", "WI", "WY", "DC"}

NEW_BUILD_TEXT = re.compile(
    r"not a business sale|no existing business|equipment\s*/\s*build-?out package|"
    r"new (?:vended )?laundry project|project package|equipment and related project services",
    re.I)
NEW_BUILD_STAT = re.compile(r"equipment|renovation|leasehold|construction", re.I)

ASK_LABELS = re.compile(r"^(asking price|sale price|list(?:\s*price)?|total price)\b", re.I)
CF_LABELS = re.compile(r"^(cash flow|net income|net\b|ebitd?a)", re.I)
REV_LABELS = re.compile(r"^(gross revenue|revenue|annual gross income|gross income|sales)", re.I)

_MONEY = re.compile(r"\$\s*(\d[\d,]*(?:\.\d+)?)\s*([kKmM])?")


def money(text: Optional[str]) -> Optional[float]:
    """First amount: '$45,000-50,000' -> 45000; '$200,000+' -> 200000; '$425,00' -> 425000."""
    m = _MONEY.search(text or "")
    if not m:
        return None
    raw = m.group(1).rstrip(".")
    parts = raw.split(".")[0].split(",")
    if len(parts) > 1 and len(parts[-1]) == 2:          # "$425,00" typo
        parts[-1] += "0"
    try:
        v = float("".join(parts))
    except ValueError:
        return None
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


def split_h1(h1: str) -> Tuple[str, Optional[str], Optional[float]]:
    """'PENDING- Richmond, VA Laundry For Sale $799,000.00' ->
       ('Richmond, VA Laundry For Sale', 'PENDING', 799000.0)"""
    t = re.sub(r"\s+", " ", h1).strip()
    label = None
    m = re.match(r"^(sold|pending|sale pending)\s*[-–:]\s*", t, re.I)
    if m:
        label, t = m.group(1), t[m.end():]
    m = re.search(r"\s*[-–]\s*(sold|pending|sale pending)\s*$", t, re.I)
    if m:
        label, t = label or m.group(1), t[:m.start()]
    price = money(t)
    title = re.sub(r"\$[\d,.\s]+$", "", t).strip(" -–")
    return title, label, price


def status_of(label: Optional[str]) -> str:
    if not label:
        return "active"
    return "sold" if re.search(r"\bsold\b", label, re.I) else "active"


def location(title: str, loc_line: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """(city, state) from 'Location: Burlington, 30338, NC' or the title
    ('Hollywood, FL Laundry...', 'Homestead Florida Laundry...', 'Southwest Florida ...')."""
    if loc_line:
        parts = [p.strip() for p in loc_line.split(",") if p.strip()]
        st = next((p.upper() for p in reversed(parts) if p.upper() in STATE_CODES), None)
        city = parts[0] if parts and parts[0].upper() not in STATE_CODES and not parts[0].isdigit() else None
        if st:
            return city, st
    place = re.split(r"\s+laundr", title, flags=re.I)[0].strip(" -–,")
    m = re.search(r",\s*([A-Z]{2})\b", place)
    if m and m.group(1) in STATE_CODES:
        return place[:m.start()].strip() or None, m.group(1)
    low = place.lower()
    for name in sorted(STATE_NAMES, key=len, reverse=True):
        mm = re.search(r"\b" + name + r"\b", low)
        if mm:
            city = place[:mm.start()].strip(" ,-–") or None
            if city and re.fullmatch(r"(south|north|east|west|central)\w*", city, re.I):
                city = None                                  # "Southwest Florida" is a region
            return city, STATE_NAMES[name]
    return place or None, None


def parse_detail(url: str, html: str, account: str = "lbrokers") -> Tuple[Optional[Dict], Optional[str]]:
    """(row or None, reason when None)."""
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1.section-title") or soup.select_one("h1")
    if not h1:
        return None, "no_h1"
    title, label, h1_price = split_h1(h1.get_text(" "))

    stats = []
    for col in soup.select(".stats-row .js-stat-col"):
        lab_el = col.select_one(".stat-title")
        lab = re.sub(r"\s+", " ", lab_el.get_text(" ")).strip() if lab_el else ""
        val = re.sub(r"\s+", " ", col.get_text(" ")).replace(lab, "").strip()
        stats.append((val, lab))

    for el in soup.select("header, nav, footer, script, style"):
        el.decompose()
    main = soup.select_one("main") or soup.body or soup
    body = re.sub(r"\s+", " ", main.get_text(" ")).split("Disclaimer")[0].strip()

    if NEW_BUILD_TEXT.search(body) or any(NEW_BUILD_STAT.search(lab) for _, lab in stats):
        return None, "new_build"

    asking = next((money(v) for v, lab in stats if ASK_LABELS.match(lab)), None) or h1_price
    cf = next((money(v) for v, lab in stats if CF_LABELS.match(lab)), None)
    rev = next((money(v) for v, lab in stats if REV_LABELS.match(lab)), None)

    loc_m = re.search(r"Location:\s*([^$]{3,60}?)(?=\s+(?:Previous|View|\$|Area|Type|Listing)|$)", body)
    city, state = location(title, loc_m.group(1) if loc_m else None)

    desc = body.split("Interested?")[0].strip()
    row = format_listing(
        url, account,
        title=title, price=asking, city=city, state=state,
        location=", ".join(x for x in (city, state) if x) or None,
        description=desc[:2000] or None,
        business_type="Laundromat",
        cash_flow=cf, revenue=rev,
        status=status_of(label),
    )
    if label:
        row["broker_status_text"] = label.upper() if label.isupper() else label.title()
    return row, None


def detail_urls(index_html: str) -> List[str]:
    soup = BeautifulSoup(index_html, "html.parser")
    out = []
    for a in soup.select("a[href]"):
        u = urljoin(BASE, a["href"].split("#")[0].split("?")[0])
        if DETAIL_RE.search(u) and "valuation" not in u and u not in out:
            out.append(u)
    return out


class LBrokersScraper:
    """run_specialized entry: LBrokersScraper().scrape(account)."""

    def __init__(self):
        self.stats: Dict[str, int] = {}

    def _get(self, url: str) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(url, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "lbrokers") -> List[Dict]:
        rows = []
        for url in detail_urls(self._get(INDEX_URL)):
            try:
                row, why = parse_detail(url, self._get(url), account)
            except Exception as e:                       # one bad page never stops the rest
                why, row = f"error:{type(e).__name__}", None
            key = row["status"] if row else why
            self.stats[key] = self.stats.get(key, 0) + 1
            if row:
                rows.append(row)
        print(f"[lbrokers] {len(rows)} rows: {self.stats}")
        return rows
