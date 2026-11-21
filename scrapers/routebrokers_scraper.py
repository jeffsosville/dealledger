"""
routebrokers_scraper.py — routebrokers.com (Route Brokers, Inc., Great Neck NY).

WHY A SPECIALIZED SCRAPER
/routes-for-sale is a Duda page. Every listing is one accordion item
(li.accordion-item): the heading is "<Route> / <ref>" ("Flowers Bread /
BR-1273"; Mr. Softee has no ref), the body a few paragraphs:

    [badge]              HUGE PRICE REDUCTION! / DEPOSIT TAKEN   (optional)
    Nets $2,250/wk (after ALL expenses)
    Asking Price: $239,000          | Asking: $299,000 | "$149,000 down"
    [terms line]                    "* Terms also available with $89,000 down"
    LOCATION                        "PHOENIX-SCOTTSDALE, AZ METRO"
    <ul> bullet facts </ul>

There are no detail pages. The V6 generic crawl stored 4 rows on the bare
index URL (2 distinct URLs: the index and the mailing-list page), with titles
like "In the past 35+ years, Route Brokers has sold THOUSANDS..." and read
"14 Routes" inside one listing (MS-1265, North Dakota) as the broker's count.
On 2026-11-20 the page held 12 listings.

WHAT THIS EMITS
  url          https://routebrokers.com/routes-for-sale#dl-<ref>  (synthetic: the
               cards have no detail link; ref = the broker's BR-/JU-/PR-/CN-/MS-
               number, or a slug of the heading when it has none)
  title        the heading without the ref, plus "Route" when it names none,
               plus the location: "Flowers Bread Route - Phoenix-Scottsdale, AZ Metro"
  price        "Asking Price: $X" / "Asking: $X" — not when the amount is
               followed by "down" (that is the down payment). "$1.500,000"
               (MS-1265) reads as 1,500,000.
  price_text   the verbatim "$X down" line when it is the only amount
  broker_status_text  "DEPOSIT TAKEN" verbatim; status stays active (decision 6)
  location / state    the location line; state from a name or ", XX"
  description  the Nets line (weekly / monthly — never annualised), terms,
               and the bullet facts
"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "routebrokers.com"
INDEX_URL = "https://routebrokers.com/routes-for-sale"

_STATES = {
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
_NY_PLACES = re.compile(r"\b(?:long island|staten island|queens|brooklyn|bronx|manhattan|"
                        r"nassau|suffolk|westchester|new york city)\b", re.I)
_REF = re.compile(r"/\s*([A-Z]{2}-\d+[A-Z]?)\s*$")
_BADGE = re.compile(r"^(?:deposit\s+taken|under\s+contract|pending|sold|"
                    r"huge\s+price\s+reduction!?|price\s+reduced!?|new\s+listing!?)$", re.I)
_STATUS_BADGE = re.compile(r"^(?:deposit\s+taken|under\s+contract|pending)$", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _money(s: str) -> Optional[float]:
    m = re.search(r"\$\s*(\d{1,3}(?:[.,]\d{3})+|\d+)(?:\s*([kKmM])\b)?", s or "")
    if not m:
        return None
    v = float(re.sub(r"[.,]", "", m.group(1)))     # "$1.500,000" is a typo for 1,500,000
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


def _slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")


def _state(loc: str) -> Optional[str]:
    low = (loc or "").lower()
    m = re.search(r",\s*([A-Z]{2})\b", loc or "")
    if m and m.group(1) in _STATES.values():
        return m.group(1)
    hits = sorted((low.find(n), code) for n, code in _STATES.items()
                  if re.search(r"\b" + n + r"\b", low))
    # "new york" contains "york"; longest names are listed whole, so the
    # earliest match in the text is the first state the broker names.
    if hits:
        return hits[0][1]
    return "NY" if _NY_PLACES.search(loc or "") else None


def parse_items(html: str) -> List[Dict]:
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    for li in soup.select("li.accordion-item"):
        head = li.select_one(".accordion-title")
        body = li.select_one(".accordion-description")
        if head is None or body is None:
            continue
        heading = _clean(head.get_text(" ", strip=True))
        if not heading:
            continue
        m = _REF.search(heading)
        ref = m.group(1) if m else None
        name = _clean(_REF.sub("", heading)).rstrip(" /") if ref else heading
        paras = [_clean(p.get_text(" ", strip=True)) for p in body.find_all("p")]
        paras = [p for p in paras if p]
        facts = [_clean(x.get_text(" ", strip=True)) for x in body.find_all("li")]
        status_text, price, price_text, nets, terms, loc = None, None, None, None, [], None
        for p in paras:
            if _BADGE.match(p):
                if _STATUS_BADGE.match(p) and not status_text:
                    status_text = p
                continue
            if re.match(r"^nets?\b", p, re.I):
                nets = nets or p
                continue
            if "$" in p:
                is_down = re.search(r"\$\s*[\d.,]+\s*[kKmM]?\s*down\b", p, re.I)
                if re.match(r"^asking(?:\s+price)?\s*:", p, re.I) and not is_down and price is None:
                    price = _money(p)
                elif is_down and price is None and price_text is None and not p.startswith("*") \
                        and not re.search(r"terms", p, re.I):
                    price_text = p
                else:
                    terms.append(p)
                continue
            if p.startswith(("*", "(")):
                terms.append(p)
                continue
            if loc is None:
                loc = p
        if price is not None:
            price_text = None
        out.append({"ref": ref, "name": name, "price": price, "price_text": price_text,
                    "status_text": status_text, "nets": nets, "terms": terms,
                    "location": loc, "state": _state(loc), "facts": facts})
    return out


def to_rows(items: List[Dict], account: str = "10708") -> List[Dict]:
    rows, seen = [], {}
    for it in items:
        key = _slug(it["ref"]) if it["ref"] else _slug(it["name"])
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:
            key += f"-{seen[key]}"
        desc = " ".join(x for x in ([it["nets"]] + it["terms"] + it["facts"]) if x) or None
        title = it["name"]
        if not re.search(r"\b(?:routes?|territor(?:y|ies)|business)\b", title, re.I):
            title += " Route"
        if it["location"]:
            loc = it["location"]
            if loc.isupper():
                loc = re.sub(r",\s*([A-Z][a-z])\b", lambda m: ", " + m.group(1).upper(), loc.title())
            title += " - " + loc
        rec = format_listing(
            f"{INDEX_URL}#dl-{key}",
            account,
            title=title,
            price=it["price"],
            price_text=it["price_text"],
            state=it["state"],
            location=it["location"],
            description=desc,
            business_type=it["name"],
            status="active",
        )
        rec["broker_status_text"] = it["status_text"]
        if it["price_text"] and not it["price"]:
            rec["price_not_disclosed"] = True
        rows.append(rec)
    return rows


class RouteBrokersScraper:
    """run_specialized entry: RouteBrokersScraper().scrape(account)."""

    last_displayed: Optional[Dict] = None

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "10708") -> List[Dict]:
        RouteBrokersScraper.last_displayed = None
        items = parse_items(self.fetch())
        if not items:
            raise RuntimeError("routebrokers.com: no accordion items on /routes-for-sale")
        rows = to_rows(items, account)
        RouteBrokersScraper.last_displayed = {
            "count": len(items), "source": "counted",
            "note": f"accordion items on {INDEX_URL}"}
        print(f"[routebrokers] {len(rows)} listings, "
              f"{sum(1 for r in rows if r.get('price'))} priced, "
              f"{sum(1 for r in rows if r.get('state'))} with state")
        return rows
