"""
routeworld_scraper.py — routeworldbrokers.com (Route World Brokers Inc., Valley
Stream NY; vending and delivery routes in the NY metro area).

WHY A SPECIALIZED SCRAPER
/listings-page/ is one WordPress page. Inside div.entry-content each listing
is an <h1> heading followed by <p> paragraphs of prose; the price is written
in the text ("Total price is $165,000", "Price is $400,000 total",
"ONLY $21,000 total price"). There are no detail pages and no cards, so the
V6 generic crawl stored the page itself ("Listings Page") and a blog archive
link ("ROUTE WORLD DAYS", /2018/04/) as listings — 2 rows, neither a listing.

WHAT THIS EMITS (one row per <h1> block that states a price)
  url          https://routeworldbrokers.com/listings-page/#dl-<slug of heading>
               (synthetic: no detail links)
  title        the heading
  price        the stated total price
  state        from the text ("Dutchess County, New York", "downstate NY")
  description  the block's paragraphs

NOT EMITTED: headings with no price in their block — the "ATTENTION Vending
Route Buyers!" banner, "Vending Account Ready for Entry.." (a teaser with no
details), "ROUTE WORLD SERVES:" and the "." spacers. Counted in skipped.
"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

try:
    from routebrokers_scraper import _state
except Exception:                                   # pragma: no cover
    def _state(_):
        return None

DOMAIN = "routeworldbrokers.com"
INDEX_URL = "https://routeworldbrokers.com/listings-page/"

_PRICE = (
    re.compile(r"total\s+price\s+(?:is\s+)?\$\s?([\d,]+)", re.I),
    re.compile(r"\bprice\s+is\s+\$\s?([\d,]+)", re.I),
    re.compile(r"\$\s?([\d,]+)\s+total\b", re.I),
)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _slug(s: str) -> str:
    return re.sub(r"-+", "-", re.sub(r"[^a-z0-9]+", "-", s.lower())).strip("-")[:80].strip("-")


def _price(text: str) -> Optional[float]:
    for rx in _PRICE:
        m = rx.search(text)
        if m:
            v = float(m.group(1).replace(",", "") or 0)
            if v > 0:
                return v
    return None


def parse_blocks(html: str) -> Dict:
    soup = BeautifulSoup(html or "", "html.parser")
    root = soup.select_one("div.entry-content") or soup.body or soup
    blocks, cur = [], None
    for el in root.find_all(recursive=False):
        if el.name == "h1":
            cur = {"heading": _clean(el.get_text(" ", strip=True)), "paras": []}
            blocks.append(cur)
        elif cur is not None and el.name == "p":
            t = _clean(el.get_text(" ", strip=True))
            if t:
                cur["paras"].append(t)
    items, skipped = [], 0
    for b in blocks:
        text = " ".join(b["paras"])
        price = _price(text)
        if not b["heading"] or price is None:
            skipped += 1
            continue
        items.append({"title": b["heading"], "price": price, "text": text,
                      "state": _state(b["heading"] + " " + text)})
    return {"items": items, "skipped": skipped}


def to_rows(items: List[Dict], account: str = "routeworldbrokers") -> List[Dict]:
    rows, seen = [], {}
    for it in items:
        key = _slug(it["title"])
        seen[key] = seen.get(key, 0) + 1
        if seen[key] > 1:
            key += f"-{seen[key]}"
        rows.append(format_listing(
            f"{INDEX_URL}#dl-{key}", account,
            title=it["title"], price=it["price"], state=it["state"],
            description=it["text"][:2000] or None,
            business_type="Vending Route" if re.search(r"vending", it["title"], re.I) else "Route",
            status="active"))
    return rows


class RouteWorldScraper:
    """run_specialized entry: RouteWorldScraper().scrape(account)."""

    last_displayed: Optional[Dict] = None

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "routeworldbrokers") -> List[Dict]:
        RouteWorldScraper.last_displayed = None
        p = parse_blocks(self.fetch())
        rows = to_rows(p["items"], account)
        RouteWorldScraper.last_displayed = {
            "count": len(rows), "source": "counted",
            "note": f"priced <h1> blocks on {INDEX_URL} ({p['skipped']} unpriced headings)"}
        print(f"[routeworld] {len(rows)} listings ({p['skipped']} headings without a price)")
        return rows
