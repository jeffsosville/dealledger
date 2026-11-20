"""
gasstationsusa_scraper.py — gasstationsusa.com (Gas Stations USA Realty,
Florida gas stations / c-stores, "selling gas stations since 1980").

WHY A SPECIALIZED SCRAPER
1. V6 has failed HTTP_403 on every run since 2026-11-17 (its fetcher profile
   is refused; a plain curl_cffi Chrome request to the same URL is 200).
2. When V6 did read the page (to 2026-11-12) it wrote all 14 cards as active
   listings: the "Selling a Gas Station in Florida?" call-to-action card
   (a property post that is the broker's sales pitch, stored as "Selling Gas
   Stations in Florida Since 1980"), two vacant lots, and two cards marked
   "Recently Sold!". Its prices were the first card bullet ("$80,000 Store
   Sales Per Month") instead of the price box ($499,000).

THE PAGE (/properties/, /properties/page/2/, ...)
    <div class="lista_listing prop">
      <h4><a href=".../properties/single/<slug>/">#2394 Palm Beach County</a></h4>
      <li>$80,000 Store Sales Per Month</li> ... <li>Sale Pending!</li> ...
      <h3 class="price">$499,000</h3>

WHAT THIS EMITS
  url          the card's detail URL (identity, principle 4)
  title        the <h4> text ("#2394 Palm Beach County")
  price        the price box
  status       "Recently Sold!" / "Sold" bullet -> sold;
               "Sale Pending!" / "Contract Pending!" -> active, label kept verbatim
               in broker_status_text (decision 6)
  state        FL (every listing on this site is in Florida; the region filter
               is North / Central / South Florida)
NOT EMITTED (logged): a card with no "#<number>" reference and no price (the
call-to-action), and a vacant / commercial lot (land-only is out of scope).
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

DOMAIN = "gasstationsusa.com"
INDEX_URL = "https://gasstationsusa.com/properties/"
MAX_PAGES = 10

_REF = re.compile(r"^#\s?\d{3,5}\b")
_SOLD = re.compile(r"^\s*(recently\s+sold|sold)\b", re.I)
_PENDING = re.compile(r"^\s*((?:sale|contract)\s+pending|under\s+contract)\b", re.I)
_LAND = re.compile(r"\b(vacant\s+(?:commercial\s+)?lot|commercial\s+lot|vacant\s+land)\b", re.I)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _money(s: str) -> Optional[float]:
    m = re.search(r"\$\s*(\d{1,3}(?:,\d{3})+|\d+)", s or "")
    return float(m.group(1).replace(",", "")) if m else None


def parse_cards(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for card in soup.select("div.lista_listing"):
        a = card.select_one("h4 a[href]")
        if not a:
            continue
        price_el = card.select_one("h3.price")
        out.append({
            "url": a["href"].strip(),
            "title": _clean(a.get_text(" ")),
            "bullets": [_clean(li.get_text(" ")) for li in card.select("li") if _clean(li.get_text(" "))],
            "price_text": _clean(price_el.get_text(" ")) if price_el else "",
        })
    return out


def to_rows(cards: List[Dict], account: str = "gasstationsusa", log=print) -> List[Dict]:
    rows, seen = [], set()
    for c in cards:
        if c["url"] in seen:
            continue
        seen.add(c["url"])
        price = _money(c["price_text"])
        if not _REF.match(c["title"]) and price is None:
            log(f"[gasstationsusa] not emitted (not a property card): {c['title']}")
            continue
        text = " | ".join([c["title"]] + c["bullets"])
        if _LAND.search(text):
            log(f"[gasstationsusa] not emitted (land / lot): {c['title']}")
            continue
        status, label = "active", None
        for b in c["bullets"]:
            if _SOLD.match(b):
                status, label = "sold", b.rstrip("!").strip()
                break
            if _PENDING.match(b):
                label = b.rstrip("!").strip()
        row = format_listing(
            c["url"],
            account,
            title=c["title"],
            price=price,
            price_text=c["price_text"] or None,
            state="FL",
            location="FL",
            description="; ".join(c["bullets"]) or None,
            status=status,
        )
        if label:
            row["broker_status_text"] = label
        rows.append(row)
    return rows


def has_next(html: str) -> bool:
    return bool(re.search(r"Next\s*(?:&raquo;|»)", html))


class GasStationsUSAScraper:
    """run_specialized entry: GasStationsUSAScraper().scrape(account)."""

    def fetch(self, url: str) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(url, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "gasstationsusa") -> List[Dict]:
        cards = []
        for page in range(1, MAX_PAGES + 1):
            url = INDEX_URL if page == 1 else f"{INDEX_URL}page/{page}/"
            html = self.fetch(url)
            got = parse_cards(html)
            cards.extend(got)
            if not got or not has_next(html):
                break
            time.sleep(2)
        rows = to_rows(cards, account)
        print(f"[gasstationsusa] {len(cards)} cards over {page} page(s), {len(rows)} emitted "
              f"({sum(1 for r in rows if r['status'] == 'active')} active, "
              f"{sum(1 for r in rows if r['status'] == 'sold')} sold)")
        return rows
