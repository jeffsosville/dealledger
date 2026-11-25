"""
salehgroup_scraper.py - The Saleh Group (salehgroup.com, Cleveland OH; gas
stations, c-stores, grocery, restaurants, hotels).

WHY A SPECIALIZED SCRAPER
https://salehgroup.com/business-directory is a Muzopilot page whose HTML is an
empty grid; the cards are painted by JavaScript from one public JSON call,
    GET https://api.muzopilot.com/public/listings/saleh-group
which returns every card in one response ({"totalActive": N, "data": [...]}).
V6 saw the painted cards but never a per-card link, because a card has no
<a>: the click handler does location.href = "/listing?id=" + data-id. So every
row was stored with the directory page as its URL (1 distinct URL for 286
rows). This reads the JSON and builds the real detail URL itself:
    https://salehgroup.com/listing?id=<id>

Per item: id (cuid, the detail-URL key), sgNumber ("SG 12026"; the site also
writes "sg 11771", "SG-11765", "SG11711", "SG 11680 "), title, assetClass (a
", "-joined list of tags), state, city, askingPrice (an exact integer, or null
= the card prints "Inquire"), status (always "active" on 2026-11-23).
Prices come straight from the integer, not from the "$175K" / "$1.20M" text on
the card, which rounds ("$1.20M" can be 1,200,000 or 1,204,999).

SCOPE (Jeff, Nov 22; skipped cards are counted and listed in .skipped)
  out: apartments / multi-family / residential development, shopping plazas and
       commercial real estate, development land, "Beer and Wine Licenses".
  in:  gas stations, c-stores, grocery, restaurants, hotels - including when
       sold with the property ("Gas Station C Store Property", "Truck Stop
       Plaza and Restaurant", "Food Mart and Plaza Property").
A card is out only when EVERY asset-class tag is a real-estate / licence tag
(or the title names real estate and no operating business) AND the title names
no operating business. A card whose tags or title still name an operating
business is kept; a few of those are borderline and recorded in .flagged for
Jeff (e.g. a convenience store sold with apartments).

IDENTITY
The detail URL, /listing?id=<cuid> (run_specialized.listing_key's default: the
raw URL). The cuid is the platform's own record id; it carries no price or
title. The SG number is the broker's public ref and is unique per card (the
scraper de-duplicates on it inside one run), but it is not in the URL.
"""

import re
from typing import Dict, List, Optional, Tuple

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "salehgroup.com"
INDEX_URL = "https://salehgroup.com/business-directory"
API_URL = "https://api.muzopilot.com/public/listings/saleh-group"
DETAIL_URL = "https://salehgroup.com/listing?id={id}"

# An asset-class tag that is real estate / a licence, not an operating business.
OUT_TAG = re.compile(
    r"^(?:and\s+)?(?:apartments?(?:\s+(?:building|complex))?|\d+-unit\+?\s+apartment\s+building|"
    r"multi-?family\b.*|shopping\s+(?:plaza|center|centre)|commercial\s+(?:real\s+estate|property|plaza)|"
    r"real\s+estate|development\s+land|land|beer\s+and\s+wine\s+licen[sc]es?|"
    r"redevelopment\s+commercial\s+property|retail\s+shops?)$", re.I)
# Title words that say the card is real estate / a licence.
OUT_TITLE = re.compile(
    r"\bapartments?\b|\bmulti-?family\b|\btownhomes?\b|\bresidential\s+development\b|"
    r"\bshopping\s+(?:plaza|center|centre)\b|\bdevelopment\s+(?:land|opportunity)\b|"
    r"\bbeer\s+and\s+wine\b.*\blicen[sc]es?\b|\blicen[sc]es?\s+for\s+sale\b", re.I)
# Title words that name an operating business (these override the out signals).
BUSINESS_TITLE = re.compile(
    r"gas\s+stations?|\bc[- ]?stores?\b|convenience|food\s+mart|grocery|supermarket|"
    r"restaurant|\bhotel|\bmotel|liquor\s+store|smoke\s+shop|car\s+wash|child\s*care|"
    r"\bdeli\b|pizza|truck\s+stop|drive\s+thru", re.I)
# Kept, but Jeff should look: real estate wording next to an operating business.
FLAG_TITLE = re.compile(r"\bapartments?\b|rental\s+income|mixed|redevelopment", re.I)
FLAG_TAG = re.compile(r"multi-?family|commercial\s+real\s+estate|redevelopment|apartment", re.I)


def sg_number(item: Dict) -> Optional[str]:
    """'sg 11771' / 'SG-11765' / 'SG11711' / 'SG 11680 ' -> '11771'; falls back to the title."""
    for src in (item.get("sgNumber"), item.get("title")):
        m = re.search(r"\bSG[\s\-]*(\d{4,6})\b", src or "", re.I)
        if m:
            return m.group(1)
    return None


def tags(item: Dict) -> List[str]:
    return [t.strip() for t in (item.get("assetClass") or "").split(",") if t.strip()]


def scope(item: Dict) -> Tuple[bool, str]:
    """(in_scope, reason). Reason is set for every skip and for every kept-but-flagged card."""
    title = item.get("title") or ""
    tg = tags(item)
    biz_title = bool(BUSINESS_TITLE.search(title))
    all_out = bool(tg) and all(OUT_TAG.match(t) for t in tg)
    flag = bool(FLAG_TITLE.search(title) or any(FLAG_TAG.search(t) for t in tg))
    if (all_out or OUT_TITLE.search(title)) and not biz_title:
        why = ("asset class is only real estate/licence: " + "; ".join(tg)) if all_out \
            else "title names real estate/licence"
        return False, why
    if flag:
        return True, "kept_flag: operating business named alongside real estate (" + "; ".join(tg) + ")"
    return True, ""


def num(v) -> Optional[float]:
    """Only a real number. grossSales/sde are sometimes prose ("Over $150,000"); that is not a figure."""
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) and v > 0 else None


def price_of(item: Dict) -> Tuple[Optional[float], Optional[str]]:
    p = item.get("askingPrice")
    if isinstance(p, (int, float)) and p > 0:
        return float(p), None
    return None, "Inquire"


def parse_items(items: List[Dict], account: str = "salehgroup") -> Tuple[List[Dict], Dict]:
    """(rows, info). info = {'cards', 'kept', 'skipped': [...], 'flagged': [...], 'dupes'}."""
    rows: List[Dict] = []
    info: Dict = {"cards": len(items), "skipped": [], "flagged": [], "dupes": 0, "no_id": 0}
    seen = set()
    for it in items:
        sg, cid = sg_number(it), it.get("id")
        title = re.sub(r"\s+", " ", it.get("title") or "").strip()
        if not cid or not sg or not title:
            info["no_id"] += 1
            info["skipped"].append({"sg": sg, "title": title, "reason": "no id/SG number/title"})
            continue
        ok, why = scope(it)
        if not ok:
            info["skipped"].append({"sg": sg, "title": title, "reason": why})
            continue
        if sg in seen:
            info["dupes"] += 1
            continue
        seen.add(sg)
        if why:
            info["flagged"].append({"sg": sg, "title": title, "reason": why})
        price, price_text = price_of(it)
        st = it.get("state") or (it.get("states") or [None])[0]
        rows.append(format_listing(
            DETAIL_URL.format(id=cid), account,
            title=title, price=price, price_text=price_text,
            location=it.get("location") or None,
            city=it.get("city") or None,
            state=st or None,
            description=None,
            business_type=(tags(it) or [None])[0],
            revenue=num(it.get("grossSales")),
            cash_flow=num(it.get("sde")),
            status="active"))
    info["kept"] = len(rows)
    return rows, info


class SalehGroupScraper:
    """run_specialized entry: SalehGroupScraper().scrape(account). One request."""

    last_displayed: Optional[Dict] = None
    last_info: Optional[Dict] = None

    def fetch(self) -> Dict:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(API_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.json()

    def scrape(self, account: str = "salehgroup") -> List[Dict]:
        SalehGroupScraper.last_displayed = None
        j = self.fetch()
        items = j.get("data") or []
        rows, info = parse_items(items, account)
        SalehGroupScraper.last_info = info
        stated = j.get("totalActive")
        SalehGroupScraper.last_displayed = {
            "count": len(rows), "source": "counted",
            "note": f"{stated} stated by {INDEX_URL}, {len(items)} cards, "
                    f"{len(info['skipped'])} out of scope"}
        print(f"[salehgroup] {len(rows)} rows of {len(items)} cards (stated {stated}); "
              f"skipped {len(info['skipped'])}, flagged {len(info['flagged'])}, dupes {info['dupes']}")
        return rows
