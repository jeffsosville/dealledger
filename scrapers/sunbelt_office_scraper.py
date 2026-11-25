"""
sunbelt_office_scraper.py — Sunbelt office sites on the Next.js/Sanity template
(sunbeltmidwest.com; sunbeltsd.com serves the identical feed, see below).

WHY A SPECIALIZED SCRAPER
The index shows "125 Total Sunbelt Business Search Results" but only 20 cards
are server-rendered, and `?page=N` returns the SAME 20 (the paging is client
side). Our V6 crawl therefore held ~22 rows, and its price reader took the
revenue figure as the asking price. The page's <script id="__NEXT_DATA__">
carries every listing at props.pageProps.page.pageBuilder[<n>].allBusiness,
so one GET reads all of them. No pagination is built or needed.

IDENTITY
listingID verbatim, P/F prefixes kept ("P59694", "F60218", "P3-2135").
Detail URL = /buy-a-business/search-businesses-for-sale/{slug}?slug=search-businesses-for-sale
where slug is the listingID (one id arrives with a stray leading space,
" 1960430"; the site's own `slug` field has it trimmed, and so do we).
Price never enters the identity (principle 4).

PRICE (verified against detail pages on 2026-11-23)
  useBusinessTextPrice true  -> not disclosed: price None, price_text = the
                                broker's words ("Not Disclosed", "Undisclosed.
                                Will review IOIs.") so price_disclosure flags it.
  otherwise                  -> businessPrice, plus realEstateAskingPrice only
                                when realEstateIncludedInAskingPrice == "yes"
                                (59406: 3,439,500 + 560,500 = 4,000,000 shown as
                                "Asking Price: $4,000,000"; 58092 has real estate
                                350,000 "no" and shows 99,000).
  no businessPrice at all    -> the page shows only "Real Estate Asking Price"
                                (property-led rows), so that is the price.
  revenue / cashFlow (else EBITDA) go to their own fields.

STATUS
There is no status field in the feed. `realEstateStatus` ("Leased"/"Owned") is
the PROPERTY's tenure, not the listing's, and is deliberately never fed to the
status rules (43 rows would be dropped as "Leased"). Title and the broker's
price text are run through listing_status so a "SOLD" / "Under Contract"
marker would be honoured; on 2026-11-23 none of the 125 carries one.

PARAMETERISED BY DOMAIN
SunbeltOfficeScraper(domain).scrape(account). sunbeltsd.com serves the same
125 listings; which domain owns the feed is the owner's decision, so only
sunbeltmidwest.com is registered. Adding sunbeltsd.com is one BROKERS entry.
"""

import json
import re
from typing import Dict, List, Optional

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

try:
    from listing_status import apply_status, DROP
except Exception:                                   # pragma: no cover
    apply_status = None
    DROP = "DROP"

INDEX_PATH = "/buy-a-business/search-businesses-for-sale"
_NEXT = re.compile(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_TOTAL = re.compile(r"(\d[\d,]*)\s+Total\b[^<]{0,60}?\bResults\b", re.I)
# A status word in a title, only as a standalone marker (never a bare word in prose).
_TITLE_MARK = re.compile(
    r"(?:^|\s[-–—|]\s|[\[(])(sold|under contract|sale pending|pending|under loi|coming soon)"
    r"(?:\s[-–—|]\s|[\])]|:\s|!?\s*$)", re.I)


def _clean(s) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _num(v) -> Optional[float]:
    if isinstance(v, bool) or v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 0 else None


def find_all_business(next_data: Dict) -> List[Dict]:
    """The allBusiness array, found by key among pageBuilder blocks."""
    blocks = (((next_data.get("props") or {}).get("pageProps") or {})
              .get("page") or {}).get("pageBuilder") or []
    for b in blocks:
        if isinstance(b, dict) and isinstance(b.get("allBusiness"), list):
            return b["allBusiness"]
    raise ValueError("no pageBuilder block with allBusiness (template changed?)")


def stated_total(html: str) -> Optional[int]:
    text = re.sub(r"<[^>]+>", " ", html or "")
    m = _TOTAL.search(re.sub(r"\s+", " ", text))
    return int(m.group(1).replace(",", "")) if m else None


def asking_price(rec: Dict):
    """(price, price_text). price_text only when the broker withholds the price."""
    if rec.get("useBusinessTextPrice") is True:
        return None, _clean(rec.get("businessTextPrice")) or None
    biz = _num(rec.get("businessPrice"))
    re_price = _num(rec.get("realEstateAskingPrice"))
    if biz is None:
        return re_price, None
    if re_price and str(rec.get("realEstateIncludedInAskingPrice") or "").lower() == "yes":
        return biz + re_price, None
    return biz, None


def _status(rec: Dict, title: str, domain: str, row: Dict):
    """Row with status applied, or DROP. Only title and price text are read."""
    if apply_status is None:
        return row
    for text in (title, _clean(rec.get("businessTextPrice"))):
        cand = text
        m = _TITLE_MARK.search(text or "")
        if m:
            cand = m.group(1)
        out = apply_status(row, cand, domain)
        if out == DROP:
            return DROP
        row = out
    return row


def to_rows(records: List[Dict], domain: str, account: str) -> Dict:
    base = f"https://{domain}{INDEX_PATH}"
    rows, seen, dropped = [], set(), []
    for rec in records:
        lid = _clean(rec.get("listingID"))
        slug = _clean(rec.get("slug")) or lid
        if not lid or lid in seen:
            continue
        seen.add(lid)
        title = _clean(rec.get("title"))
        price, ptext = asking_price(rec)
        locs = [l.get("location") for l in (rec.get("locations") or []) if isinstance(l, dict)]
        inds = [i.get("industry") for i in (rec.get("industries") or []) if isinstance(i, dict)]
        row = format_listing(
            f"{base}/{slug}?slug=search-businesses-for-sale", account,
            title=title, price=price, price_text=ptext,
            location=", ".join(l for l in locs if l) or None,
            state=_state(" ".join(l for l in locs if l)),
            business_type=inds[0] if inds else None,
            revenue=_num(rec.get("revenue")),
            cash_flow=_num(rec.get("cashFlow")) or _num(rec.get("EBITDA")),
            status="active")
        row = _status(rec, title, domain, row)
        if row == DROP:
            dropped.append(lid)
            continue
        rows.append(row)
    return {"rows": rows, "dropped": dropped}


class SunbeltOfficeScraper:
    """run_specialized entry: SunbeltOfficeScraper("sunbeltmidwest.com").scrape(account)."""

    last_displayed: Dict[str, Dict] = {}            # domain -> {count, source, note}

    def __init__(self, domain: str):
        self.domain = domain

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(f"https://{self.domain}{INDEX_PATH}", impersonate="chrome", timeout=60)
        r.raise_for_status()
        return r.text

    def parse(self, html: str) -> Dict:
        m = _NEXT.search(html or "")
        if not m:
            raise ValueError(f"{self.domain}: no __NEXT_DATA__ block")
        records = find_all_business(json.loads(m.group(1)))
        out = to_rows(records, self.domain, self.domain.split(".")[0])
        out["array_len"], out["stated"] = len(records), stated_total(html)
        return out

    def scrape(self, account: Optional[str] = None) -> List[Dict]:
        SunbeltOfficeScraper.last_displayed.pop(self.domain, None)
        p = self.parse(self.fetch())
        rows = p["rows"]
        if account:
            for r in rows:
                r["broker_account"] = account
        stated, n = p["stated"], p["array_len"]
        SunbeltOfficeScraper.last_displayed[self.domain] = {
            "count": stated or n, "source": "stated" if stated else "counted",
            "note": f"'N Total ... Results' on index; feed array {n}; {len(rows)} rows emitted"
                    f"{'; dropped ' + ','.join(p['dropped']) if p['dropped'] else ''}"}
        print(f"[{self.domain}] {len(rows)} listings (feed {n}, stated {stated}, dropped {len(p['dropped'])})")
        if stated and stated != n:
            print(f"[{self.domain}] WARNING stated {stated} != feed {n}")
        return rows
