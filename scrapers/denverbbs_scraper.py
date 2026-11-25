"""
denverbbs.com (Business Brokerage Services, Denver) — specialized scraper.

WHY A SPECIALIZED SCRAPER
https://denverbbs.com/buy-a-business/ is NOT the atmbrokerage.com template.
The server HTML carries no listing cards at all (V6: NO_PATTERN). The page
embeds a small script that, in the visitor's browser, calls the broker's own
CRM (SaleStream) and builds the cards client-side:

    GET https://app.salestreamcrm.com/api/v1/business/public?page=1&limit=1000

The CRM picks the company from the request Origin; with no Origin it answers
HTTP 400, so this scraper sends the same Origin/Referer the page's script
does (https://denverbbs.com). It returns every record in one response (the
`page`/`limit` parameters are ignored; there is no `?paged=N`). Each card
links to https://denverbbs.salestreamcrm.com/buy-a-business/<slug>, a React
shell with no server-rendered detail.

FIELDS (one record per listing)
  _id                  identity (24-hex; the slug follows the title)
  title, slug
  status               active | under LOI | under-contract | sold
  businessOpportunity  the asking price of the business (0 = none stated)
  realEstate           the price of the real estate (0/None/"1" = none)
  cashFlow, grossSales  -> cash_flow, revenue (0 = none stated)
  city[], state[]      lists, frequently empty or duplicated

STATUS (listing_status policy, owner decisions 5/6)
  sold            -> status 'sold', label verbatim (broker_marked sold)
  under LOI / under-contract -> active, label verbatim
  active          -> active, no label
  anything else   -> active, never a guess; unknown labels are counted

SCOPE
  A record with no business price and a real-estate price is a property sale
  (e.g. "Industrial Real Estate"): not emitted, counted and listed in the
  run log (and in the staging CSV) so it can be reviewed, never silently lost.
  Sold service engagements the CRM also lists ("Business Valuation for ...",
  "Liquor License Renewal") are emitted as sold, the way the broker shows them.

IDENTITY
  https://denverbbs.com/buy-a-business/#dl-<_id>   (synthetic, on the broker's
  own domain so broker_domain stays denverbbs.com). The real detail link is
  carried on the row as `detail_url` and at the end of the description.
"""

import re
from typing import Dict, List, Optional, Tuple

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

try:
    from listing_status import apply_status, DROP
except Exception:                                   # pragma: no cover
    apply_status = None
    DROP = "DROP"

try:
    from routebrokers_scraper import _state
except Exception:                                   # pragma: no cover
    def _state(_):
        return None

DOMAIN = "denverbbs.com"
INDEX_URL = "https://denverbbs.com/buy-a-business/"
API_URL = "https://app.salestreamcrm.com/api/v1/business/public?page=1&limit=1000"
DETAIL_BASE = "https://denverbbs.salestreamcrm.com/buy-a-business/"
ORIGIN = "https://denverbbs.com"


def _clean(s) -> str:
    return re.sub(r"\s+", " ", str(s or "").replace("\xa0", " ")).strip()


def _amount(v) -> Optional[float]:
    """A CRM money field: a positive number, else None. 0, None and the stray
    '1' the CRM stores in realEstate when there is no property are 'none'."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f > 1 else None


def _first(v) -> Optional[str]:
    if isinstance(v, list):
        for x in v:
            if _clean(x):
                return _clean(x)
        return None
    return _clean(v) or None


def row_url(rec: Dict) -> str:
    return f"{INDEX_URL}#dl-{rec['_id']}"


def detail_url(rec: Dict) -> str:
    return DETAIL_BASE + str(rec.get("slug") or "")


def parse_records(payload: Dict) -> Tuple[List[Dict], Dict]:
    """Pure. payload = the CRM's JSON -> (rows, stats). Raises ValueError when
    the payload has no `business` list (a failed request must not read as an
    empty broker)."""
    recs = (payload or {}).get("business")
    if not isinstance(recs, list):
        raise ValueError("CRM payload has no 'business' list")
    rows, seen = [], set()
    stats = {"records": len(recs), "for_sale": 0, "sold": 0, "re_only": [],
             "dropped": [], "unknown_status": {}, "labels": {}}
    for rec in recs:
        rid, title = rec.get("_id"), _clean(rec.get("title"))
        if not rid or not title or rid in seen:
            stats["dropped"].append(title or str(rid))
            continue
        seen.add(rid)
        price = _amount(rec.get("businessOpportunity"))
        re_price = _amount(rec.get("realEstate"))
        if price is None and re_price is not None and _clean(rec.get("status")).lower() != "sold":
            stats["re_only"].append(title)          # property-only: out of scope
            continue
        state = _state(_first(rec.get("state")) or "")
        city = _first(rec.get("city"))
        raw = _clean(rec.get("status"))
        row = format_listing(
            row_url(rec), "denverbbs", title=title, price=price,
            city=city, state=state,
            location=", ".join(x for x in (city, state) if x) or None,
            revenue=_amount(rec.get("grossSales")), cash_flow=_amount(rec.get("cashFlow")),
            description=_clean(rec.get("dummyDescription"))[:400] or None,
            status="active")
        row["detail_url"] = detail_url(rec)
        if apply_status is not None:
            out = apply_status(row, raw, DOMAIN)
            if out == DROP:
                stats["dropped"].append(title)
                continue
            row = out
            if row.get("status") == "active" and raw and raw.lower() != "active" \
                    and not row.get("broker_status_text"):
                stats["unknown_status"][raw] = stats["unknown_status"].get(raw, 0) + 1
        if row["status"] == "sold":
            stats["sold"] += 1
        else:
            stats["for_sale"] += 1
            lab = row.get("broker_status_text")
            if lab:
                stats["labels"][lab] = stats["labels"].get(lab, 0) + 1
        rows.append(row)
    return rows, stats


class DenverBBSScraper:
    """run_specialized entry: DenverBBSScraper().scrape(account)."""

    last_displayed: Optional[Dict] = None

    def fetch(self) -> Dict:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(API_URL, impersonate="chrome", timeout=60,
                              headers={"Origin": ORIGIN, "Referer": INDEX_URL})
        r.raise_for_status()
        return r.json()

    def scrape(self, account: str = "denverbbs") -> List[Dict]:
        DenverBBSScraper.last_displayed = None
        rows, st = parse_records(self.fetch())
        if not rows:
            raise RuntimeError("denverbbs: CRM answered but no listings parsed")
        DenverBBSScraper.last_displayed = {
            "count": st["for_sale"], "source": "counted",
            "note": f"for-sale records in the CRM feed behind {INDEX_URL} "
                    f"({st['sold']} sold, {len(st['re_only'])} property-only skipped)"}
        print(f"[denverbbs] {st['for_sale']} for sale {st['labels']}, {st['sold']} sold, "
              f"{len(st['re_only'])} property-only skipped {st['re_only']}, "
              f"unknown statuses {st['unknown_status']}, dropped {st['dropped']}")
        return rows
