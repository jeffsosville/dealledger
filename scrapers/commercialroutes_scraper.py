"""
commercialroutes_scraper.py — commercialroutesforsale.com (Routes to Success,
Tom Arceri, Celebration FL; bread, snack, beverage and FedEx routes, mostly
NY/NJ/PA).

WHY A SPECIALIZED SCRAPER
The whole inventory is one ASP.NET GridView on RouteList.aspx
(table#ctl00_ContentPlaceHolder1_gvRouteList). One <tr> per route, rows
alternating class="even" / class="odd", each row opening
RouteListDetail.aspx?RouteID=<n> from an onclick (there is no <a>). Columns,
read from each cell's data-title:

    Route Number | Route Type | Total Price | Down Payment | Weekly Net |
    City | State | Description

The V6 generic crawl got this wrong three ways:
  * it matched one of the two row classes, so it saw 30 of 61 (2026-11-11);
  * it stored the 500-character Description cell as the title;
  * from 2026-11-12 it followed the header's Sort$ postbacks as pages
    (42 "pages", 2,520 counted rows) and every run since has closed
    failed/TITLE_REPETITION_REJECT, so nothing has been refreshed.

WHAT THIS EMITS (format_listing shape, one row per route)
  url          https://commercialroutesforsale.com/RouteListDetail.aspx?RouteID=<n>
               — the broker's own detail page, the same URL the V6 rows hold
               (listings_direct_inherit_first_seen carries their first_seen)
  title        "<Route Type> Route - <City>, <ST>"  (never the description)
  price        Total Price
  description  "Down payment $X. Weekly net $Y. <Description>"
  business_type Route Type
  status       active (the page lists no sold/pending rows)

Weekly Net is the broker's weekly figure. It is NOT revenue and it is not
annualised into cash_flow here (no stated method for that yet); it is kept
verbatim in the description.

NOT EMITTED
  * the "call us" placeholder row (6107 "We have several other FedEx routes
    available") — no price, city or state; counted in skipped_placeholder.

displayed_count: the routes in the grid (rows with a Route Number and a
price), source 'counted'.
"""

import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "commercialroutesforsale.com"
INDEX_URL = "https://commercialroutesforsale.com/RouteList.aspx"
DETAIL_URL = "https://commercialroutesforsale.com/RouteListDetail.aspx?RouteID={}"
GRID_ID = "ctl00_ContentPlaceHolder1_gvRouteList"


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _money(s: str) -> Optional[float]:
    m = re.search(r"\$?\s*(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*([kKmM])?\b", s or "")
    if not m:
        return None
    v = float(m.group(1).replace(",", ""))
    suf = (m.group(2) or "").lower()
    v *= 1_000 if suf == "k" else 1_000_000 if suf == "m" else 1
    return v if v > 0 else None


def _route_type(s: str) -> str:
    s = _clean(s)
    # "WISE SNACKS" -> "Wise Snacks"; mixed case ("FedEx Home & Ground") kept.
    return s.title() if s.isupper() else s


def parse_grid(html: str) -> Dict:
    """{'rows': [dict per route], 'skipped_placeholder': n, 'grid_rows': n}.
    Raises ValueError if the GridView is not on the page."""
    soup = BeautifulSoup(html or "", "html.parser")
    grid = soup.find(id=GRID_ID) or soup.find("table", class_="GridView")
    if grid is None:
        raise ValueError("RouteList.aspx: GridView table not found")
    out, skipped, grid_rows, seen = [], 0, 0, set()
    for tr in grid.find_all("tr"):
        cells = {td.get("data-title"): _clean(td.get_text(" ", strip=True))
                 for td in tr.find_all("td") if td.get("data-title")}
        if not cells:
            continue                                  # header row (th only)
        grid_rows += 1
        rid = cells.get("Route Number") or ""
        if not rid.isdigit():
            m = re.search(r"RouteID=(\d+)", tr.get("onclick") or "")
            rid = m.group(1) if m else ""
        price = _money(cells.get("Total Price"))
        if not rid or price is None:
            skipped += 1                              # 6107 "call us" placeholder
            continue
        if rid in seen:
            continue
        seen.add(rid)
        out.append({
            "route_id": rid,
            "route_type": _route_type(cells.get("Route Type")),
            "price": price,
            "down": _clean(cells.get("Down Payment")),
            "weekly_net": _clean(cells.get("Weekly Net")),
            "city": _clean(cells.get("City")) or None,
            "state": (_clean(cells.get("State")) or "").upper() or None,
            "description": _clean(cells.get("Description")),
        })
    return {"rows": out, "skipped_placeholder": skipped, "grid_rows": grid_rows}


def to_rows(parsed: List[Dict], account: str = "30194") -> List[Dict]:
    rows = []
    for p in parsed:
        place = ", ".join(x for x in (p["city"], p["state"]) if x) or None
        title = f"{p['route_type'] or 'Route'} Route" if not re.search(
            r"\broutes?\b", p["route_type"] or "", re.I) else p["route_type"]
        if place:
            title = f"{title} - {place}"
        lead = []
        if p["down"]:
            lead.append(f"Down payment {p['down']}.")
        if p["weekly_net"]:
            lead.append(f"Weekly net {p['weekly_net']}.")
        desc = " ".join(lead + ([p["description"]] if p["description"] else [])) or None
        rows.append(format_listing(
            DETAIL_URL.format(p["route_id"]),
            account,
            title=title,
            price=p["price"],
            city=p["city"].title() if p["city"] and p["city"].isupper() else p["city"],
            state=p["state"],
            location=place,
            description=desc,
            business_type=p["route_type"] or None,
            status="active",
        ))
    return rows


class CommercialRoutesScraper:
    """run_specialized entry: CommercialRoutesScraper().scrape(account)."""

    # Last run's broker count, read by run_specialized (displayed_count).
    last_displayed: Optional[Dict] = None

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "30194") -> List[Dict]:
        CommercialRoutesScraper.last_displayed = None
        g = parse_grid(self.fetch())
        rows = to_rows(g["rows"], account)
        CommercialRoutesScraper.last_displayed = {
            "count": len(g["rows"]), "source": "counted",
            "note": (f"GridView rows with a Route Number and price: {len(g['rows'])} of "
                     f"{g['grid_rows']} ({g['skipped_placeholder']} placeholder)")}
        print(f"[commercialroutes] {len(rows)} routes ({g['grid_rows']} grid rows, "
              f"{g['skipped_placeholder']} placeholder skipped), "
              f"{sum(1 for r in rows if r.get('state'))} with state")
        return rows
