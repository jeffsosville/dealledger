"""
bma1_scraper.py - Brandywine Mergers & Acquisitions (www.bma1.com).

WHY A SPECIALIZED SCRAPER
/business-listing/ is a WordPress archive of `article.slw_business_listing`
cards (client number, <h2> title linking to its own detail page, a
"Status: ..." line, an anonymous teaser, and a "$ Thousands" Revenue /
Adjusted EBITDA table). V6's generic pass took the whole teaser as the title,
read the table's "$ Thousands" revenue as the ASKING PRICE ($14,838 for an
$8.4M-revenue firm), gave every row the same index URL, and lost one of the
three cards (Client #1413, the only "Available" one). The site publishes no
asking price at all.

WHAT IS EMITTED (one row per card, 3 on 2026-11-23)
  url         the card's own detail page (/business-listing/<slug>/) -- the identity
  title       the <h2> ("Premier Interior Design Firm"), never the teaser
  price       None: the site discloses no asking price (never revenue)
  revenue     the actual-year column of the table x 1,000 (the table says
              "$ Thousands"); a "Proj" column is never used
  cash_flow   None: the table gives Adjusted EBITDA, which is not a cash-flow
              figure we may relabel. It is kept verbatim in the description.
  status      via the shared policy: Available -> active; Sale Pending ->
              active, broker_status_text kept; Sold -> sold
"""

import re
from typing import Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

try:
    from listing_status import apply_status, DROP
except Exception:                                   # keep the module importable alone
    apply_status = None
    DROP = "DROP"

BASE = "https://www.bma1.com"
INDEX_URL = BASE + "/business-listing/"

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

_AMT = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)")


def _text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(" ")).strip() if el else ""


def state_from(teaser: str) -> Optional[str]:
    """State only from 'Based in <...> <State name>' (the teaser's first sentence);
    'Mid-Atlantic' / 'Mid Atlantic' name a region, not a state -> None."""
    m = re.match(r"\s*(?:this\s+)?(?:company|business|firm)?[, ]*based in ([^,.]{3,60})[,.]", teaser, re.I)
    if not m:
        return None
    place = m.group(1).lower()
    for name in sorted(STATE_NAMES, key=len, reverse=True):
        if re.search(r"\b" + name + r"\b", place):
            return STATE_NAMES[name]
    return None


def parse_table(card) -> Dict[str, Optional[float]]:
    """{'revenue': dollars, 'ebitda': dollars, 'year': 'FY25'} from the
    '$ Thousands' table. The first column without 'Proj' is the actual year.
    The amounts are in thousands, as the table states."""
    out = {"revenue": None, "ebitda": None, "year": None}
    tbl = card.find("table")
    if not tbl:
        return out
    rows = [[_text(c) for c in tr.find_all(["th", "td"])] for tr in tbl.find_all("tr")]
    header = next((r for r in rows if any(re.fullmatch(r"FY\d+.*", c) for c in r)), None)
    if not header:
        return out
    cols = [c for c in header if re.fullmatch(r"FY\d+.*", c)]
    idx = next((i for i, c in enumerate(cols) if "proj" not in c.lower()), None)
    if idx is None:
        return out
    out["year"] = cols[idx]
    for r in rows:
        label = r[0].lower() if r else ""
        vals = [c for c in r[1:] if _AMT.search(c)]
        if idx >= len(vals):
            continue
        amt = float(_AMT.search(vals[idx]).group(1).replace(",", "")) * 1000
        if label.startswith("revenue"):
            out["revenue"] = amt
        elif "ebitda" in label:
            out["ebitda"] = amt
    return out


def parse_index(html: str, account: str = "bma1") -> List[Dict]:
    """One row per slw_business_listing card. Cards with no detail link get no row."""
    soup = BeautifulSoup(html, "html.parser")
    rows = []
    for card in soup.select("article.slw_business_listing"):
        a = card.select_one("h2.entry-title a[href]")
        if not a:
            continue
        url = urljoin(BASE, a["href"].split("#")[0])
        title = _text(a)
        client = _text(card.select_one(".slw-bl-client-number"))
        status_line = re.sub(r"^status:\s*", "", _text(card.select_one(".slw-bl-status")), flags=re.I)
        entry = card.select_one(".entry-content, .slw-bl-description")
        # teaser = first paragraph of the entry content (the table follows it)
        p = (entry or card).find("p")
        teaser = _text(p)
        tb = parse_table(card)

        facts = []
        if tb["revenue"]:
            facts.append(f"Revenue {tb['year']} ${tb['revenue']/1000:,.0f}K")
        if tb["ebitda"]:
            facts.append(f"Adjusted EBITDA {tb['year']} ${tb['ebitda']/1000:,.0f}K")
        # figures first: format_listing keeps only the first 500 characters
        desc = " ".join(x for x in [client + "." if client else "",
                                    "; ".join(facts) + "." if facts else "", teaser] if x)

        row = format_listing(
            url, account,
            title=title, price=None, state=state_from(teaser),
            description=desc or None, business_type=None,
            revenue=tb["revenue"], cash_flow=None,
        )
        if status_line:
            if apply_status is not None:
                out = apply_status(row, status_line, "bma1.com")
                if out == DROP:
                    continue
                row = out
            else:
                row["status"] = "sold" if re.search(r"\bsold\b", status_line, re.I) else "active"
                row["broker_status_text"] = status_line
        rows.append(row)
    return rows


class Bma1Scraper:
    """run_specialized entry: Bma1Scraper().scrape(account)."""

    last_displayed: Optional[Dict] = None

    def fetch(self) -> str:
        from curl_cffi import requests as curl_requests
        r = curl_requests.get(INDEX_URL, impersonate="chrome", timeout=45)
        r.raise_for_status()
        return r.text

    def scrape(self, account: str = "bma1") -> List[Dict]:
        Bma1Scraper.last_displayed = None
        html = self.fetch()
        n_cards = len(BeautifulSoup(html, "html.parser").select("article.slw_business_listing"))
        if not n_cards:
            raise RuntimeError("bma1.com: no article.slw_business_listing on /business-listing/")
        rows = parse_index(html, account)
        Bma1Scraper.last_displayed = {
            "count": n_cards, "source": "counted",
            "note": f"article.slw_business_listing cards on {INDEX_URL}"}
        print(f"[bma1] {len(rows)} rows from {n_cards} cards")
        return rows
