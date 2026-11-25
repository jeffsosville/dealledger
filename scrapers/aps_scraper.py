"""
aps_scraper.py — accountingpracticesales.com (The Holmes Group / APS):
CPA, tax and bookkeeping practices for sale.

WHY A SPECIALIZED SCRAPER
The V6 generic crawl read the "3778 listings found" header as the displayed
count and then failed TITLE_REPETITION_REJECT on the paginated result list
(latest run failed; 6 active of 12 rows held). The site is a plain,
server-rendered card list, so one parser reads it exactly.

SOURCE
  GET https://accountingpracticesales.com/united-states/any/N/   N = 0, 1, 2 ...
  20 cards per page, ALL statuses (active and SOLD mixed, newest first).
  Each card is <a class="apslistingitem_n" href=detail>:
      .apslistingitem_lstatus   New / Available / Available after 4/15 /
                                Sale Pending / On Hold / SOLD
      .apslistingitem_lname     title
      .listingstat              Listing # / Location / Annual Revenue /
                                Asking Price / Type
  A page carries a "Next" link (a.pagebuttonthin) while more pages remain.

THE BROKER'S OWN COUNT
  POST /aps360hooks/listingsearch.php (the public search form's XHR) with
  aps_country=United-States, aps_region=any, aps_listingsfilter=active, all
  aps_listingtype_* checked, price/gross 0 -> "275 listings found". Sent once
  per scrape(); a failure there is logged and NOT fatal (the crawl is the
  product, the count is the cross-check).

IDENTITY
  Listing # (e.g. IL6308). The detail URL carries the site's numeric internal
  id ("/21455-Northeast+Ohio.../") which is one-to-one with the Listing #;
  run_specialized.listing_key() keys on that id so a slug or region change
  does not re-key the row. Nothing mutable (price, status) is in the key.

STATUS
  SOLD                       -> status 'sold', broker_status_text 'SOLD'
  Sale Pending / On Hold /
  Available after ...        -> status 'active', label verbatim (Jeff 22 Nov:
                                all of these stay active)
  New / Available            -> active, no label (the default)
  anything else              -> active, label verbatim, counted in stats
                                'unknown_status' and logged

SCOPE
  Canada is out. /united-states/any/ should exclude it; every card is still
  checked (detail URL must sit under /united-states/, Location must not name
  a Canadian province) and a non-US card is dropped and counted ('non_us').
  State comes from Location, else from the URL's state segment (old SOLD
  cards have an empty Location). Virtual practices (Listing # like V5499,
  CAV6646, or "virtual" in the title) are kept ONLY when a US state resolves.

FAILURE
  A page that errors after retries raises ApsCrawlError, and so does a
  "Next" link that leads to a page with no cards, a page whose cards cannot
  be parsed, or a crawl past MAX_PAGES. A short list is never returned as
  success (principle 17).
"""

import re
import time
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

try:
    from specialized_scrapers import format_listing
except Exception:                                   # tests import this file standalone
    def format_listing(url, broker_account, **kw):
        return {"listing_url": url, "broker_account": broker_account, **kw}

DOMAIN = "accountingpracticesales.com"
BASE = "https://accountingpracticesales.com"
INDEX_URL = BASE + "/united-states/any/"
SEARCH_URL = BASE + "/aps360hooks/listingsearch.php"
MAX_PAGES = 300                 # measured 189 pages on 23 Nov (3,778 cards, 20/page); hard stop
PAGE_DELAY = 1.0                # seconds between page requests
RETRIES = 3

US_STATES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "district of columbia": "DC", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR", "guam": "GU",
    "virgin islands": "VI", "u.s. virgin islands": "VI",
}
_ABBR = set(US_STATES.values())

_SOLD = re.compile(r"\bsold\b", re.I)
_DEFAULT_LABELS = {"new", "available"}
_KNOWN = re.compile(r"^(new|available|available after\b.*|sale pending|on hold|sold)$", re.I)


class ApsCrawlError(RuntimeError):
    """The crawl could not be completed; the caller must not treat it as a result."""


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").replace("\xa0", " ")).strip()


def _money(s: str) -> Optional[float]:
    m = re.search(r"\$?\s*([\d][\d,]*(?:\.\d+)?)", s or "")
    if not m:
        return None
    v = float(m.group(1).replace(",", ""))
    return v if v > 0 else None


_CANADA = re.compile(r"\b(canada|ontario|quebec|british columbia|alberta|manitoba|"
                     r"saskatchewan|nova scotia|new brunswick|newfoundland)\b", re.I)
_STATE_WORDS = sorted(US_STATES, key=len, reverse=True)      # "west virginia" before "virginia"


def us_state(location: str) -> Optional[str]:
    """Two-letter code when `location` names a US state/territory ("Ohio", "OH",
    "Central Florida", "Gulf Coast, FL"), else None."""
    loc = _clean(location).lower()
    if not loc:
        return None
    if loc in US_STATES:
        return US_STATES[loc]
    for name in _STATE_WORDS:
        if re.search(rf"\b{re.escape(name)}\b", loc):
            return US_STATES[name]
    for tok in re.findall(r"\b[A-Za-z]{2}\b", location):
        if tok.isupper() and tok in _ABBR:
            return tok
    return None


def url_state(url: str) -> Optional[str]:
    """State from /united-states/<region>/<state>[-north|-south|...]/<id>-slug/."""
    m = re.search(r"/united-states/[^/]+/([^/]+)/\d+-", url or "")
    if not m:
        return None
    seg = re.sub(r"-(north|south|east|west|central)$", "", m.group(1)).replace("-", " ")
    return US_STATES.get(seg)


def _internal_id(url: str) -> Optional[str]:
    m = re.search(r"/(\d+)-[^/]*/?$", url or "")
    return m.group(1) if m else None


def parse_cards(html: str) -> Tuple[List[Dict], bool]:
    """(cards, has_next) for one result page. A card whose Listing # or title is
    missing is a parse failure, not a skip: it raises."""
    soup = BeautifulSoup(html or "", "html.parser")
    cards = []
    for a in soup.select("a.apslistingitem_n"):
        stats = {}
        for st in a.select("div.listingstat"):
            t = st.select_one(".listingstattitle")
            i = st.select_one(".listingstatinfo")
            if t is not None and i is not None:
                stats[_clean(t.get_text()).lower()] = _clean(i.get_text(" "))
        st_el = a.select_one(".apslistingitem_lstatus")
        name_el = a.select_one(".apslistingitem_lname")
        card = {
            "url": a.get("href") or "",
            "status": _clean(st_el.get_text(" ")) if st_el else "",
            "title": _clean(name_el.get_text(" ")) if name_el else "",
            "listing_no": stats.get("listing #", ""),
            "location": stats.get("location", ""),
            "revenue": stats.get("annual revenue", ""),
            "price": stats.get("asking price", ""),
            "type": stats.get("type", ""),
        }
        if not (card["url"] and card["title"] and card["listing_no"] and card["status"]):
            raise ApsCrawlError(f"unparseable card (missing url/title/Listing #/status): {card}")
        cards.append(card)
    has_next = any("next" in _clean(b.get_text()).lower()
                   for b in soup.select("a.pagebuttonthin"))
    return cards, has_next


def to_rows(cards: List[Dict], account: str = "aps") -> Tuple[List[Dict], Dict]:
    """Rows for the in-scope cards plus stats. The same detail URL twice (the
    list shifting while we page) keeps the first card; the same Listing # under
    two URLs is a re-listing and both rows are kept (counted, 'dup_listing_no')."""
    stats = {"cards": len(cards), "active": 0, "sold": 0, "non_us": 0,
             "duplicate": 0, "unknown_status": 0, "virtual_kept": 0,
             "virtual_dropped": 0,
             "state_unresolved": 0, "dup_listing_no": 0}
    rows, seen, seen_nos = [], set(), set()
    labels: Dict[str, int] = {}
    non_us: List[str] = []
    for c in cards:
        is_virtual = bool(re.match(r"^(?:[A-Z]{2,3})?V\d", c["listing_no"])) or \
            bool(re.search(r"\bvirtual\b", c["title"], re.I))
        in_us_path = "/united-states/" in c["url"]
        st = us_state(c["location"]) or url_state(c["url"])
        if not in_us_path or _CANADA.search(c["location"]) or (is_virtual and st is None):
            stats["non_us"] += 1
            if is_virtual:
                stats["virtual_dropped"] += 1
            non_us.append(f"{c['listing_no']}({c['location'] or c['url']})")
            continue
        if st is None:
            stats["state_unresolved"] += 1
        if c["url"] in seen:                    # same card on two pages (list shifted mid-crawl)
            stats["duplicate"] += 1
            continue
        seen.add(c["url"])
        if c["listing_no"] in seen_nos:         # re-listed under a new internal id: kept, counted
            stats["dup_listing_no"] += 1
        seen_nos.add(c["listing_no"])
        if is_virtual:
            stats["virtual_kept"] += 1
        label = c["status"]
        labels[label] = labels.get(label, 0) + 1
        sold = bool(_SOLD.search(label))
        if not _KNOWN.match(label):
            stats["unknown_status"] += 1
        status = "sold" if sold else "active"
        stats["sold" if sold else "active"] += 1
        row = format_listing(
            c["url"], account,
            title=c["title"],
            price=_money(c["price"]),
            price_text=c["price"] or None,
            revenue=_money(c["revenue"]),
            location=c["location"], state=st,
            description=f"{c['type']} practice. Listing # {c['listing_no']}".strip(),
            business_type="Accounting Practice",
            status=status,
        )
        if sold or label.lower() not in _DEFAULT_LABELS:
            row["broker_status_text"] = label
        row["broker_listing_no"] = c["listing_no"]
        rows.append(row)
    stats["labels"] = labels
    stats["non_us_listing_nos"] = non_us
    return rows, stats


def parse_found_count(html: str) -> Optional[int]:
    m = re.search(r"([\d,]+)\s+listings?\s+found", html or "", re.I)
    return int(m.group(1).replace(",", "")) if m else None


# the 10 listing-type checkboxes on the public search form
_TYPES = ("TAX", "CPA", "cfp", "EA", "payroll", "OTHER", "bookkeeping", "audit",
          "REVIEW", "COMPILATION")
_TYPE_VALUES = {"cfp": "CFP-IA"}


def search_form(listingsfilter: str = "active") -> Dict[str, str]:
    d = {"aps_country": "United-States", "aps_region": "any",
         "aps_listingsfilter": listingsfilter,
         "aps_askingprice_min": "0", "aps_askingprice_max": "0",
         "aps_grossamount_min": "0", "aps_grossamount_max": "0"}
    for t in _TYPES:
        d[f"aps_listingtype_{t}"] = _TYPE_VALUES.get(t, t)
    return d


class ApsScraper:
    """run_specialized entry: ApsScraper().scrape(account)."""

    last_displayed: Optional[Dict] = None
    last_stats: Optional[Dict] = None

    def __init__(self, delay: float = PAGE_DELAY):
        self.delay = delay
        self.pages = 0

    def _get(self, url: str) -> str:
        from curl_cffi import requests as curl_requests
        err = None
        for attempt in range(RETRIES):
            try:
                r = curl_requests.get(url, impersonate="chrome", timeout=45)
                if r.status_code == 200:
                    return r.text
                err = f"HTTP {r.status_code}"
            except Exception as e:                  # network error: retry, then fail loud
                err = repr(e)
            time.sleep(2 * (attempt + 1))
        raise ApsCrawlError(f"GET {url} failed after {RETRIES} tries: {err}")

    def fetch_page(self, n: int) -> str:
        return self._get(f"{INDEX_URL}{n}/")

    def broker_count(self) -> Optional[int]:
        """The broker's own active count (one POST). None when unreadable."""
        try:
            from curl_cffi import requests as curl_requests
            r = curl_requests.post(SEARCH_URL, data=search_form("active"),
                                   impersonate="chrome", timeout=45)
            if r.status_code != 200:
                print(f"[aps] broker count POST -> HTTP {r.status_code}")
                return None
            return parse_found_count(r.text)
        except Exception as e:
            print(f"[aps] broker count POST failed: {e!r}")
            return None

    def crawl(self) -> List[Dict]:
        cards: List[Dict] = []
        cards_prev_next = False
        header_total = None
        for n in range(MAX_PAGES):
            html = self.fetch_page(n)
            if n == 0:
                header_total = parse_found_count(html)
            page, has_next = parse_cards(html)
            self.pages = n + 1
            if not page:
                if n == 0 or cards_prev_next:
                    raise ApsCrawlError(
                        f"page {n} has no cards" + (" although page "
                        f"{n - 1} linked Next" if n else ""))
                break
            cards.extend(page)
            cards_prev_next = has_next
            if not has_next:
                # The site can gain or lose a listing during the ~4 minute walk,
                # so allow a small drift; a truly partial crawl is far outside it.
                if (header_total is not None and
                        abs(len(cards) - header_total) > max(3, header_total // 200)):
                    raise ApsCrawlError(
                        f"partial crawl: {len(cards)} cards over {n + 1} pages, "
                        f"site header says {header_total}")
                return cards
            time.sleep(self.delay)
        raise ApsCrawlError(f"no end of pagination within {MAX_PAGES} pages")

    def scrape(self, account: str = "aps") -> List[Dict]:
        ApsScraper.last_displayed = None
        ApsScraper.last_stats = None
        cards = self.crawl()
        rows, stats = to_rows(cards, account)
        stats["pages"] = self.pages
        bc = self.broker_count()
        ApsScraper.last_stats = stats
        if bc:
            ApsScraper.last_displayed = {
                "count": bc, "source": "stated",
                "note": f"POST {SEARCH_URL} aps_listingsfilter=active ({stats['active']} active "
                        f"of {stats['cards']} cards over {self.pages} pages)"}
        print(f"[aps] {len(rows)} rows from {stats['cards']} cards / {self.pages} pages: "
              f"{stats['active']} active, {stats['sold']} sold, {stats['non_us']} non-US dropped, "
              f"{stats['duplicate']} duplicates, {stats['unknown_status']} unknown status; "
              f"broker says {bc}; labels {stats['labels']}")
        return rows
