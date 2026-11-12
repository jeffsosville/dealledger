"""Job 3 PAGINATION (2026-11-12): pager reading, sold-archive stop, the
Participants-Database card layout (therouteexchange.com) and the Astro
island props feed (corbettrestaurantgroup.com).

Every rule is also tested against input that must keep passing (principle 8).
"""
import json
import os
import sys
from html import escape

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from bs4 import BeautifulSoup  # noqa: E402

import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import (  # noqa: E402
    PaginationHandler, ListingExtractor, card_layout, card_selector_override,
    parse_corbett_props, _astro_decode, astro_island_props, extract_via_api,
)
from junk_filter import is_junk_title  # noqa: E402


def soup(h):
    return BeautifulSoup(h, "html.parser")


# ── numbered pager links (mechanism a) ───────────────────────────────────

TOOLSET_PAGER = """
<ul class="wpv-pagination-nav-links-container">
 <li class="wpv_page_current"><span class="page-link">1</span></li>
 <li><a class="wpv-filter-pagination-link" title="2"
   href="/business-for-sale/?wpv_view_count=596016&#038;wpv_paged=2" data-page="2">2</a></li>
 <li><a href="/business-for-sale/?wpv_view_count=596016&#038;wpv_paged=3">3</a></li>
 <li><a class="wpv-filter-last-link" href="/business-for-sale/?wpv_view_count=596016&#038;wpv_paged=8">Last</a></li>
</ul>"""


def test_toolset_numbered_link_is_followed():
    s = soup(TOOLSET_PAGER)
    u = "https://v-aidgroup.com/business-for-sale"
    assert PaginationHandler.find_next_page(s, u) is None   # no "next" on this pager
    got = PaginationHandler.find_numbered_page(s, u, 2)
    assert got == "https://v-aidgroup.com/business-for-sale/?wpv_view_count=596016&wpv_paged=2"
    assert PaginationHandler.find_numbered_page(s, got, 3).endswith("wpv_paged=3")


def test_bootstrap_pager_on_another_path():
    s = soup("""<ul class="pagination"><li class="page-item active"><span>1</span></li>
      <li class="page-item"><a class="page-link"
        href="https://www.unionbaygroup.com/listings?show=all&amp;page=2">2</a></li></ul>""")
    got = PaginationHandler.find_numbered_page(
        s, "https://www.unionbaygroup.com/listings/all", 2)
    assert got == "https://www.unionbaygroup.com/listings?show=all&page=2"


def test_offset_pager_inside_pagination_nav():
    # krbrokers.com: "2" -> ?limitstart=10 (the number is not in the URL)
    s = soup("""<nav aria-label="Pagination" class="fwre-listing-pagination"><div class="g">
      <div><a href="https://www.krbrokers.com/our-listings/current-listings?limitstart=0#t">1</a></div>
      <div><a href="https://www.krbrokers.com/our-listings/current-listings?limitstart=10#t">2</a></div>
      </div></nav>""")
    got = PaginationHandler.find_numbered_page(
        s, "https://krbrokers.com/kr-business-broker-listings", 2)
    assert got and "limitstart=10" in got


def test_a_two_outside_any_pager_is_not_a_page():
    # A "2" in a menu or a bedroom count, URL without the number, no pager.
    s = soup("""<ul class="menu"><li><a href="/team/">2</a></li></ul>
                <div class="card"><a href="/listing/two-unit-laundromat/">2</a></div>""")
    assert PaginationHandler.find_numbered_page(s, "https://x.com/listings/", 2) is None


def test_other_host_and_self_link_are_not_pages():
    s = soup("""<div class="pagination"><a href="https://other.com/list?page=2">2</a>
                <a href="https://x.com/listings/?page=2">2</a></div>""")
    assert PaginationHandler.find_numbered_page(
        s, "https://x.com/listings/?page=2", 2) is None


def test_wordpress_pager_unchanged():
    # Explicit next still wins; numbered agrees with it.
    s = soup("""<div class="pagination clearfix"><span class="current">1</span>
      <a class="inactive" href="https://f.com/businesses-for-sale/page/2/">2</a>
      <a class="pagination-next" href="https://f.com/businesses-for-sale/page/2/" rel="next">Next</a></div>""")
    u = "https://f.com/businesses-for-sale"
    assert PaginationHandler.find_next_page(s, u) == "https://f.com/businesses-for-sale/page/2/"
    assert PaginationHandler.find_numbered_page(s, u, 2) == "https://f.com/businesses-for-sale/page/2/"


# ── a card's own arrow is not "next" ─────────────────────────────────────

CARD_ARROWS = """
<div class="listingBox"><div class="listingTitle"><a href="https://c.com/listing/traffic-control/">Traffic Control Mfr</a></div>
 <div class="listingFooter"><div class="footerRight"><a href="https://c.com/listing/traffic-control/">&gt;</a></div></div></div>
<div class="listingBox"><div class="listingTitle"><a href="https://c.com/listing/hvac/">HVAC Service Co</a></div>
 <div class="listingFooter"><div class="footerRight"><a href="https://c.com/listing/hvac/">&gt;</a></div></div></div>
"""


def test_card_arrow_is_skipped():
    s = soup(CARD_ARROWS)
    u = "https://c.com/businesses-for-sale"
    cards = {"https://c.com/listing/traffic-control/", "https://c.com/listing/hvac/"}
    # unchanged default: the arrow is (wrongly) taken
    assert PaginationHandler.find_next_page(s, u) == "https://c.com/listing/traffic-control/"
    assert PaginationHandler.find_next_page(s, u, exclude=cards) is None


def test_real_next_after_card_arrows_is_found():
    s = soup(CARD_ARROWS + '<div class="nav"><a href="/businesses-for-sale?pg=2">&gt;</a></div>')
    u = "https://c.com/businesses-for-sale"
    cards = {"https://c.com/listing/traffic-control/", "https://c.com/listing/hvac/"}
    assert PaginationHandler.find_next_page(s, u, exclude=cards) == \
        "https://c.com/businesses-for-sale?pg=2"


# ── therouteexchange.com: Participants Database records ──────────────────

def pdb_record(rid, slug, status, price="$68,000", net="$54,000"):
    link = (f'<span class="pdb-field-data"><a href="https://therouteexchange.com/{slug}/">'
            f'Full Listing</a></span>' if slug else '<span class="pdb-field-data"></span>')
    return (f'<section id="record-{rid}">'
            f'<div class="pdb-field pdb-field-full_listing"> <span class="pdb-field-title">Full Listing</span> {link}</div>'
            f'<div class="pdb-field pdb-field-city"> <span class="pdb-field-title">City</span> <span class="pdb-field-data">Columbiana</span></div>'
            f'<div class="pdb-field pdb-field-state"> <span class="pdb-field-title">State</span> <span class="pdb-field-data">AL</span></div>'
            f'<div class="pdb-field pdb-field-route_type"> <span class="pdb-field-title">Route Type</span> <span class="pdb-field-data">UTZ</span></div>'
            f'<div class="pdb-field pdb-field-asking_price"> <span class="pdb-field-title">Asking Price</span> <span class="pdb-field-data">{price}</span></div>'
            f'<div class="pdb-field pdb-field-average_weekly_sales"> <span class="pdb-field-title">Weekly Sales</span> <span class="pdb-field-data">$6,193</span></div>'
            f'<div class="pdb-field pdb-field-gross_annual_income"> <span class="pdb-field-title">Net Income</span> <span class="pdb-field-data">{net}</span></div>'
            f'<div class="pdb-field pdb-field-status"> <span class="pdb-field-title">Status</span> <span class="pdb-field-data">{status}</span></div>'
            f'<div class="pdb-field pdb-field-route_"> <span class="pdb-field-title">Route #</span> <span class="pdb-field-data">00788</span></div>'
            f'</section>')


TRE = "https://therouteexchange.com/routes-for-sale/"


def tre_cards(html):
    sel = card_selector_override("therouteexchange.com")
    return [c for c in (ListingExtractor.from_card(e, TRE, "therouteexchange.com")
                        for e in soup(html).select(sel)) if c]


def test_tre_layout_is_the_record():
    assert card_selector_override("therouteexchange.com") == "section[id^='record-']"
    assert card_selector_override("www.therouteexchange.com") == "section[id^='record-']"


def test_tre_status_field():
    cs = tre_cards(pdb_record(1, "shelby-county-utz-snack-route-for-sale", "Avail")
                   + pdb_record(2, "fresno-pepperidge-farm-route-for-sale", "Pending")
                   + pdb_record(3, "tulsa-frito-lay-route-for-sale", "SOLD"))
    got = {c["url"].rsplit("/", 2)[-2]: (c["status"], c.get("broker_status_text")) for c in cs}
    assert got == {
        "shelby-county-utz-snack-route-for-sale": ("active", None),
        "fresno-pepperidge-farm-route-for-sale": ("active", "Pending"),
        "tulsa-frito-lay-route-for-sale": ("sold", "SOLD"),
    }


def test_tre_title_price_cash_flow():
    (c,) = tre_cards(pdb_record(1, "shelby-county-utz-snack-route-for-sale", "Avail"))
    assert c["title"].lower().startswith("shelby county utz snack route")
    assert c["asking_price"] == 68000 and c["cash_flow"] == 54000


def test_tre_record_without_link_not_emitted():
    assert tre_cards(pdb_record(9, None, "SOLD")) == []


def test_full_listing_is_a_junk_title():
    assert is_junk_title("Full Listing") and is_junk_title("View Full Listing")
    assert not is_junk_title("Full Service Restaurant in Tulsa")


# ── corbettrestaurantgroup.com: Astro island props ───────────────────────

def astro(v):
    """Encode like Astro: objects {k: [0, v]}, arrays [1, [...]]."""
    if isinstance(v, dict):
        return [0, {k: astro(x) for k, x in v.items()}]
    if isinstance(v, list):
        return [1, [astro(x) for x in v]]
    return [0, v]


def corbett_page(items):
    props = {"allListings": [1, [astro(i) for i in items]]}
    return ('<html><body><astro-island uid="Z" component-url="/_astro/ListingsContainer.BK.js" '
            f'component-export="default" props="{escape(json.dumps(props))}"></astro-island>'
            '<div>Showing 1 to 30 of %d listings</div>'
            '<a href="https://corbettrestaurantgroup.com/">corbettrestaurantgroup.com</a>'
            '</body></html>' % len(items))


def corbett_item(slug, status, typ="full_service", **kw):
    d = {"id": f"uuid-{slug}", "slug": slug, "title": slug.replace("-", " ").title(),
         "status": status, "type": typ, "listing_price": 350000, "cash_flow": 120000,
         "sales": 900000, "city": "", "state": "", "location": "Norfolk County",
         "content": "<p>Busy spot</p>", "deleted_at": None}
    d.update(kw)
    return d


CORBETT = "https://corbettrestaurantgroup.com/listings"


def test_astro_decode():
    assert _astro_decode([0, {"a": [0, 1], "b": [1, [[0, "x"], [0, None]]]}]) == \
        {"a": 1, "b": ["x", None]}


def test_corbett_statuses_and_urls():
    items = [corbett_item("italian-pizzeria", "new_listing"),
             corbett_item("breakfast-cafe", "price_reduced"),
             corbett_item("irish-pub", "under_agreement"),
             corbett_item("pizza-and-subs", "off_market"),
             corbett_item("greek-diner", "sold"),
             corbett_item("brewery", "active")]
    rows = parse_corbett_props(corbett_page(items), CORBETT, "corbett")
    got = {r["url"]: (r["status"], r["broker_status_text"]) for r in rows}
    base = "https://corbettrestaurantgroup.com/listings/"
    assert got == {
        base + "italian-pizzeria": ("active", None),
        base + "breakfast-cafe": ("active", None),
        base + "irish-pub": ("active", "Under Agreement"),
        base + "pizza-and-subs": ("active", "Off Market"),
        base + "greek-diner": ("sold", "Sold"),
        base + "brewery": ("active", None),
    }
    r = rows[0]
    assert (r["asking_price"], r["cash_flow"], r["revenue"]) == (350000, 120000, 900000)
    assert r["broker_domain"] == "corbettrestaurantgroup.com"
    assert r["url_is_listing_specific"] is True
    assert len({x["id"] for x in rows}) == 6


def test_corbett_for_lease_type_not_emitted():
    items = [corbett_item("pub-space", "active", typ="for_lease"),
             corbett_item("cafe", "active")]
    dropped = []
    rows = parse_corbett_props(corbett_page(items), CORBETT, "corbett", dropped)
    assert [r["url"].rsplit("/", 1)[-1] for r in rows] == ["cafe"]
    assert dropped == ["For Lease (type)"]


def test_corbett_handler_via_registry_and_other_hosts():
    page = corbett_page([corbett_item("breakfast-lunch-cafe", "active")])
    rows = extract_via_api(CORBETT, page, "corbett", None)
    assert rows and rows[0]["url"].endswith("/listings/breakfast-lunch-cafe")
    # same markup on another host: handler declines
    assert v6._handler_corbett("https://example.com/listings", page, "x", None) is None


def test_no_props_blob_is_none():
    assert parse_corbett_props("<html><body>nothing</body></html>", CORBETT, "c") is None
    assert astro_island_props("<astro-island component-url='/x/Other.js' props='{}'></astro-island>",
                              "ListingsContainer") is None


# ── the index loop itself: sold-archive stop and URL-keyed repeat checks ──

from collections import Counter  # noqa: E402


class _Cache:
    patterns = {}

    def __init__(self, sel):
        self.sel = sel

    def get(self, d):
        return {"container_selector": self.sel}

    def store(self, *a):
        pass

    def predict(self, *a):
        return None


class _Fetcher:
    def __init__(self, pages):
        self.pages, self.calls = pages, []
        self._proxy_domains_runtime = set()
        self.last_scroll_growth = 0

    def fetch(self, url, use_proxy=False):
        self.calls.append(url)
        if url in self.pages:
            return self.pages[url], "requests"
        raise RuntimeError("404 " + url)

    def fetch_playwright(self, url, use_proxy=False):
        raise RuntimeError("no playwright in tests")


def _card(i, sold=False, title=None):
    t = title or f"Profitable Business Number {i}"
    st = "<p>Status: Sold</p>" if sold else ""
    return (f'<div class="lc"><h3><a href="https://ex.com/listing/biz-number-{i}/">{t}</a></h3>'
            f'<p>Asking Price: ${100 + i},000</p>{st}</div>')


def _page(ids, nxt=None, sold=(), title=None):
    h = "<html><body>" + "".join(_card(i, i in sold, title) for i in ids)
    if nxt:
        h += f'<div class="pagination"><a class="next" href="{nxt}">Next</a></div>'
    return h + "</body></html>"


def _run(pages, start, monkeypatch):
    monkeypatch.setattr(v6.time, "sleep", lambda *_: None)
    monkeypatch.setattr(v6, "ENRICH_DETAIL_PAGES", False, raising=False)
    s = v6.DealLedgerScraper.__new__(v6.DealLedgerScraper)
    s.pattern_cache, s.fetcher = _Cache("div.lc"), _Fetcher(pages)
    s.supabase_writer = s.failure_writer = None
    s.stats = {"brokers_attempted": 0, "brokers_success": 0, "brokers_failed": 0,
               "failure_types": Counter(), "patterns_learned": 0,
               "detail_pages_fetched": 0, "total_listings": 0, "listings_with_price": 0,
               "listings_with_cashflow": 0, "listings_with_state": 0,
               "verticals": Counter()}
    s.failures, s.embed_brokers, s._displayed = [], [], None
    out = s._scrape_broker_inner({"name": "ex.com", "url": start, "domain": "ex.com"})
    return out, [c for c in s.fetcher.calls if "/listing/" not in c]


def test_sold_archive_page_stops_the_walk(monkeypatch, capsys):
    P = {"https://ex.com/l": _page(range(1, 6), "https://ex.com/l?page=2", sold={5}),
         "https://ex.com/l?page=2": _page(range(6, 11), "https://ex.com/l?page=3",
                                          sold=set(range(6, 11))),
         "https://ex.com/l?page=3": _page(range(11, 16), sold=set(range(11, 16)))}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert "https://ex.com/l?page=3" not in calls
    assert "sold_only p2" in capsys.readouterr().out
    assert Counter(l["status"] for l in out) == {"active": 4, "sold": 1}


def test_a_page_with_one_active_card_is_walked(monkeypatch):
    P = {"https://ex.com/l": _page(range(1, 6), "https://ex.com/l?page=2"),
         "https://ex.com/l?page=2": _page(range(6, 11), "https://ex.com/l?page=3",
                                          sold={6, 7, 8, 9}),
         "https://ex.com/l?page=3": _page(range(11, 16))}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert "https://ex.com/l?page=3" in calls
    assert len(out) == 15


def test_same_cta_titles_on_every_page_still_paginates(monkeypatch):
    # Every card titled by its CTA; the detail URLs differ page to page.
    P = {"https://ex.com/l": _page(range(1, 6), "https://ex.com/l?page=2",
                                   title="Business Opportunity Listing"),
         "https://ex.com/l?page=2": _page(range(6, 11), title="Business Opportunity Listing")}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert "https://ex.com/l?page=2" in calls
    assert len({l["url"] for l in out}) == 10


def test_page_one_served_again_still_stops(monkeypatch, capsys):
    # A guessed / ignored page parameter that serves page 1 again: same URLs.
    P = {"https://ex.com/l": _page(range(1, 6), "https://ex.com/l?page=2"),
         "https://ex.com/l?page=2": _page(range(1, 6), "https://ex.com/l?page=3"),
         "https://ex.com/l?page=3": _page(range(11, 16))}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert "https://ex.com/l?page=3" not in calls
    assert len(out) == 5


def test_numbered_pager_walked_in_the_loop(monkeypatch):
    def pg(ids, n):
        links = "".join(f'<li><a href="/l/?view=7&wpv_paged={k}">{k}</a></li>'
                        for k in range(1, 4) if k != n)
        return _page(ids).replace("</body>", f'<ul class="wpv-pagination">{links}</ul></body>')
    P = {"https://ex.com/l": pg(range(1, 6), 1),
         "https://ex.com/l/?view=7&wpv_paged=2": pg(range(6, 11), 2),
         "https://ex.com/l/?view=7&wpv_paged=3": pg(range(11, 14), 3)}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert calls[:3] == ["https://ex.com/l", "https://ex.com/l/?view=7&wpv_paged=2",
                         "https://ex.com/l/?view=7&wpv_paged=3"]
    assert len(out) == 13


def test_pager_stall_crosses_a_run_of_identical_cards(monkeypatch):
    # unionbaygroup.com: pages 2-3 hold only cards linking one shared URL.
    shared = ('<div class="lc"><h3><a href="https://ex.com/listing/investment-opportunity/">'
              'Investment Opportunity</a></h3><p>Price: $44,000</p></div>')

    def pg(body, n):
        return f'<html><body>{body}<div class="pagination"><a class="next" href="https://ex.com/l?page={n}">Next</a></div></body></html>'
    P = {"https://ex.com/l": pg(_page(range(1, 4)) + shared, 2),
         "https://ex.com/l?page=2": pg(shared * 5, 3),
         "https://ex.com/l?page=3": pg(shared * 5, 4),
         "https://ex.com/l?page=4": _page(range(10, 16))}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert "https://ex.com/l?page=4" in calls
    assert len({l["url"] for l in out}) == 3 + 1 + 6


def test_pager_stall_limit(monkeypatch, capsys):
    shared = ('<div class="lc"><h3><a href="https://ex.com/listing/investment-opportunity/">'
              'Investment Opportunity</a></h3><p>Price: $44,000</p></div>')

    def pg(body, n):
        return f'<html><body>{body}<div class="pagination"><a class="next" href="https://ex.com/l?page={n}">Next</a></div></body></html>'
    P = {"https://ex.com/l": pg(_page(range(1, 4)) + shared, 2)}
    for n in range(2, 8):
        P[f"https://ex.com/l?page={n}"] = pg(shared * 5, n + 1)
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    # page 2 and 3 tolerated, page 4 stops
    assert "https://ex.com/l?page=4" in calls and "https://ex.com/l?page=5" not in calls


def test_pager_link_back_to_a_visited_page_stops(monkeypatch):
    P = {"https://ex.com/l": _page(range(1, 6), "https://ex.com/l?page=2"),
         "https://ex.com/l?page=2": _page(range(6, 11), "https://ex.com/l")}
    out, calls = _run(P, "https://ex.com/l", monkeypatch)
    assert calls.count("https://ex.com/l") == 1 and len(out) == 10


# ── practiceconcepts.com: BusinessBroker.net records ─────────────────────

def pc_record(n, slug, title, status, revenue="$1,200,000"):
    st = f'<div class="cell listing-status legacygreen"><span><strong>{status}</strong></span></div>' if status else ""
    return (f'<li class="element_row{n}"><div class="cell listings-picture"><a href="/listings/{slug}">'
            f'<img src="x.jpg"/></a></div>{st}<div class="cell title"><div class="link-title"><span><strong>'
            f'{title}<br/></strong></span></div><div class="location-link-title"><span class="moreinfo">'
            f'<strong><a href="/listings/{slug}">More Info</a>&gt;&gt;</strong></span></div></div>'
            f'<div class="cell details"><ul><li><h4>REVENUE</h4><p><strong>{revenue}</strong></p></li>'
            f'<li><h4>STATE</h4><p><strong>California</strong></p></li><li><h4>REFERENCE</h4>'
            f'<p><strong>CA-7{n}</strong></p></li></ul></div></li>')


def test_practiceconcepts_record_layout():
    u = "https://practiceconcepts.com/pages/practices-for-sale"
    html = "<ul>" + pc_record(1, "sacramento-area-grossing-over-1-2-million",
                              "Sacramento area Grossing Over $1.2 million", "Sold") \
        + pc_record(2, "bay-area-practice-for-sale",
                    "Highly Profitable Optometry Practice – San Jose Area", "") \
        + pc_record(3, "500k-gross-coastal-orange-county-ca-optometry",
                    "$500K Gross Coastal Orange County, CA Optometry", "") + "</ul>"
    sel = card_selector_override("practiceconcepts.com")
    cs = [c for c in (ListingExtractor.from_card(e, u, "practiceconcepts.com")
                      for e in soup(html).select(sel)) if c]
    by = {c["url"].rsplit("/", 1)[-1]: c for c in cs}
    assert len(by) == 3
    s = by["sacramento-area-grossing-over-1-2-million"]
    assert (s["status"], s["broker_status_text"]) == ("sold", "Sold")
    assert s["title"] == "Sacramento area Grossing Over $1.2 million"
    assert s["asking_price"] is None and s["revenue"] == 1_200_000   # Revenue is not the price
    b = by["bay-area-practice-for-sale"]
    assert b["status"] == "active" and b["title"].startswith("Highly Profitable Optometry")
    # a name the junk gate reads as a price fragment falls back to the slug, not dropped
    assert "Orange County" in by["500k-gross-coastal-orange-county-ca-optometry"]["title"]


def test_layout_domains_unchanged_elsewhere():
    assert card_layout("example.com") is None
    assert card_selector_override("healthcarebizsales.com") == "article.type-business"
