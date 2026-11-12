"""Real-links (job 3, 2026-11-12): the card shows its own detail href but V6
stored a different one (or the index URL).

  A  taxonomy archive link outranks the detail link   (indianabusinessadvisors, clandbiz)
  B  the card IS, or sits inside, an <a href>          (companysellers, jobahcri, baltimore)
  C  whole index embedded as JSON, JS pager            (iconic.co lx-data)
"""
import json
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import (  # noqa: E402
    ListingExtractor, _best_detail_link, _enclosing_link, _is_taxonomy_href,
    stable_listing_id)


def _el(html, sel):
    return BeautifulSoup(html, "html.parser").select_one(sel)


# ── A: taxonomy archive links ──────────────────────────────────────────────

INDIANA = ('<div class="acadp-card"><a href="https://indianabusinessadvisors.com/acadp_listings/25-year-brand/">'
           '<img></a><h3><a href="https://indianabusinessadvisors.com/acadp_listings/25-year-brand/">25+ Year Brand</a></h3>'
           '<a href="https://indianabusinessadvisors.com/listing-category-2/manufacturing/">Manufacturing</a>'
           '<span>$2,100,000.00</span></div>')

CLAND = ('<div class="card"><a href="https://www.clandbiz.com/status/business/">business</a>'
         '<a href="https://www.clandbiz.com/label/active/">Active</a>'
         '<a href="https://www.clandbiz.com/business/popular-korean-fast-casual/">Popular Korean Fast-Casual</a>'
         '<a href="https://www.clandbiz.com/agent/sunkkim/">Sun Kim</a><span>$299,230</span></div>')


def test_category_chip_does_not_outrank_listing():
    el = _el(INDIANA, "div.acadp-card")
    assert _best_detail_link(el) == "https://indianabusinessadvisors.com/acadp_listings/25-year-brand/"


def test_houzez_status_chip_does_not_outrank_listing():
    el = _el(CLAND, "div.card")
    assert _best_detail_link(el) == "https://www.clandbiz.com/business/popular-korean-fast-casual/"


def test_card_with_only_an_archive_link_is_unchanged():
    el = _el('<div class="c"><a href="/status/for-sale/">For sale</a> Laundromat $90,000</div>', "div.c")
    assert _best_detail_link(el) == "/status/for-sale/"


def test_taxonomy_matches_archives():
    for u in ("https://indianabusinessadvisors.com/listing-category-2/retail/",
              "https://www.clandbiz.com/status/business/", "https://www.clandbiz.com/label/new/",
              "https://www.deliveryroutesforsale.com/type/linehaul/",
              "https://www.usroutesales.com/listing-category/p-and-d-route/",
              "/agent/sunkkim/", "/property-type/retail/", "/business_category/food/"):
        assert _is_taxonomy_href(u), u


def test_taxonomy_never_matches_real_detail_urls():
    # principle 8: URL shapes of real listings seen in snapshots
    for u in ("https://www.clandbiz.com/business/korean-restaurant-palisades-park/",
              "https://indianabusinessadvisors.com/acadp_listings/25-year-brand-with-backlog/",
              "https://baltimorebusinessbrokers.com/business-detail/1126",
              "https://www.companysellers.com/businesses-for-sale/10023/",
              "https://jobahcri.com/listings/retail-commercial-building-for-sale",
              "https://iconic.co/listings/big-bear-lake-cleaning-company/",
              "https://centralbrokers.com/business-listings/assisted-living-facility-15727/",
              "https://americangymtrader.com/gyms-for-sale/1546",
              "https://www.naabconsulting.com/practice-listing/cpa-practice-cincinnati-9643/",
              "https://robbinspellegrino.com/marketplace/some-listing",
              "https://www.healthcarebizsales.com/business/medical-spa/",
              "https://example.com/listing/?listing=5255-Playa",
              "https://example.com/property/typewriter-repair-shop/",
              "https://example.com/listings/city-cafe-for-sale/",
              "https://example.com/business/status-quo-barbershop/"):
        assert not _is_taxonomy_href(u), u


# ── B: card is / sits inside an <a href> ───────────────────────────────────

def test_card_that_is_the_anchor():
    el = _el('<a class="bfs-row" href="/businesses-for-sale/10023/"><h2>Massage Franchise</h2>'
             '<span>$450,000</span></a>', "a.bfs-row")
    assert _best_detail_link(el) == "/businesses-for-sale/10023/"


def test_card_inside_anchor():
    html = ('<div class="grid"><a href="/listings/retail-building-hwy-99"><div class="img"></div>'
            '<div class="flex p-6"><h3>Retail / Commercial Building</h3><p>$1,200,000</p></div></a></div>')
    el = _el(html, "div.flex.p-6")
    assert _best_detail_link(el) is None
    assert _enclosing_link(el) == "/listings/retail-building-hwy-99"


def test_enclosing_link_refuses_dead_anchors_and_distance():
    assert _enclosing_link(_el('<a href="#x"><div class="c">t</div></a>', "div.c")) is None
    assert _enclosing_link(_el('<a href="javascript:void(0)"><div class="c">t</div></a>', "div.c")) is None
    deep = '<a href="/listing/1"><div><div><div><div><div class="c">t</div></div></div></div></div></a>'
    assert _enclosing_link(_el(deep, "div.c")) is None
    assert _enclosing_link(_el('<section><div class="c">t</div></section>', "div.c")) is None


def test_from_card_uses_enclosing_link_end_to_end():
    base = "https://jobahcri.com/listings"
    html = ('<div class="grid"><a href="/listings/approved-development-opportunity">'
            '<div class="card-body"><h3>Approved Development Opportunity Retail Center</h3>'
            '<p>Pixley, California</p><p>Asking $1,250,000</p></div></a></div>')
    c = ListingExtractor.from_card(_el(html, "div.card-body"), base, "jobahcri.com")
    assert c is not None
    assert c["url"] == "https://jobahcri.com/listings/approved-development-opportunity"
    assert c["id"] == stable_listing_id(c["title"], c["url"], base, "jobahcri.com")


def test_own_link_beats_enclosing_link():
    # a card with its own detail link never looks upward
    html = ('<a href="/listings/"><div class="c"><h3><a href="/listing/77-bakery">Bakery</a></h3>'
            '<span>$90,000</span></div></a>')
    el = _el(html, "div.c")
    assert _best_detail_link(el) == "/listing/77-bakery"


def test_plain_card_unchanged():
    # control shape (robbinspellegrino/peak-like): inner title link, no anchor ancestor
    html = ('<div class="item"><h3><a href="/marketplace/hvac-company">HVAC Company</a></h3>'
            '<a href="/category/hvac/">HVAC</a><span>$1,000,000</span></div>')
    el = _el(html, "div.item")
    assert _best_detail_link(el) == "/marketplace/hvac-company"
    assert _enclosing_link(el) is None


def test_baltimore_card_selector():
    assert v6.card_selector_override("baltimorebusinessbrokers.com") == "a.business-listing-card--list"
    assert v6.card_layout("baltimorebusinessbrokers.com").get("fields") is None
    assert v6.card_selector_override("www.healthcarebizsales.com") == "article.type-business"


# ── C: lx-data embedded index ──────────────────────────────────────────────

def _lx_page(listings):
    return ('<html><body><div class="lx-card__body">x</div>'
            '<script type="application/json" id="lx-data">'
            + json.dumps({"kind": "all", "listings": listings}) + '</script></body></html>')


LX = [
    {"id": "lst_1", "status": "live", "title": "Established HVAC service company serving Owensboro",
     "href": "/listings/established-hvac-owensboro/", "loc": "Owensboro, KY", "st": "KY",
     "price": 900000, "rev": 2000000, "profit": 300000, "text": "hvac"},
    {"id": "lst_2", "status": "exploring", "title": "Metal stamping manufacturer in Texas",
     "href": "/listings/metal-stamping-texas/", "loc": "Grand Prairie, TX", "st": "TX",
     "price": None, "rev": None, "profit": None, "text": "metal"},
    {"id": "lst_3", "status": "live", "title": "Franchised residential bathroom remodeler",
     "href": "/listings/franchised-bathroom-remodeler/", "loc": "Denver Metro Area", "st": None,
     "price": 249000, "rev": 535638, "profit": 67955, "text": "bath"},
    {"id": "lst_4", "status": "live", "title": "Owner-independent landscaping company near Philadelphia",
     "href": "/listings/landscaping-philadelphia/", "loc": "Glenside, PA", "st": "PA",
     "price": None, "priceText": "Not disclosed", "rev": 900000, "profit": 200000, "text": "lawn"},
    {"id": "lst_1", "status": "live", "title": "Established HVAC service company serving Owensboro",
     "href": "/listings/established-hvac-owensboro/", "loc": "Owensboro, KY", "st": "KY",
     "price": 900000, "rev": 2000000, "profit": 300000, "text": "hvac dup"},
]


def test_lx_data_emits_live_only_keyed_on_href():
    base = "https://iconic.co/listings/"
    rows = v6.extract_via_api(base, _lx_page(LX), "iconic.co", None)
    assert [r["url"] for r in rows] == ["https://iconic.co/listings/established-hvac-owensboro/",
                                        "https://iconic.co/listings/franchised-bathroom-remodeler/",
                                        "https://iconic.co/listings/landscaping-philadelphia/"]
    for r in rows:
        assert r["id"] == stable_listing_id(r["title"], r["url"], base, "iconic.co")
        assert r["status"] == "active" and r["url_is_listing_specific"]
    assert rows[0]["asking_price"] == 900000 and rows[0]["state"] == "KY" and rows[0]["city"] == "Owensboro"
    assert [r["price_not_disclosed"] for r in rows] == [False, False, True]


def test_lx_data_absent_is_not_claimed():
    assert v6._handler_lx_data("https://x.com/", "<html><div id='lx'></div></html>", "x", None) is None
    assert v6._handler_lx_data("https://x.com/", '<script id="lx-data">not json</script>', "x", None) is None
