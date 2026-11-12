"""WPRestScraper status / index-intersection / price tests (2026-11-11).

Install as tests/test_wp_rest_status.py. Offline: no HTTP, no Supabase.
The scrapers dir is APPENDED to sys.path so PYTHONPATH (e.g. a patched copy)
takes precedence over the repo copy.
"""
import hashlib
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import specialized_scrapers as ss  # noqa: E402
from specialized_scrapers import (  # noqa: E402
    WPRestScraper, wp_status_value, wp_slug_sold, wp_badge_status, wp_money,
    wp_card_financials, wp_price_hidden,
)


def _item(link, title, slug=None, class_list=(), acf=None, meta=None,
          date="2026-01-02T00:00:00"):
    return {"link": link, "title": {"rendered": title},
            "slug": slug or link.rstrip("/").rsplit("/", 1)[-1],
            "class_list": list(class_list), "acf": acf or {}, "meta": meta or {},
            "date": date, "modified": date}


def _scraper(index_url=None, off_index="inactive"):
    s = WPRestScraper.__new__(WPRestScraper)
    s.domain, s.rest_base = "example.com", "listing"
    s.fetch_financials = False
    s.index_urls = [index_url] if index_url else []
    s.index_stats = {}
    s.off_index = off_index
    return s


# ── status vocabulary ───────────────────────────────────────────────────────
def test_status_value_mapping():
    for v in ("Sold", "sold-out", "soldout", "past", "Closed", "recently_sold"):
        assert wp_status_value(v) == "sold", v
    # decision 6: the under-contract family stays active, label kept
    for v in ("Under Contract", "pending", "sale-pending", "pending-sale", "Off Market",
              "under offer", "Under LOI", "Has Accepted Offer", "accepted-offer"):
        assert wp_status_value(v) == "active", v
        assert ss.wp_status(v) == ("active", True), v
    # decision 5: Coming Soon is not emitted; For Lease is out of scope
    for v in ("coming soon", "Coming-Soon", "for lease", "leased"):
        assert wp_status_value(v) == ss.WP_DROP == "DROP", v
    for v in ("hold", "paused", "withdrawn", "expired", "Not Available"):
        assert wp_status_value(v) == "inactive", v
    for v in ("For Sale", "active", "current", "new-listing", "reduced", "No Banner"):
        assert wp_status_value(v) == "active", v
        assert ss.wp_status(v) == ("active", False), v
    for v in ("publish", "restaurant", "", None, True):
        assert wp_status_value(v) is None, v


def test_class_list_status_terms():
    s = _scraper()
    cases = [
        (["status-sold"], "sold"),                         # bbofsa, sellmydeliveryroutes
        (["listing-type-sold"], "sold"),                   # trustbusinessbrokers (old regex missed)
        (["listing_category-soldout"], "sold"),            # businessexits (old regex missed)
        (["listing_status-past"], "sold"),                 # exitequity (old regex missed)
        (["listing-status-hold"], "inactive"),             # ubcutah
        (["status-paused"], "inactive"),                   # bbofsa
        (["property_status-under-contract"], "active"),    # bizbuyandsellga (decision 6)
        (["listing_status-coming-soon"], "DROP"),          # decision 5
        (["listing_status-current"], "active"),
        # sperityventures: for-sale on every post, sold on most -> sold wins
        (["status-businesses-for-sale", "status-sold"], "sold"),
    ]
    for cl, want in cases:
        st, src, text = s._resolve_status(
            _item("https://e.com/l/a/", "A Business", class_list=cl), "A Business", {})
        assert st == want, (cl, st)
        assert src == "class_list"
        # a plain for-sale/current term carries no label; every other status does
        assert (text is None) if cl == ["listing_status-current"] else (text in cl)


def test_under_contract_label_beats_plain_for_sale():
    s = _scraper()
    st, src, text = s._resolve_status(
        _item("https://e.com/l/a/", "A Business",
              class_list=["property_status-for-sale", "property_status-under-contract"]),
        "A Business", {})
    assert (st, text) == ("active", "property_status-under-contract")


def test_post_type_and_wp_status_classes_are_not_status():
    s = _scraper()
    it = _item("https://e.com/l/a/", "A Business",
               class_list=["post-1", "listing", "type-listing", "status-publish", "hentry"])
    assert s._resolve_status(it, "A Business", {}) == ("active", None, None)


def test_meta_availability_field():
    s = _scraper()
    rec = s._parse(_item("https://e.com/l/a/", "Bar & Grill",
                         meta={"business_availability": "Off Market", "business_price": "0"}), "1")
    assert rec["status"] == "active"                  # decision 6: off market -> active + label
    assert rec["status_source"] == "field:availability"
    assert rec["broker_status_text"] == "Off Market"
    # For Lease is still not a business for sale (unchanged behaviour)
    assert s._parse(_item("https://e.com/l/b/", "Space",
                          meta={"business_availability": "For Lease"}), "1") is None


# ── sold slug / title ───────────────────────────────────────────────────────
def test_sold_slug_detection():
    assert wp_slug_sold("sold-it-company-with-strong-cash-flow")
    assert wp_slug_sold("sold_specialty-retail")
    assert wp_slug_sold("hvac-company-sold")
    assert not wp_slug_sold("soldering-equipment-maker")
    assert not wp_slug_sold("custom-closet-design-2084")
    s = _scraper()
    rec = s._parse(_item("https://www.vikingmergers.com/listing/sold-it-company/", "IT Company",
                         slug="sold-it-company"), "563")
    assert rec["status"] == "sold" and rec["status_source"] == "slug"


def test_title_markers():
    s = _scraper()
    sold = s._parse(_item("https://e.com/l/a/", "Profitable Italian Restaurant - SOLD"), "1")
    assert sold["status"] == "sold" and sold["status_source"] == "title"
    uc = s._parse(_item("https://e.com/l/b/", "Diner: Under Contract"), "1")
    assert uc["status"] == "active" and uc["status_source"] == "title"
    assert uc["broker_status_text"] == "Under Contract"


def test_badges():
    assert wp_badge_status("Coming Soon!") == "DROP"
    assert wp_badge_status("For Lease") == "DROP"
    assert wp_badge_status("SOLD FOR") == "sold"
    assert wp_badge_status("SOLD!") == "sold"
    assert wp_badge_status("Under Offer") == "active"
    assert wp_badge_status("Sale Pending") == "active"
    assert wp_badge_status("Has Accepted Offer") == "active"
    assert wp_badge_status("Withdrawn") == "inactive"
    assert wp_badge_status("For Sale") is None          # no label to carry
    assert wp_badge_status("Patent pending technology with strong margins") is None


# ── price ───────────────────────────────────────────────────────────────────
def test_market_price_is_not_disclosed():
    f = wp_card_financials("Metal Distributor #2057 Price: Market Price Revenues: $64,939,613 "
                           "Cash Flow: $2,799,477 Location: Southeast U.S.")
    assert f["price"] is None and f["price_not_disclosed"] is True
    assert f["revenue"] == 64939613 and f["cash_flow"] == 2799477
    assert wp_price_hidden("Call for price") and wp_price_hidden("TBD")


def test_down_payment_is_not_a_price():
    f = wp_card_financials("Chiropractic Practice for Sale in Puyallup Washington High Profit "
                           "Practice - Now Possible to Buy With $100k Down! (Inquire For Details)")
    assert f["price"] is None and f["price_not_disclosed"] is True


def test_numeric_card_price():
    f = wp_card_financials("Specialty Trucking #2013 Price: $1,250,000 Revenues: $4.2M")
    assert f["price"] == 1250000 and f["price_not_disclosed"] is False
    assert f["revenue"] == 4200000


def test_money_first_amount_only():
    assert wp_money("$1,750,000 (Business) + $3,450,000 (Real Estate)") == 1750000
    assert wp_money("$4.99M") == 4990000
    assert wp_money("TBD") is None and wp_money("0") is None and wp_money(0) is None
    assert wp_money(450000) == 450000


def test_acf_market_price_flag():
    s = _scraper()
    rec = s._parse(_item("https://e.com/l/a/", "A Business", acf={"asking_price": "Market Price"}), "1")
    assert rec["price"] is None and rec["price_not_disclosed"] is True
    rec = s._parse(_item("https://e.com/l/b/", "B Business", acf={"asking_price": "$425,000"}), "1")
    assert rec["price"] == 425000 and rec["price_not_disclosed"] is False


# ── index intersection ──────────────────────────────────────────────────────
INDEX = """<html><body><h2>For Sale</h2><div class="grid">
<div class="item"><a class="card" href="https://e.com/l/live-one/"><span>Live One</span>
  <span>Price: $500,000</span></a></div>
<div class="item"><a class="card" href="https://e.com/l/soon-one/"><span>Soon One</span>
  <div class="coming-soon">Coming Soon!</div></a></div>
<div class="item"><a class="card" href="/contact/"><span>Contact us</span></a></div>
</div>
<h2>Sold Practices</h2><div class="grid">
<div class="item"><a class="card" href="/recently-sold/"><span>Sold Practice In Boise</span>
  <span>SOLD FOR</span><span>$330,000</span></a></div>
<div class="item"><a class="card" href="/recently-sold/"><span>Another Sold Place</span>
  <span>SOLD FOR</span></a></div>
<div class="item"><a class="card" href="/recently-sold/"><span>Third Sold Place</span>
  <span>SOLD FOR</span></a></div>
</div></body></html>"""


def _rest_records(s):
    items = [
        _item("https://e.com/l/live-one/", "Live One"),
        _item("https://e.com/l/soon-one/", "Soon One"),
        _item("https://e.com/l/boise/", "Sold Practice in Boise"),
        _item("https://e.com/l/old-2014/", "Old Practice", date="2014-03-01T00:00:00"),
    ]
    return [s._parse(it, "1") for it in items]


def test_intersection_keeps_only_index_links(monkeypatch):
    s = _scraper("https://e.com/for-sale/")
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://e.com/for-sale/", INDEX)])
    recs = _rest_records(s)
    ids_before = [(r["listing_id"], r["listing_url"]) for r in recs]
    s._apply_index(recs, verbose=False)
    by = {r["title"]: r for r in recs}
    assert by["Live One"]["status"] == "active"
    assert by["Live One"]["price"] == 500000 and by["Live One"]["price_not_disclosed"] is False
    assert by["Soon One"]["status"] == "DROP"             # removed by scrape(); see below
    assert by["Soon One"]["broker_status_text"] == "Coming Soon!"
    assert by["Sold Practice in Boise"]["status"] == "sold"
    assert by["Sold Practice in Boise"]["broker_status_text"] == "SOLD FOR"
    old = by["Old Practice"]
    assert old["status"] == "inactive" and old["status_source"] == "not_on_index"
    assert len(recs) == 4                                  # off-index: emitted, not dropped
    assert s.index_stats["on_index"] == 2 and s.index_stats["not_on_index"] == 1
    assert s.index_stats["badge_drop"] == 1
    # ids / urls unchanged (principle 4): listing_id is md5(url) as before
    assert [(r["listing_id"], r["listing_url"]) for r in recs] == ids_before
    for lid, url in ids_before:
        assert lid == hashlib.md5(url.encode()).hexdigest()


def test_index_failure_is_loud_and_changes_nothing(monkeypatch, capsys):
    s = _scraper("https://e.com/for-sale/")

    def boom(verbose=True):
        raise RuntimeError("index https://e.com/for-sale/ HTTP 503")
    monkeypatch.setattr(s, "_fetch_index_pages", boom)
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    assert [r["status"] for r in recs] == ["active"] * 4
    assert all(r["index_check"] == "failed" for r in recs)
    assert "INDEX CHECK FAILED" in capsys.readouterr().out


def test_index_with_no_matches_fails_loud(monkeypatch):
    s = _scraper("https://e.com/for-sale/")
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://e.com/for-sale/", "<html><body>JS app</body></html>")])
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    assert all(r["status"] == "active" and r["index_check"] == "failed" for r in recs)


def test_ajax_paginated_index_fails_loud(monkeypatch):
    s = _scraper("https://e.com/for-sale/")
    page = INDEX.replace('<div class="grid">', '<div class="grid" data-pages="6">', 1)
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://e.com/for-sale/", page)])
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    assert all(r["index_check"] == "failed" for r in recs)
    assert "data-pages=6" in s.index_stats["index_error"]


def test_status_handlers_hook():
    s = _scraper()
    calls = []

    def handler(item, title, fields):
        calls.append(title)
        return ("sold", "custom:tax", "Closed Deal") if "Closed" in title else None
    old = WPRestScraper.status_handlers
    WPRestScraper.status_handlers = [handler]
    try:
        rec = s._parse(_item("https://e.com/l/a/", "Closed Bakery"), "1")
        assert rec["status"] == "sold" and rec["status_source"] == "custom:tax"
        assert s._parse(_item("https://e.com/l/b/", "Open Bakery"), "1")["status"] == "active"
    finally:
        WPRestScraper.status_handlers = old
    assert calls == ["Closed Bakery", "Open Bakery"]


# ── 2026-11-11 later: truforte Featured-only, routeconsultant POST index ────
def test_asking_and_owner_benefit_labels():
    f = wp_card_financials("Sub and Ice Cream Shop Listing #95112108 Asking: $225,000 "
                           "Owner Benefit: $120,000 Lee County Learn More")
    assert f["price"] == 225000 and f["cash_flow"] == 120000
    # the listing number is never read as money
    assert f["price"] != 95112108


def test_listing_price_and_total_revenue_labels():
    f = wp_card_financials("Central Massachusetts FedEx P&D Routes 29 Routes "
                           "Total Revenue: $3,807,635 Listing Price: $1,850,000 SBA Eligible")
    assert f["price"] == 1850000 and f["revenue"] == 3807635


def test_off_index_drop_mode(monkeypatch):
    s = _scraper("https://e.com/for-sale/", off_index="drop")
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://e.com/for-sale/", INDEX)])
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    titles = {r["title"] for r in recs}
    assert "Old Practice" not in titles               # off-index post left out
    assert "Live One" in titles and len(recs) == 3
    assert s.index_stats["dropped_off_index"] == 1


def test_off_index_drop_mode_returns_nothing_when_index_fails(monkeypatch):
    s = _scraper("https://e.com/for-sale/", off_index="drop")

    def boom(verbose=True):
        raise RuntimeError("index HTTP 500")
    monkeypatch.setattr(s, "_fetch_index_pages", boom)
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    assert recs == []                                  # never the whole archive


def test_off_index_value_is_validated():
    with pytest.raises(ValueError):
        WPRestScraper("e.com", "listing", off_index="delete")


RC_GRID = """<div class="results">
<div class="card"><a href="https://rc.example/listings/rcoh1/">View More</a><span>Northern Ohio</span>
 <span>Bread</span><span>6 Accounts</span><span>Total Revenue: $288,028</span>
 <span>Listing Price: $50,000</span></div>
<div class="card"><a href="https://rc.example/listings/pdnc2/">View More</a><span>Central North Carolina</span>
 <span>FedEx P&amp;D Routes</span><span>14 Routes</span><span>Total Revenue: $1,684,539</span>
 <span>Listing Price: $950,000</span><span>Status: Under Contract</span></div>
<div class="card"><a href="https://rc.example/listings/other/">View More</a><span>Elsewhere</span></div>
</div>"""


def test_post_index_and_status_label_kept_active(monkeypatch):
    s = _scraper("POST https://rc.example/wp-json/x/v1/filter-routes")

    class R:
        status_code = 200

        def json(self):
            return {"result": True, "data": RC_GRID}
    posted = {}

    class Sess:
        def post(self, url, json=None, timeout=None, proxies=None):
            posted["url"], posted["json"] = url, json
            return R()
    s.session, s._proxies, s._use_proxy = Sess(), None, False
    pages = s._fetch_index_pages(False)
    assert posted == {"url": "https://rc.example/wp-json/x/v1/filter-routes", "json": {}}
    # REST links are /route/<code>/, the grid links /listings/<code>/: matched by slug
    recs = [s._parse(_item("https://rc.example/route/rcoh1/", "Northern Ohio", slug="rcoh1"), "1"),
            s._parse(_item("https://rc.example/route/pdnc2/", "Central North Carolina", slug="pdnc2"), "1"),
            s._parse(_item("https://rc.example/route/gone9/", "Central Georgia", slug="gone9"), "1")]
    monkeypatch.setattr(s, "_fetch_index_pages", lambda verbose=True: pages)
    s._apply_index(recs, verbose=False)
    oh, nc, gone = recs
    assert oh["status"] == "active" and oh["price"] == 50000 and oh["revenue"] == 288028
    assert nc["status"] == "active"                    # under contract stays active
    assert nc["broker_status_text"] == "Under Contract"
    assert nc["price"] == 950000
    assert gone["status"] == "inactive" and gone["status_source"] == "not_on_index"


def test_route_type_taxonomy_is_category():
    s = _scraper()
    rec = s._parse(_item("https://rc.example/route/rcoh1/", "Northern Ohio", slug="rcoh1",
                         class_list=["route", "type-route", "region-central",
                                     "route_type-bread-route", "price-lt_500k"]), "1")
    assert rec["business_type"] == "Bread Route"


# ── 2026-11-11 later still: route titles, synergy detail financials ────────
def test_route_title_builder():
    from specialized_scrapers import wp_route_title, _rc_card_title
    import re as _re
    grid = _re.compile(r"^(central|northern|southern|eastern|western)\s+[a-z]+$", _re.I)
    assert wp_route_title("Central Georgia", "Bread", "15 Accounts") == \
        "Central Georgia Bread Route, 15 Accounts"
    assert wp_route_title("New England", "FedEx P&D Routes", "15 Routes") == \
        "New England FedEx P&D Routes, 15 Routes"
    t = _rc_card_title({"title": "Central Georgia"},
                       ["Central Georgia", "Bread", "15 Accounts", "Total Revenue: $249,820",
                        "Listing Price: $75,000", "SBA Eligible", "View More ›"])
    assert t == "Central Georgia Bread Route, 15 Accounts" and not grid.match(t)
    assert _rc_card_title({"title": "Eastern Washington"},
                          ["Eastern Washington", "Bread", "Total Revenue: $838,552"]) == \
        "Eastern Washington Bread Route"


def test_route_titles_built_from_card_in_index_pass(monkeypatch):
    s = _scraper("POST https://rc.example/x")
    s.domain = "listings.routeconsultant.com"
    recs = [s._parse(_item("https://rc.example/route/rcoh1/", "Northern Ohio", slug="rcoh1"), "1"),
            s._parse(_item("https://rc.example/route/gone9/", "Central Georgia", slug="gone9",
                           class_list=["route_type-bread-route"]), "1")]
    ids = [r["listing_id"] for r in recs]
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://rc.example/x", RC_GRID)])
    s._apply_index(recs, verbose=False)
    assert recs[0]["title"] == "Northern Ohio Bread Route, 6 Accounts"
    assert recs[1]["title"] == "Central Georgia Bread Route"      # off index: taxonomy fallback
    assert [r["listing_id"] for r in recs] == ids                   # id never from the title


SYN_DETAIL = """<html><body><nav><a>Listings by Price</a></nav>
<section class="individual-detailed-info"><div class="sale-list-item"><div class="sale-list-item-content">
<div class="sale-list-item-price"><span>Price:</span> $102,000,000</div>
<h5><span>Annual Revenue: <strong>$74,200,000</strong></span>
<span>Net Cash Flow: <strong>$11,443,000</strong></span></h5></div></div></section>
<aside><div class="card">Luxury Transport Companies for sale <span>Asking Price: $5,900,000</span>
<span>Annual Revenue: $3,523,325</span><span>Net Cash Flow: $1,550,306</span></div></aside>
</body></html>"""


def test_detail_financials_reads_own_block_not_sidebar():
    f = WPRestScraper._detail_financials(SYN_DETAIL)
    assert f["price"] == 102000000 and f["revenue"] == 74200000 and f["cash_flow"] == 11443000
    assert WPRestScraper._detail_financials("<html><body>No numbers</body></html>")["price"] is None


def test_enrich_only_active_without_price(monkeypatch):
    s = _scraper()
    got = []

    class R:
        status_code = 200
        text = SYN_DETAIL

    class Sess:
        def get(self, url, timeout=None):
            got.append(url)
            return R()
    s.session = Sess()
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    recs = [s._parse(_item("https://e.com/l/a/", "Live Co"), "1"),
            s._parse(_item("https://e.com/l/b/", "Gone Co - SOLD"), "1"),
            s._parse(_item("https://e.com/l/c/", "Priced Co", acf={"asking_price": "$1,000,000"}), "1")]
    s._enrich_financials(recs, verbose=False)
    assert got == ["https://e.com/l/a/"]                       # sold and priced skipped
    assert recs[0]["price"] == 102000000 and recs[0]["cash_flow"] == 11443000


# ── price_text carried verbatim (W14 writers read it) ──────────────────────
def test_price_text_is_carried_verbatim():
    f = wp_card_financials("Metal Distributor #2057 Price: Market Price Revenues: $64,939,613")
    assert f["price_not_disclosed"] is True and "Market Price" in f["price_text"]
    f = wp_card_financials("Chiropractic Practice - Now Possible to Buy With $100k Down! (Inquire)")
    assert f["price_not_disclosed"] is True and f["price_text"] == "$100k Down"
    f = wp_card_financials("Specialty Trucking Price: $1,250,000")
    assert f["price_text"] is None and f["price_not_disclosed"] is False
    s = _scraper()
    rec = s._parse(_item("https://e.com/l/a/", "A Business", acf={"asking_price": "Market Price"}), "1")
    assert rec["price_text"] == "Market Price" and rec["price_not_disclosed"] is True


def test_bare_call_and_na_are_not_statements():
    assert ss.wp_price_text("N/A") is None
    assert ss.wp_price_text("call") is None
    assert ss.wp_price_text("$450,000 negotiable") is None     # a real total
    assert ss.wp_price_text("Call for price") == "Call for price"


def test_uses_shared_helper_when_present():
    import importlib.util
    shared = importlib.util.find_spec("price_disclosure") is not None
    assert (ss._pd_phrase is not None) == shared


# ── decision 5: Coming Soon is not emitted, and the drop is logged ─────────
def test_coming_soon_not_emitted_and_logged(monkeypatch, capsys):
    s = _scraper("https://e.com/for-sale/")
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://e.com/for-sale/", INDEX)])
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    s._drop_not_emitted(recs, verbose=False)
    assert "Soon One" not in {r["title"] for r in recs} and len(recs) == 3
    assert all(r["status"] != "DROP" for r in recs)
    assert s.index_stats["dropped_status"] == 1
    assert "status DROP: 1 rows not emitted (Coming Soon!: 1)" in capsys.readouterr().out


def test_coming_soon_class_term_dropped_without_index(capsys):
    s = _scraper()
    recs = [s._parse(_item("https://e.com/l/a/", "Soon Co", class_list=["listing_status-coming-soon"]), "1"),
            s._parse(_item("https://e.com/l/b/", "Live Co"), "1")]
    s._drop_not_emitted(recs, verbose=False)
    assert [r["title"] for r in recs] == ["Live Co"]


def test_under_contract_badge_keeps_active_with_label(monkeypatch):
    s = _scraper("https://e.com/for-sale/")
    page = INDEX.replace('<div class="coming-soon">Coming Soon!</div>',
                         '<div class="badge">Under Contract</div>')
    monkeypatch.setattr(s, "_fetch_index_pages",
                        lambda verbose=True: [("https://e.com/for-sale/", page)])
    recs = _rest_records(s)
    s._apply_index(recs, verbose=False)
    soon = next(r for r in recs if r["title"] == "Soon One")
    assert soon["status"] == "active" and soon["broker_status_text"] == "Under Contract"
    assert soon["status_source"] == "index_badge" and s.index_stats["badge_active"] == 1


def test_uses_shared_status_module_when_present():
    import importlib.util
    shared = importlib.util.find_spec("listing_status") is not None
    assert (ss._ls_lookup is not None) == shared
