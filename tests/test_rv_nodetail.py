"""rv-nodetail (2026-11-24): per-domain readers in scrapers/rv_nodetail.py for brokers
whose listings have no detail page (nerestaurantbrokers.com, newbizbrokers.com,
marigoldresources.com) and their hook in V6.

Principle 8: each filter/reader is paired with input that must still pass.
Fixtures in tests/fixtures/rv are trimmed copies of the live pages (24 Nov 2026).
"""
import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import rv_nodetail as rv  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "rv"
ROOT = "https://www.nerestaurantbrokers.com"


def fx(name):
    return (FIX / name).read_text(errors="replace")


def nerb_pages():
    return [(ROOT + p, fx(f"nerb_{p[1:]}.html")) for p in rv.NERB_PAGES]


def make(base_url, broker_name, **kw):
    return v6._make_api_listing(base_url, broker_name, **kw)


# ---------------------------------------------------------------- money
def test_money_forms():
    assert rv.money("$395K for the business, plus fair market value for the real estate") == 395000
    assert rv.money("$$495,000 or partnership available") == 495000
    assert rv.money("$ 45,000. TAKE OVER THE LEASE") == 45000
    assert rv.money("$675.000.00") == 675000
    assert rv.money("$3.80MM") == 3_800_000
    assert rv.money("1.63MM, $239K EBITDA") == 1_630_000
    assert rv.money("$1,250,000") == 1_250_000
    assert rv.money("N/A") is None


# ------------------------------------------------- nerestaurantbrokers.com
def test_nerb_raw_cards_and_distinct():
    pages = nerb_pages()
    raw = [len(rv.parse_nerb_page(h, u)) for u, h in pages]
    assert raw == [6, 12, 3]                           # the broker's own counts
    assert len(rv.merge_nerb(pages)) == 18             # 21 cards, 3 shown twice


def test_nerb_dedupe_across_pages():
    cards = rv.merge_nerb(nerb_pages())
    codes = [c["code"] for c in cards]
    assert codes.count("DS2299") == 1 and codes.count("DS1232") == 1
    assert "DS2212" not in codes                       # same pub as DS4456
    pub = next(c for c in cards if c["code"] == "DS4456")
    assert pub["aliases"] == ["DS2212"]
    assert sum(1 for c in cards if "NORTH CONWAY" in c["title"]) == 1
    assert sum(1 for c in cards if c["code"] is None) == 4     # Weymouth, Natick, Northboro, Newton
    assert len({rv._norm_title(c["title"]) for c in cards}) == 18


def test_nerb_distinct_listings_are_not_merged():
    """DS2299 and DS1232 are both sports/restaurant cards; different titles stay two rows."""
    cards = rv.merge_nerb(nerb_pages())
    assert {"DS2299", "DS1232", "JR9977", "DS3333"} <= {c["code"] for c in cards}


def test_nerb_sheet_urls_never_the_nda():
    cards = rv.merge_nerb(nerb_pages())
    for c in cards:
        assert c["sheet"] is None or "/_files/ugd/" in c["sheet"]
        assert "Buyer%20NDA" not in (c["sheet"] or "")
    by = {c["code"] or c["title"][:8]: c for c in cards}
    assert by["DS4456"]["sheet"].endswith(".pdf")
    assert by["DS2299"]["sheet"]                       # from the full-service page; the liquor card has none
    assert by["DS3333"]["sheet"] is None
    assert next(c for c in cards if c["title"].startswith("NATICK"))["sheet"] is None
    assert next(c for c in cards if c["title"].startswith("NEWTON"))["sheet"] is None


def test_nerb_rows_fragment_fallback_and_identity():
    base = ROOT + "/full-service-restaurants"
    rows = rv.nerb_rows(rv.merge_nerb(nerb_pages()), base, "nerestaurantbrokers.com", make=make,
                        status_lookup=v6._ls_lookup)
    assert len(rows) == 18 and len({r["id"] for r in rows}) == 18 and len({r["url"] for r in rows}) == 18
    by = {r["source_listing_id"]: r for r in rows if r["source_listing_id"]}
    assert by["DS3333"]["url"] == ROOT + "/fast-food-restaurants#DS3333"
    assert by["DS3333"]["url_is_listing_specific"] is False
    assert "/_files/ugd/" in by["DS4456"]["url"] and by["DS4456"]["url_is_listing_specific"] is True
    natick = next(r for r in rows if r["title"].startswith("NATICK"))
    assert natick["url"].startswith(ROOT + "/fast-food-restaurants#natick-framingham")
    # identity is the code: a new sheet upload (new URL) must not change the id
    again = rv.nerb_rows([dict(c, sheet=c["sheet"] and c["sheet"] + "?v=2") for c in rv.merge_nerb(nerb_pages())],
                         base, "nerestaurantbrokers.com", make=make)
    assert {r["id"] for r in again} == {r["id"] for r in rows}


def test_nerb_prices():
    rows = rv.nerb_rows(rv.merge_nerb(nerb_pages()), ROOT + "/x", "n", make=make)
    p = {r["source_listing_id"] or r["title"][:6]: r["asking_price"] for r in rows}
    assert p["DS4456"] == 395000        # business price, real estate extra
    assert p["DS5561"] == 495000        # "or partnership available"
    assert p["NEWTON"] == 45000         # "(plus lease takeover)"
    assert p["WEYMOU"] == 675000        # "$675.000.00"
    assert p["JR7755"] == 1_000_000


def test_nerb_only_stated_annual_figures():
    rows = rv.nerb_rows(rv.merge_nerb(nerb_pages()), ROOT + "/x", "n", make=make)
    by = {r["source_listing_id"]: r for r in rows if r["source_listing_id"]}
    assert by["DS4456"]["revenue"] == 1_500_000        # "$1.5M/ yr."
    assert by["DS1232"]["revenue"] is None             # "$25K wk." is not annualised
    assert by["DB5588"]["cash_flow"] == 250_000        # "VERIFIABLE ANNUAL NET INCOME"


def test_nerb_under_agreement_is_active_with_label_and_history_text_is_not_a_status():
    rows = rv.nerb_rows(rv.merge_nerb(nerb_pages()), ROOT + "/x", "n", make=make, status_lookup=v6._ls_lookup)
    by = {r["source_listing_id"]: r for r in rows if r["source_listing_id"]}
    assert by["JR9977"]["status"] == "active" and by["JR9977"]["broker_status_text"] == "UNDER AGREEMENT"
    others = [r for r in rows if r["source_listing_id"] != "JR9977"]
    assert all(r["status"] == "active" and not r.get("broker_status_text") for r in others)
    # Natick says "Recently sold, the seller must now sell" and Northboro "Recently sold in July":
    # history inside a description, not a status
    assert next(r for r in rows if r["title"].startswith("NATICK"))["status"] == "active"


def test_nerb_sold_card_is_recorded_sold():
    html = ("<html><body>National Restaurant Exchange<div id='a'><div>TYPE OF BUSINESS: X PIZZA SELLING PRICE: $100K "
            "LISTING ID: DS1111 Click for Listing Sheet</div><div>SOLD</div></div></body></html>")
    cards = rv.parse_nerb_page(html, ROOT + "/liquor-businesses")
    rows = rv.nerb_rows(cards, ROOT + "/liquor-businesses", "n", make=make, status_lookup=v6._ls_lookup)
    assert rows[0]["status"] == "sold"


def test_nerb_title_cleanup():
    cards = {c["code"]: c for c in rv.merge_nerb(nerb_pages()) if c["code"]}
    assert "LISTING ID" not in cards["DS1232"]["title"]
    assert cards["GV2579"]["title"].startswith("SHARON AREA")
    assert "~" not in cards["DS4456"]["title"]


class _Resp:
    def __init__(self, text, url=""):
        self.status_code, self.text, self.url = 200, text, url


class _Sess:
    def __init__(self, routes):
        self.routes = routes

    def get(self, url, timeout=None, allow_redirects=True):
        r = self.routes.get(url)
        if r is None:
            return SimpleNamespace(status_code=404, text="", url=url)
        return r if not isinstance(r, str) else _Resp(r, url)


def test_nerb_handler_end_to_end_and_other_domains_untouched():
    routes = {ROOT + p: fx(f"nerb_{p[1:]}.html") for p in rv.NERB_PAGES}
    fetcher = SimpleNamespace(session=_Sess(routes), fetch=lambda u, use_proxy=False: ("", "x"))
    rv.install([], {"make": make, "status": v6._ls_lookup})
    rows = rv.handler_nerb(ROOT + "/full-service-restaurants", routes[ROOT + "/full-service-restaurants"],
                           "n", fetcher)
    assert len(rows) == 18
    # an unrelated broker on a Wix page with the same words is not captured
    assert rv.handler_nerb("https://other-broker.com/x", routes[ROOT + "/liquor-businesses"], "n", fetcher) is None


def test_nerb_missing_category_page_is_logged_not_fatal():
    routes = {ROOT + "/full-service-restaurants": fx("nerb_full-service-restaurants.html")}
    fetcher = SimpleNamespace(session=_Sess(routes), fetch=lambda u, use_proxy=False: ("", "x"))
    rv.install([], {"make": make, "status": v6._ls_lookup})
    rows = rv.handler_nerb(ROOT + "/full-service-restaurants", routes[ROOT + "/full-service-restaurants"],
                           "n", fetcher)
    assert len(rows) == 6
    assert any("fast-food" in w for w, _ in rv.SCOPE_LOG.get(rv.NERB, []))


# ------------------------------------------------------ newbizbrokers.com
def test_newbiz_22_cards_and_fields():
    cards = rv.parse_newbiz(fx("newbiz_catalog.html"))
    assert len(cards) == 22
    assert len({rv._norm_title(c["title"]) for c in cards}) == 22
    glass = cards[0]
    assert (glass["title"], glass["price"], glass["cash_flow"]) == (
        "Fabulous Glass Business in Myrtle Beach", 1_250_000, 400_000)
    wicked = next(c for c in cards if c["title"].startswith("Ocean Front Wicked Tuna"))
    assert wicked["price"] == 1_250_000 and wicked["cash_flow"] is None      # "Cash Flow: N/A"
    # multi-span titles are joined, not cut after the first span
    assert any(c["title"].startswith("Premiere Resort Maintenance") for c in cards)
    assert any(c["title"].startswith("Rare Find - 40+ years") and "Mercedes and Bosch" in c["title"] for c in cards)
    assert not any("Widget" in c["title"] for c in cards)


def test_newbiz_location_from_text():
    cards = {c["title"][:12]: c for c in rv.parse_newbiz(fx("newbiz_catalog.html"))}
    assert (cards["Great moving"]["city"], cards["Great moving"]["state"]) == ("Lynchburg", "VA")
    assert cards["Propane dist"]["state"] == "SC" and cards["Propane dist"]["city"] == "Murrells Inlet"
    assert cards["Incredible W"]["city"] == "Georgetown"
    assert cards["Rare Find - "]["city"] == "Summerville"
    assert cards["Incredible L"]["state"] is None          # says nothing: left empty, not guessed
    assert rv.nb_location("Great business in Charleston, South Carolina") == ("Charleston", "SC")


def test_newbiz_rows_url_fragment_identity():
    base = "https://www.newbizbrokers.com/business-catalog"
    rows = rv.newbiz_rows(rv.parse_newbiz(fx("newbiz_catalog.html")), base, "n", make=make)
    assert len(rows) == 22 and len({r["id"] for r in rows}) == 22
    assert rows[0]["url"] == base + "#fabulous-glass-business-in-myrtle-beach"
    assert all(r["url"] != base and r["url_is_listing_specific"] is False for r in rows)
    # a price change does not change identity
    changed = [dict(c, price=(c["price"] or 0) + 1) for c in rv.parse_newbiz(fx("newbiz_catalog.html"))]
    assert [r["id"] for r in rv.newbiz_rows(changed, base, "n", make=make)] == [r["id"] for r in rows]


def test_newbiz_duplicate_title_is_suffixed_not_collapsed():
    c = {"title": "Same Title", "price": 1, "cash_flow": None, "description": "", "city": None, "state": None}
    rows = rv.newbiz_rows([c, dict(c)], "https://www.newbizbrokers.com/business-catalog", "n", make=make)
    assert len({r["id"] for r in rows}) == 2


# --------------------------------------------------- marigoldresources.com
MG = "https://www.marigoldresources.com/for-sale/"


def test_marigold_index_follows_image_links_and_ignores_the_rest():
    links, unlinked = rv.parse_marigold_index(fx("marigold_for_sale.html"), MG)
    hrefs = [h for h, _ in links]
    assert len(hrefs) == 11                                   # 8 basecamp + 3 short domains
    assert sum("basecamp.com" in h for h in hrefs) == 8
    assert {"http://www.marigold.mobi/skipark", "http://www.marigold.mobi/clinic",
            "http://www.marigold.com/therapy"} <= set(hrefs)
    assert not any("bizbuysell" in h or "ourdayindavenport" in h or "prescore" in h
                   or "value-builder" in h or "mailto" in h for h in hrefs)
    assert dict(links)["https://public.3.basecamp.com/p/5vMw62FYdr8HBF775A7i1i6o"] == "In Closing"
    assert len(unlinked) == 3                                 # kennel, livery (labelled) + tanning salon


def test_marigold_basecamp_fields():
    p = rv.parse_basecamp(fx("basecamp_1.html"))
    assert p["title"] == "Legacy Service & Sales Agri-Business in Central Iowa"
    assert (p["price"], p["revenue"], p["cash_flow"], p["state"]) == (3_800_000, 5_000_000, 875_000, "IA")
    ski = rv.parse_basecamp(fx("basecamp_9.html"))
    assert ski["price"] == 2_500_000                           # the NEW price, not the stale $3.5MM line
    auto = rv.parse_basecamp(fx("basecamp_6.html"))
    assert (auto["price"], auto["revenue"], auto["cash_flow"]) == (750_000, 2_285_000, 59_000)  # latest year
    fire = rv.parse_basecamp(fx("basecamp_4.html"))
    assert (fire["price"], fire["revenue"], fire["cash_flow"]) == (700_000, 1_630_000, None)    # EBITDA is not SDE
    cav = rv.parse_basecamp(fx("basecamp_8.html"))
    assert (cav["price"], cav["revenue"]) == (200_000, 435_000)


def test_marigold_handler_rows_status_and_dead_short_links():
    pages = {
        "https://public.3.basecamp.com/p/xWsKzArQ6cAx4GpTqjLQ4mXD": fx("basecamp_1.html"),
        "https://public.3.basecamp.com/p/5vMw62FYdr8HBF775A7i1i6o": fx("basecamp_8.html"),
        "http://www.marigold.mobi/skipark": _Resp(fx("basecamp_9.html"),
                                                  "https://public.app.basecamp.com/p/FDQrEdmadMVzvKPrePhSJ9qg"),
        # a parked page: 200 but no Basecamp URL at the end -> no row
        "http://www.marigold.mobi/clinic": _Resp("<html><body>still being worked on</body></html>",
                                                 "http://www.marigold.mobi/clinic"),
    }
    fetcher = SimpleNamespace(session=_Sess(pages))
    rv.install([], {"make": make, "status": v6._ls_lookup})
    rows = rv.handler_marigold(MG, fx("marigold_for_sale.html"), "marigoldresources.com", fetcher)
    assert len(rows) == 3
    by = {r["url"]: r for r in rows}
    assert "https://public.app.basecamp.com/p/FDQrEdmadMVzvKPrePhSJ9qg" in by      # final URL after redirect
    cav = by["https://public.3.basecamp.com/p/5vMw62FYdr8HBF775A7i1i6o"]
    assert cav["status"] == "active" and cav["broker_status_text"] == "In Closing"
    assert all(r["broker_domain"].endswith("marigoldresources.com") for r in rows)
    assert not any("clinic" in r["url"] for r in rows)
    assert any("clinic" in w for w, _ in rv.SCOPE_LOG[rv.MARIGOLD])
    assert rv.handler_marigold("https://other.com/for-sale/", fx("marigold_for_sale.html"), "x", fetcher) is None


# ---------------------------------------------------------------- V6 hook
def test_v6_registers_the_handlers_once():
    sigs = [fn.__name__ for _s, fn in v6.API_HANDLERS]
    for name in ("handler_nerb", "handler_newbiz", "handler_marigold"):
        assert sigs.count(name) == 1


def test_v6_extract_via_api_routes_by_domain():
    routes = {ROOT + p: fx(f"nerb_{p[1:]}.html") for p in rv.NERB_PAGES}
    fetcher = SimpleNamespace(session=_Sess(routes), fetch=lambda u, use_proxy=False: ("", "x"))
    got = v6.extract_via_api(ROOT + "/liquor-businesses", routes[ROOT + "/liquor-businesses"], "n", fetcher)
    assert got and len(got) == 18
    nb = v6.extract_via_api("https://www.newbizbrokers.com/business-catalog", fx("newbiz_catalog.html"), "n", fetcher)
    assert nb and len(nb) == 22


def test_v6_fragment_urls_are_identity_not_index_rows():
    """A '#<slug>' URL differs from the index URL, so V6 keys the row on it (id = sha256 of that URL)
    and the index-URL guard keeps it."""
    base = "https://www.newbizbrokers.com/business-catalog"
    a = make(base, "n", title="A", detail_url=base + "#a")
    b = make(base, "n", title="B", detail_url=base + "#b")
    assert a["id"] != b["id"] and a["url"] == base + "#a"
    import cr_w3_platforms as w3
    kept, dropped = w3.drop_index_url_rows([a, b, make(base, "n", title="C")], base)
    assert dropped == 1 and len(kept) == 2


# ------------------------------------------------ tbxllc.com Under Contract label (rv-tbx-label)
def _tbx_cards():
    from bs4 import BeautifulSoup
    return BeautifulSoup(fx("tbxllc_cards.html"), "html.parser").find_all("div", class_="listingBox")


def test_tbx_under_contract_button_gives_active_with_label_verbatim():
    cards = _tbx_cards()
    uc = [c for c in cards if c.find(class_="contractButton")]
    assert len(uc) == 2 and len(cards) == 4
    for c in uc:
        title = c.find(class_="listingTitle").get_text(" ", strip=True)
        for dom in ("tbxllc.com", "www.tbxllc.com"):
            assert v6.card_status(c, title, dom) == ("active", "UNDER CONTRACT")


def test_tbx_plain_cards_stay_active_without_label():
    for c in _tbx_cards():
        if not c.find(class_="contractButton"):
            title = c.find(class_="listingTitle").get_text(" ", strip=True)
            assert v6.card_status(c, title, "tbxllc.com") == ("active", None)


def test_contract_button_class_is_ignored_for_every_other_domain():
    """The extra selector is opt-in per domain: same card on another broker reads as before."""
    c = next(c for c in _tbx_cards() if c.find(class_="contractButton"))
    title = c.find(class_="listingTitle").get_text(" ", strip=True)
    assert v6.card_status(c, title, "otherbroker.com") == ("active", None)
    assert v6.badge_status(c, "otherbroker.com") is None


def test_generic_badge_reading_unchanged_for_tbx_and_others():
    from bs4 import BeautifulSoup
    sold = BeautifulSoup('<div class="card"><span class="ribbon">SOLD</span><h3>X</h3></div>', "html.parser").div
    for dom in ("tbxllc.com", "otherbroker.com"):
        assert v6.card_status(sold, "X", dom)[0] == "sold"
