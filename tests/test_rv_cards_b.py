"""rv-cards-b (2026-11-24): card readers in scrapers/rv_cards_b.py for
routeadvisors.com, bizbrokeragehub.com and number1businessbroker.com, plus the
two ID_KEY_RULES entries and the wp_rest_brokers.csv row for bizbrokeragehub.

Principle 8: each rejecting case is paired with input that must still pass.
Fixtures in tests/fixtures/rv are trimmed copies of the live pages (24 Nov 2026).
"""
import csv
import hashlib
import json
import os
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import rv_cards_b as rv  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "rv"
ROOT = Path(__file__).parent.parent


def fx(name):
    return (FIX / name).read_text(errors="replace")


class _R:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


class _Sess:
    def __init__(self, post=None, get=None):
        self._post, self._get, self.posts = post, get, []

    def post(self, url, data=None, headers=None, timeout=None):
        self.posts.append((url, dict(data or {}), dict(headers or {})))
        return self._post(data)

    def get(self, url, timeout=None):
        return self._get(url) if self._get else _R("", 404)


class _Fetcher:
    def __init__(self, pages=None, post=None, get=None):
        self.pages = pages or {}
        self.session = _Sess(post, get)
        self.fetched = []

    def fetch(self, url, use_proxy=False):
        self.fetched.append(url)
        if url in self.pages:
            return self.pages[url], "requests"
        raise RuntimeError("no page " + url)


def ctx():
    return SimpleNamespace(make_row=v6._make_api_listing,
                           state_from_location=v6._state_from_location,
                           needs_proxy=lambda d: False)


# --------------------------------------------------------------------------
# routeadvisors.com
# --------------------------------------------------------------------------
RA = "https://www.routeadvisors.com/available-routes"


def ra_rows():
    f = _Fetcher(pages={})
    # the pager's own links are followed; page 2/3 are served the same fixture
    # minus the pager so the follow-up pages end the walk.
    page = fx("ra_cards.html")
    bare = page.split("<nav>")[0] + "</body></html>"
    f.pages = {RA + "/2/": bare, RA + "/3/": bare}
    return rv.read(RA, page, "Route Advisors", f, ctx()), f


def by_route(rows):
    return {r.get("source_listing_id") or r["title"]: r for r in rows}


def test_ra_price_is_asking_price_not_gross_revenue():
    rows, _ = ra_rows()
    r = by_route(rows)["2572227"]
    assert r["asking_price"] == 2_900_000          # Asking Price
    assert r["revenue"] == 2_237_649               # Gross Revenue, its own field
    assert r["cash_flow"] == 419_309
    assert r["url"] == "https://www.routeadvisors.com/routes/15-fedex-pd-routes-7/"
    assert r["state"] == "NY"


def test_ra_tbd_is_null_and_never_a_number():
    r = by_route(ra_rows()[0])
    solo = r["2464740"]                            # Cash Flow: TBD
    assert solo["cash_flow"] is None and solo["asking_price"] == 449_000
    allbd = r["2390834"]                           # price, revenue, cash flow all TBD
    assert allbd["asking_price"] is None and allbd["revenue"] is None and allbd["cash_flow"] is None


def test_ra_sold_badge_is_broker_marked_sold_verbatim():
    r = by_route(ra_rows()[0])["2480055"]          # Route #2480055, 8 FedEx P&D, ID
    assert r["status"] == "sold" and r["broker_status_text"] == "Sold!"
    assert r["asking_price"] == 1_050_000


def test_ra_pending_stays_active_with_label_and_on_hold_is_inactive():
    r = by_route(ra_rows()[0])
    assert r["2358884"]["status"] == "active" and r["2358884"]["broker_status_text"] == "Sale pending!"
    assert r["2276231"]["status"] == "inactive" and r["2276231"]["broker_status_text"] == "On Hold"
    # plain cards (SBA!, New! ribbons) stay active with no label
    assert r["2572227"]["status"] == "active" and not r["2572227"].get("broker_status_text")


def test_ra_placeholder_card_is_not_a_listing_but_a_card_with_no_route_number_is():
    rows, _ = ra_rows()
    titles = [x["title"] for x in rows]
    assert not any(t.startswith("Tell us") for t in titles)
    lowes = [x for x in rows if x["title"].startswith("Lowe")]
    assert lowes and lowes[0]["asking_price"] == 125_000     # no Route #, real card


def test_ra_follows_the_pager_and_does_not_fetch_detail_pages():
    rows, f = ra_rows()
    assert f.fetched == [RA + "/2/", RA + "/3/"]
    assert not any("/routes/" in u for u in f.fetched)
    # the same card on the same url across pages is one row
    urls = [x["url"] for x in rows]
    assert len(urls) == len(set(urls))


def test_ra_money_parser():
    assert rv.money("Asking Price: $2,900,000") == 2_900_000
    for bad in ("TBD", "$", "$0", "", None, "20"):
        assert rv.money(bad) is None


def test_ra_ids_are_the_url_keyed_ids_already_stored():
    rows, _ = ra_rows()
    r = by_route(rows)["2572227"]
    assert r["id"] == hashlib.sha256(r["url"].encode()).hexdigest()[:16]
    assert r["id"] == "500b6cb2" + r["id"][8:]     # same id as the 2026-11-23 snapshot row


# --------------------------------------------------------------------------
# bizbrokeragehub.com
# --------------------------------------------------------------------------
BBH = "https://bizbrokeragehub.com/listings"


def bbh_rows():
    page = fx("bbh_cards.html")
    bare = page.replace("dr-card", "x-none")        # later pages: no cards -> walk stops
    f = _Fetcher(pages={f"https://bizbrokeragehub.com/listings/?dl_page={n}": bare for n in (1, 2, 3, 4)},
                 get=lambda url: _R("{}", 200))
    return rv.read(BBH, page, "Bizbrokerage Hub", f, ctx()), f


def bbh_by_title(rows):
    return {r["title"].split(" - ")[0].split(" – ")[0][:20]: r for r in rows}


def test_bbh_price_is_the_card_price_above_the_status_not_revenue():
    rows, _ = bbh_rows()
    d = [r for r in rows if r["title"].startswith("Established Longstanding Diner")][0]
    assert d["asking_price"] == 1_850_000          # we stored 3,729,576
    assert d["revenue"] == 3_729_576 and d["cash_flow"] == 760_968


def test_bbh_under_loi_and_under_contract_stay_active_with_label():
    rows, _ = bbh_rows()
    loi = [r for r in rows if r["title"].startswith("Award-Winning Coastal")][0]
    assert loi["status"] == "active" and loi["broker_status_text"] == "Under LOI"
    assert loi["asking_price"] == 899_000
    uc = [r for r in rows if r["title"].startswith("Laundromat")][0]
    assert uc["status"] == "active" and uc["broker_status_text"] == "Under Contract"
    plain = [r for r in rows if r["title"].startswith("New Exclusive")][0]
    assert plain["status"] == "active" and not plain.get("broker_status_text")


def test_bbh_zero_price_is_null_and_missing_meta_is_null():
    rows, _ = bbh_rows()
    z = [r for r in rows if r["title"].startswith("Residential & Commercial")][0]
    assert z["asking_price"] is None               # "$0" is not a price
    b = [r for r in rows if r["title"].startswith("Brand-New Bagel")][0]
    assert b["asking_price"] == 899_000 and b["revenue"] is None and b["cash_flow"] is None


def test_bbh_ref_number_and_clean_url_and_stable_id():
    rows, _ = bbh_rows()
    car = [r for r in rows if r["title"].startswith("New Exclusive")][0]
    assert car["source_listing_id"] == "2801"
    assert car["url"] == "https://bizbrokeragehub.com/deal-listing/new-exclusive-renovated-car-wash-in-queens-2801/"
    diner = [r for r in rows if r["title"].startswith("Established Longstanding Diner")][0]
    assert diner["source_listing_id"] == "79810"   # trailing ref after the long title
    card = {"title": "Plain Title With No Ref", "href": "https://bizbrokeragehub.com/deal-listing/plain/",
            "price": "$10,000", "status": "Active", "revenue": None, "profits": None, "highlights": ""}
    row, _ = rv.bbh_row(card, BBH, "x", ctx())
    assert "source_listing_id" not in row          # no ref: identity stays the slug
    # id key is the slug in the form the Sept ids were hashed from, so the
    # Kitchen & Bath held row (0680634dbaab921b) is the one a crawl updates
    key = v6.listing_id_key("https://bizbrokeragehub.com/deal-listing/kitchen-and-bath-22686/")
    assert hashlib.sha256(key.encode()).hexdigest()[:16] == "0680634dbaab921b"


def test_bbh_id_ignores_page_position_in_the_link():
    a = v6.stable_listing_id("t", "https://bizbrokeragehub.com/deal-listing/x/?back=..%3Fdl_page%3D1", "i", "bizbrokeragehub.com")
    b = v6.stable_listing_id("t", "https://bizbrokeragehub.com/deal-listing/x/?back=..%3Fdl_page%3D3", "i", "bizbrokeragehub.com")
    assert a == b


def test_bbh_total_and_pager():
    page = fx("bbh_cards.html")
    assert rv.bbh_total(page) == 39
    assert len(rv.bbh_page_urls(page, BBH)) == 4
    assert rv.bbh_total("<p>nothing</p>") is None


def test_bbh_state_from_the_listing_feed_when_the_card_has_none():
    js = json.dumps({"structured_data": json.dumps(
        {"name": "Kitchen & Bath Business for Sale | Suffolk County, NY"})})
    assert rv.bbh_json_state(js, ctx()) == "NY"
    assert rv.bbh_json_state("not json", ctx()) is None


def test_bbh_is_not_in_the_wp_rest_registry_any_more():
    with open(ROOT / "data" / "wp_rest_brokers.csv") as f:
        domains = {(r.get("domain") or "").replace("www.", "") for r in csv.DictReader(f)}
    assert "bizbrokeragehub.com" not in domains          # its REST type holds 1 post of 39
    assert len(domains) > 20                             # the rest of the registry is intact


# --------------------------------------------------------------------------
# number1businessbroker.com
# --------------------------------------------------------------------------
N1 = "https://number1businessbroker.com/search/"


def n1_rows(pages=None):
    frag = fx("n1_fragment.html")
    empty = "<div id='bbcrm-listings-searchresults'>No search results.</div>"
    pages = pages or [frag, empty]

    def post(data):
        n = int(data["search"].split("pagenumber=")[1].split("&")[0])
        html = pages[n - 1] if n <= len(pages) else empty
        return _R(json.dumps({"code": 200, "body": {"html": html}}))
    f = _Fetcher(post=post)
    return rv.read(N1, "<html/>", "Number 1 Business Broker", f, ctx()), f


def test_n1_motel_is_emitted_with_its_labelled_figures():
    rows, _ = n1_rows()
    m = [r for r in rows if r["title"].startswith("High Profit Motel")][0]
    assert m["asking_price"] == 3_900_000          # Asking Price block
    assert m["revenue"] == 751_521                 # Gross Revenue block
    assert m["cash_flow"] == 434_290               # Net Cashflow block (not EBITDA, not 'first $')
    assert m["source_listing_id"] == "320" and m["state"] == "NJ"


def test_n1_description_dollar_amounts_are_never_the_price():
    rows, _ = n1_rows()
    m = [r for r in rows if r["title"].startswith("High Profit Motel")][0]
    # the blurb says "over $400,000" and "over $750,000"; the label blocks win
    assert m["asking_price"] not in (400_000, 750_000)


def test_n1_in_contract_is_active_with_label_and_prefix_stripped_from_title():
    rows, _ = n1_rows()
    p = [r for r in rows if r["title"].startswith("NYC Sheet Metal")][0]
    assert p["status"] == "active" and p["broker_status_text"] == "IN CONTRACT - SALE PENDING"
    assert not p["title"].startswith("IN CONTRACT")
    assert p["asking_price"] == 4_900_000          # was stored as revenue 5,750,000


def test_n1_recently_sold_is_sold_and_plain_card_is_active_with_no_label():
    rows, _ = n1_rows()
    s = [r for r in rows if r["title"].startswith("Premier Famous Airboat")][0]
    assert s["status"] == "sold" and s["broker_status_text"] == "RECENTLY SOLD!"
    assert not s["title"].startswith("RECENTLY")
    e = [r for r in rows if r["title"].startswith("Electrical Contractor Business")][0]
    assert e["status"] == "active" and not e.get("broker_status_text")


def test_n1_sba_badge_is_not_a_status_and_is_not_in_the_title():
    rows, _ = n1_rows()
    s = [r for r in rows if "Engineering and Staffing" in r["title"]][0]
    assert s["status"] == "active" and not s.get("broker_status_text")
    assert not s["title"].startswith("SBA")
    assert s["asking_price"] == 3_200_000


def test_n1_identity_is_the_leading_listing_number_not_the_title():
    a = v6.stable_listing_id("t", "https://number1businessbroker.com/listing/?listing=315-Westchester's Crown Jewel",
                             N1, "number1businessbroker.com")
    b = v6.stable_listing_id("t", "https://number1businessbroker.com/listing/?listing=315-Turnkey Custom Cabinet",
                             N1, "number1businessbroker.com")
    c = v6.stable_listing_id("t", "https://number1businessbroker.com/listing/?listing=316-Other",
                             N1, "number1businessbroker.com")
    assert a == b and a != c
    assert a == hashlib.sha256(b"https://number1businessbroker.com/listing/?listing=315").hexdigest()[:16]
    # the stored url stays the link the site publishes (title included)
    rows, _ = n1_rows()
    assert any(r["url"].startswith("https://number1businessbroker.com/listing/?listing=320-High Profit") for r in rows)


def test_n1_pages_until_the_crm_returns_no_results_and_posts_the_sites_own_request():
    frag = fx("n1_fragment.html")
    rows, f = n1_rows([frag, frag.replace("listing=320", "listing=999").replace("listing=317", "listing=998")
                       .replace("listing=312", "listing=997")])
    pages = [p[1]["search"] for p in f.session.posts]
    assert pages[0].startswith("pagenumber=1&resultsperpage=10")
    assert any(p.startswith("pagenumber=2") for p in pages)
    ids = [r["source_listing_id"] for r in rows]
    assert len(ids) == len(set(ids))
    assert f.session.posts[0][2]["Referer"].endswith("/search/")


def test_n1_api_failure_falls_through_not_silent_empty():
    def boom(data):
        return _R("", 500)
    f = _Fetcher(post=boom)
    assert rv.read(N1, "<html/>", "x", f, ctx()) is None


def test_n1_labels_map_by_label_only():
    html = ("<div class='main'><a href='/listing/?listing=7-X'><div class='mob name'><p></p>X Co</div></a>"
            "<div class='mob'>Gross Revenue<p>$900,000</p></div>"
            "<div class='mob'>Asking Price<p>$1,000,000</p></div></div>")
    c = rv.n1_cards(html)[0]
    assert c["blocks"] == {"revenue": "$900,000", "price": "$1,000,000"}   # order never matters


# --------------------------------------------------------------------------
# scope: the reader touches only its three domains
# --------------------------------------------------------------------------
def test_other_domains_are_untouched():
    f = _Fetcher()
    assert rv.read("https://example-broker.com/listings", "<html/>", "x", f, ctx()) is None
    assert rv.read("https://plus.nebba.org/x", fx("ra_cards.html"), "x", f, ctx()) is None
    assert f.fetched == [] and f.session.posts == []
