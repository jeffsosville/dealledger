"""accountingpracticesales.com parser (scrapers/aps_scraper.py).

Fixture tests/fixtures/aps_cards.html: 9 cards copied verbatim from the live
/united-states/any/N/ pages of 23 Nov 2026 - New, Available, Sale Pending,
"Available after 4/15", On Hold, a virtual practice with a state, three SOLD
(one with an empty Location, one with a sub-region Location "North Florida"),
plus a "Next" link. Expected: 6 active, 3 sold, nothing dropped.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import aps_scraper as A  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "aps_cards.html")


def _html():
    return open(FIX, encoding="utf-8").read()


def _rows():
    cards, _ = A.parse_cards(_html())
    return A.to_rows(cards)


def test_parse_page():
    cards, has_next = A.parse_cards(_html())
    assert len(cards) == 9 and has_next
    first = cards[0]
    assert first["listing_no"] == "OH6682" and first["status"] == "New"
    assert first["title"] == "Northeast Ohio - East of Cleveland"
    assert first["price"] == "$435,000" and first["revenue"] == "$334,000"
    assert first["url"].endswith("/ohio/21455-Northeast+Ohio+-+East+of+Cleveland/")
    assert A.parse_found_count(_html()) == 3778


def test_counts_and_status():
    rows, st = _rows()
    assert (st["cards"], st["active"], st["sold"], st["non_us"], st["unknown_status"]) == (9, 6, 3, 0, 0)
    by = {r["broker_listing_no"]: r for r in rows}
    assert by["OH6682"]["status"] == "active" and "broker_status_text" not in by["OH6682"]
    assert by["AL6568"]["status"] == "active" and "broker_status_text" not in by["AL6568"]
    for no, label in (("OK6643", "Sale Pending"), ("FL6641", "On Hold"),
                      ("CAV6640", "Available after 4/15")):
        assert by[no]["status"] == "active" and by[no]["broker_status_text"] == label
    for no in ("TXN6606", "FL5571", "MI5395"):
        assert by[no]["status"] == "sold" and by[no]["broker_status_text"] == "SOLD"


def test_fields_and_money():
    rows, _ = _rows()
    r = {x["broker_listing_no"]: x for x in rows}["OH6682"]
    assert (r["price"], r["revenue"], r["state"], r["business_type"]) == (435000.0, 334000.0, "OH", "Accounting Practice")
    assert "OH6682" in r["description"]
    assert A._money("325000") == 325000.0 and A._money("") is None and A._money("Call") is None


def test_state_from_subregion_and_from_url_when_location_empty():
    rows, _ = _rows()
    by = {r["broker_listing_no"]: r for r in rows}
    assert by["FL5571"]["state"] == "FL"          # Location "North Florida"
    assert by["MI5395"]["state"] == "MI"          # Location empty -> URL /michigan/
    assert by["TXN6606"]["state"] == "TX"         # Location "Addison" -> URL /texas-north/
    assert A.us_state("Gulf Coast, FL") == "FL" and A.us_state("West Virginia") == "WV"
    assert A.us_state("Virginia") == "VA" and A.us_state("Toronto") is None


def test_virtual_with_state_is_kept():
    rows, st = _rows()
    assert {"TNV6651", "CAV6640"} <= {r["broker_listing_no"] for r in rows}
    assert st["virtual_kept"] >= 2 and st["virtual_dropped"] == 0


def _card(no, loc, url, title="Test Practice", status="Available"):
    return {"url": url, "status": status, "title": title, "listing_no": no,
            "location": loc, "revenue": "$1", "price": "$2", "type": "CPA"}


def test_canada_and_unresolved_virtual_are_dropped():
    us = "https://accountingpracticesales.com/united-states/west/oregon/1-x/"
    ca = "https://accountingpracticesales.com/canada/ontario/2-x/"
    cards = [
        _card("OR1", "Oregon", us),
        _card("ON2", "Ontario", ca),                                         # Canadian path
        _card("OR3", "Ontario", us.replace("/1-", "/3-")),                   # Canadian location
        _card("V5499", "", "https://accountingpracticesales.com/united-states/virtual/5-v/"),
        _card("V5500", "Idaho", "https://accountingpracticesales.com/united-states/virtual/6-v/"),
    ]
    rows, st = A.to_rows(cards)
    assert sorted(r["broker_listing_no"] for r in rows) == ["OR1", "V5500"]
    assert st["non_us"] == 3 and st["virtual_dropped"] == 1 and st["virtual_kept"] == 1


def test_same_url_twice_dedupes_but_relisted_number_is_kept():
    u = "https://accountingpracticesales.com/united-states/west/oregon/1-x/"
    u2 = "https://accountingpracticesales.com/united-states/west/oregon/2-x/"
    rows, st = A.to_rows([_card("OR1", "Oregon", u), _card("OR1", "Oregon", u),
                          _card("OR1", "Oregon", u2, status="SOLD")])
    assert len(rows) == 2 and st["duplicate"] == 1 and st["dup_listing_no"] == 1


def test_unknown_status_stays_active_and_is_counted():
    rows, st = A.to_rows([_card("OR1", "Oregon", "https://accountingpracticesales.com/united-states/west/oregon/1-x/",
                                status="Under Offer")])
    assert rows[0]["status"] == "active" and rows[0]["broker_status_text"] == "Under Offer"
    assert st["unknown_status"] == 1


def test_unparseable_card_raises():
    bad = '<a class="apslistingitem_n" href="https://x/united-states/a/b/1-x/"><div class="apslistingitem_lname">T</div></a>'
    with pytest.raises(A.ApsCrawlError):
        A.parse_cards(bad)


def test_listing_key_is_internal_id_not_slug():
    import run_specialized as R
    a = R.listing_key("https://accountingpracticesales.com/united-states/midwest-us/ohio/21455-Old+Slug/", "accountingpracticesales.com")
    b = R.listing_key("https://accountingpracticesales.com/united-states/west/oregon/21455-New+Slug/", "accountingpracticesales.com")
    c = R.listing_key("https://accountingpracticesales.com/united-states/west/oregon/21456-New+Slug/", "accountingpracticesales.com")
    assert a == b == "aps:21455" and c == "aps:21456"
    assert R.listing_key("https://example.com/x/", "example.com") == "https://example.com/x/"   # others untouched


def _scraper(pages):
    s = A.ApsScraper(delay=0)
    s.fetch_page = lambda n: pages[n] if isinstance(pages[n], str) else (_ for _ in ()).throw(pages[n])
    return s


def test_crawl_walks_until_no_next_and_fails_loud_on_midway_error():
    page = _html().replace("3778", "27")          # header total matches 3 pages x 9
    last = page.replace("Next", "Prev")           # no Next -> last page
    assert len(_scraper([page, page, last]).crawl()) == 27
    with pytest.raises(A.ApsCrawlError):           # page 1 errors: no short list as success
        _scraper([page, A.ApsCrawlError("HTTP 500"), last]).crawl()
    with pytest.raises(A.ApsCrawlError):           # Next promised but the page is empty
        _scraper([page, "<html></html>", last]).crawl()
    with pytest.raises(A.ApsCrawlError):           # nothing on page 0
        _scraper(["<html></html>"]).crawl()


def test_search_form_matches_the_documented_post():
    f = A.search_form("active")
    assert f["aps_country"] == "United-States" and f["aps_region"] == "any"
    assert f["aps_listingsfilter"] == "active" and f["aps_askingprice_max"] == "0"
    assert sum(k.startswith("aps_listingtype_") for k in f) == 10


def test_partial_crawl_fails_loudly():
    """Header says 3778 but the crawl ends after one 9-card page: must raise."""
    s = A.ApsScraper(delay=0)
    html_last = _html().replace("Next", "")           # no Next link -> looks like the end
    s.fetch_page = lambda n: html_last
    with pytest.raises(A.ApsCrawlError):
        s.crawl()


def test_complete_crawl_passes_when_header_matches():
    s = A.ApsScraper(delay=0)
    html_last = _html().replace("Next", "").replace("3778", "9")
    s.fetch_page = lambda n: html_last
    assert len(s.crawl()) == 9


def test_small_drift_during_the_crawl_still_passes():
    """A listing added mid-crawl must not fail a complete 3,778-card walk."""
    s = A.ApsScraper(delay=0)
    html_last = _html().replace("Next", "").replace("3778", "11")   # 9 cards, header 11
    s.fetch_page = lambda n: html_last
    assert len(s.crawl()) == 9
