"""rv-dealrelations (2026-11-24): hosted DealRelations reader.

Principle 8: each rejecting/forcing case is paired with input that must still
pass. Fixtures in tests/fixtures/rv are trimmed copies of the live pages
(24 Nov 2026): Brio's /listing_feeds/5092 pages 1-2 and the preschool hosted
/listings page 1.
"""
import hashlib
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import cr_w3_platforms as w3  # noqa: E402
import rv_dealrelations as rv  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "rv"


def fx(name):
    return (FIX / name).read_text(errors="replace")


def _install_helpers():
    w3._H.update({"make": v6._make_api_listing, "status": v6._ls_lookup,
                  "needs_proxy": v6._needs_proxy, "user_agents": v6.USER_AGENTS,
                  "stable_id": v6.stable_listing_id})


def by_title(rows, frag):
    return next(r for r in rows if frag in r["title"])


def test_cards_read_price_revenue_ebitda_and_reference():
    cards, pager = rv.parse_page(fx("dr_brio_feed_p1.html"))
    assert len(cards) == 2 and pager["has_next"] and pager["pages"] == 2
    g = next(c for c in cards if "Gourmet" in c["title"])
    assert g["labels"]["asking price"] == "$1,319,900"      # not the revenue
    assert g["labels"]["revenue"] == "$5,311,300"
    assert g["labels"]["adjusted ebitda"] == "$291,300"
    assert g["reference"] == "B26002"


def test_rows_price_is_asking_not_revenue_and_undisclosed_is_null():
    _install_helpers()
    cards, _ = rv.parse_page(fx("dr_brio_feed_p1.html"))
    rows = rv.rows_from_cards(cards, "https://briobrokers.dealrelations.com/listings",
                              "Brio Brokers Group", "briobrokers.dealrelations.com")
    g = by_title(rows, "Gourmet")
    assert (g["asking_price"], g["revenue"], g["cash_flow"]) == (1319900.0, 5311300.0, 291300.0)
    a = by_title(rows, "Auto Repair")                        # Revenue: Undisclosed
    assert a["asking_price"] == 1079900.0 and a["revenue"] is None and a["cash_flow"] == 305000.0
    assert g["source_listing_id"] == "B26002" and a["source_listing_id"] == "B25008"


def test_page2_rows_named_by_jeff_are_corrected():
    _install_helpers()
    cards, _ = rv.parse_page(fx("dr_brio_feed_p2.html"))
    rows = rv.rows_from_cards(cards, "https://briobrokers.dealrelations.com/listings",
                              "Brio Brokers Group", "briobrokers.dealrelations.com")
    ls = by_title(rows, "Landscape")
    assert (ls["asking_price"], ls["revenue"]) == (339900.0, 355000.0)   # was 355,000 / 355,000
    k12 = by_title(rows, "K-12")
    assert (k12["asking_price"], k12["revenue"], k12["cash_flow"]) == (249000.0, 354700.0, 57900.0)


def test_title_has_no_broker_prefix_and_strip_helper_is_safe():
    _install_helpers()
    cards, _ = rv.parse_page(fx("dr_brio_feed_p1.html"))
    rows = rv.rows_from_cards(cards, "u", "Brio Brokers Group", "briobrokers.dealrelations.com")
    assert all("Brio Brokers Group" not in r["title"] for r in rows)
    assert rv.clean_title("Brio Brokers Group | Dental Lab", "Brio Brokers Group") == "Dental Lab"
    assert rv.clean_title(" | Profitable Preschool", "Preschool Business Solutions") == "Profitable Preschool"
    # must still pass: a pipe INSIDE a real title, and another broker's name, are kept
    t = "High-Volume Childcare Center | $2M+ Revenue | 250-Child Capacity"
    assert rv.clean_title(t, "Brio Brokers Group") == t


def test_brio_row_ids_equal_the_stored_feed_identity():
    """Stored ids are sha256 of the feed url (item_id/from/fragment); the reader must keep them."""
    _install_helpers()
    cards, _ = rv.parse_page(fx("dr_brio_feed_p1.html"))
    rows = rv.rows_from_cards(cards, "https://briobrokers.dealrelations.com/listings",
                              "Brio Brokers Group", "briobrokers.dealrelations.com")
    a = by_title(rows, "Auto Repair")
    assert a["id"] == "7366da077ac8fc61"                     # snapshot id of the stored row
    assert a["url"].endswith("?item_id=5092&from=listing_feeds#top_registration_form")
    # a domain with no public feed id keeps the plain hosted url
    pc, _ = rv.parse_page(fx("dr_preschool_p1.html"))
    prow = rv.rows_from_cards(pc, "https://preschoolbusinesssolutions.dealrelations.com/listings",
                              "Preschool Business Solutions", "preschoolbusinesssolutions.dealrelations.com")
    assert all("?" not in r["url"] for r in prow)


def test_status_labels_win_and_nnn_is_flagged_not_dropped():
    _install_helpers()
    w3.SCOPE_LOG.clear()
    pc, _ = rv.parse_page(fx("dr_preschool_p1.html"))
    rows = rv.rows_from_cards(pc, "https://preschoolbusinesssolutions.dealrelations.com/listings",
                              "Preschool Business Solutions", "preschoolbusinesssolutions.dealrelations.com")
    mont = by_title(rows, "Montessori")                      # Under Contract stays active, with label
    assert mont["status"] == "active" and mont["broker_status_text"] == "Under Contract"
    tampa = by_title(rows, "Tampa Bay")
    assert tampa["state"] == "FL" and tampa["revenue"] == 1900000.0 and tampa["cash_flow"] is None
    assert any("NNN" in r["title"] for r in rows)            # flagged elsewhere (held), never dropped here


def _pages(feed_base, challenge_from=None):
    pages = {1: fx("dr_brio_feed_p1.html"), 2: fx("dr_brio_feed_p2.html")}

    def get(url):
        n = int(url.rsplit("page=", 1)[1])
        if challenge_from and n >= challenge_from:
            return 403, "<title>Just a moment...</title>"
        return (200, pages[n]) if n in pages else (404, "")
    return get


def test_walk_follows_next_until_last_page_and_is_complete():
    cards, info = rv.walk(fx("dr_brio_feed_p1.html"), "u", "https://x/listing_feeds/5092",
                          get=_pages("x"), sleep=0)
    assert len(cards) == 4 and info["pages_seen"] == 2 and info["complete"] and info["pager_pages"] == 2


def test_walk_blocked_page_two_is_incomplete_never_complete():
    cards, info = rv.walk(fx("dr_brio_feed_p1.html"), "u", "https://x/listings",
                          get=_pages("x", challenge_from=2), sleep=0)
    assert len(cards) == 2 and not info["complete"] and "blocked" in info["stop"]


def test_handler_blocked_crawl_states_an_upper_bound_so_it_closes_partial():
    """preschool-style: no feed id, page 2 challenged. The stated count must exceed what
    was seen by more than the 10% tolerance (displayed_count.count_verdict), else the
    crawl closes 'ok' and the absence rule retires pages 2-3."""
    import displayed_count as dc
    _install_helpers()
    orig = rv._plain_get
    rv._plain_get = lambda url: (403, "<title>Just a moment...</title>")
    w3.DISPLAYED.clear()
    try:
        host = "https://preschoolbusinesssolutions.dealrelations.com/listings"
        # fixture page 1 plus a 3-page pager (the live page shows "1 2 3 Next")
        html = fx("dr_preschool_p1.html").replace("2</a>", "2</a><a href='/listings?page=3'>3</a>", 1) \
            if "page=3" not in fx("dr_preschool_p1.html") else fx("dr_preschool_p1.html")
        rows = rv.handler_rv_dealrelations(host, html, "Preschool Business Solutions", fetcher=None)
    finally:
        rv._plain_get = orig
    d = w3.DISPLAYED["preschoolbusinesssolutions.dealrelations.com"]
    assert len(rows) == 3 and d["count"] > len(rows)
    assert dc.count_verdict(len(rows), d["count"]) is False
    assert "upper bound" in d["text"]


def test_handler_complete_crawl_states_exact_count():
    import displayed_count as dc
    _install_helpers()
    orig_get, orig_ids = rv._plain_get, dict(rv.FEED_IDS)
    pages = {"listing_feeds/5092": fx("dr_brio_feed_p1.html"), "page=2": fx("dr_brio_feed_p2.html")}

    def get(url):
        if "page=2" in url:
            return 200, pages["page=2"]
        return 200, pages["listing_feeds/5092"]
    rv._plain_get = get
    w3.DISPLAYED.clear()
    try:
        page1 = fx("dr_brio_feed_p1.html")
        rows = rv.handler_rv_dealrelations("https://briobrokers.dealrelations.com/listings",
                                           page1, "Brio Brokers Group", fetcher=None)
    finally:
        rv._plain_get = orig_get
    d = w3.DISPLAYED["briobrokers.dealrelations.com"]
    assert len(rows) == 4 and d["count"] == 4 and dc.count_verdict(4, d["count"]) is True


def test_handler_ignores_other_domains_and_other_templates():
    _install_helpers()
    cards = fx("dr_brio_feed_p1.html")
    assert rv.handler_rv_dealrelations("https://sunbeltofflorida.com/x", cards, "b", None) is None
    assert rv.handler_rv_dealrelations("https://brgbrokers.dealrelations.com/listings",
                                       "<table class='listing-table'></table>", "b", None) is None
    assert rv.handler_rv_dealrelations("https://briobrokers.dealrelations.com/listings",
                                       "<html>no cards listings-financials1</html>", "b", None) is None


def test_challenge_detector_passes_normal_pages():
    assert rv.is_challenge(403, "") and rv.is_challenge(200, "<title>Just a moment...</title>")
    assert not rv.is_challenge(200, fx("dr_brio_feed_p1.html"))


def test_v6_registers_the_handler_once():
    sigs = [s for s, h in v6.API_HANDLERS if h is rv.handler_rv_dealrelations]
    assert sigs == ["listings-financials1"]
