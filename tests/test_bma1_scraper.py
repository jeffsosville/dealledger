"""bma1.com parser (scrapers/bma1_scraper.py).

Fixture: the three `article.slw_business_listing` cards served by
https://www.bma1.com/business-listing/ on 2026-11-23 (markup verbatim).
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from bma1_scraper import parse_index, parse_table, state_from  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "bma1_index.html")


def _rows():
    return parse_index(open(FIX, encoding="utf-8").read())


def _by():
    return {r["title"]: r for r in _rows()}


def test_three_cards_three_rows_distinct_detail_urls():
    rows = _rows()
    assert len(rows) == 3
    urls = [r["listing_url"] for r in rows]
    assert len(set(urls)) == 3
    assert all("/business-listing/" in u and not u.endswith("/business-listing/") for u in urls)


def test_title_is_the_heading_not_the_teaser():
    assert set(_by()) == {"Landscaping & Hardscaping Provider", "Premier Interior Design Firm",
                          "Electrical Contractor"}


def test_no_asking_price_and_revenue_is_not_price():
    for r in _rows():
        assert r["price"] is None
        assert r["cash_flow"] is None
    by = _by()
    assert by["Premier Interior Design Firm"]["revenue"] == 8_406_000      # FY25, $ thousands
    assert by["Electrical Contractor"]["revenue"] == 6_200_000             # only FY25 given
    assert by["Landscaping & Hardscaping Provider"]["revenue"] == 2_622_000


def test_status_labels():
    by = _by()
    assert by["Landscaping & Hardscaping Provider"]["status"] == "active"
    for t in ("Premier Interior Design Firm", "Electrical Contractor"):
        assert by[t]["status"] == "active"
        assert by[t]["broker_status_text"].lower() == "sale pending"


def test_state_only_when_a_state_is_named():
    by = _by()
    assert by["Landscaping & Hardscaping Provider"]["state"] == "PA"
    assert by["Premier Interior Design Firm"]["state"] is None            # "Mid-Atlantic"
    assert state_from("This company, based in the Mid Atlantic, is a firm.") is None


def test_ebitda_kept_verbatim_in_description_not_cash_flow():
    assert "Adjusted EBITDA FY25 $995K" in _by()["Electrical Contractor"]["description"]


def test_card_without_detail_link_gives_no_row():
    assert parse_index('<article class="slw_business_listing"><h2 class="entry-title">x</h2></article>') == []


def test_parse_table_ignores_projection_column():
    from bs4 import BeautifulSoup
    c = BeautifulSoup('<div><table><tr><th>$ Thousands</th><th>FY25</th><th>FY26 Proj</th></tr>'
                      '<tr><td>Revenue</td><td>$1,000</td><td>$9,000</td></tr></table></div>', "html.parser")
    assert parse_table(c)["revenue"] == 1_000_000
