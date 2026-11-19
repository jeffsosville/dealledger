"""laundromat-broker.com parser (scrapers/laundromat_broker_scraper.py).

Fixture: the 154 listing headings on https://laundromat-broker.com/listings.html
as of 2026-11-19 (tests/fixtures/laundromat_broker_headings.txt), wrapped in the
page's Weebly markup.
"""
import html
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from laundromat_broker_scraper import parse_heading, parse_page  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "laundromat_broker_headings.txt")


def _page():
    heads = open(FIX).read().splitlines()
    body = "".join(
        f'<h2 class="wsite-content-title"><u>{html.escape(h)}</u></h2><div></div>'
        f'<div class="paragraph">Body for {i}. Positive Cash Flow of $70,000.</div>'
        for i, h in enumerate(heads))
    return ('<div class="wsite-section-elements"><div class="paragraph">All Listings Below '
            'Have Sold Or Are Currently Active</div>' + body + '</div>')


def _rows():
    return {r["listing_url"].split("#lb-")[1]: r for r in parse_page(_page())}


def test_every_heading_is_a_row_with_a_stable_url():
    rows = _rows()
    assert len(rows) == 154
    assert all(r["listing_url"].startswith("https://laundromat-broker.com/listings.html#lb-")
               for r in rows.values())


def test_status_counts():
    c = Counter(r["status"] for r in _rows().values())
    assert c == {"sold": 112, "active": 22, "inactive": 20}


def test_sold_keeps_the_year_verbatim():
    r = _rows()["1"]
    assert (r["status"], r["broker_status_text"], r["price"], r["city"], r["state"]) == \
        ("sold", "SOLD 2021", 55000.0, "Woodbridge", "CT")
    assert r["title"] == "Laundromat - Woodbridge, CT - GREAT DEAL"
    assert _rows()["29"]["status"] == "sold"            # Sold by 3rd party 2023


def test_under_deposit_and_off_market_stay_listed_with_label():
    r = _rows()
    assert (r["137"]["status"], r["137"]["broker_status_text"]) == ("active", "Under Deposit 2026")
    assert (r["130"]["status"], r["130"]["broker_status_text"]) == ("active", "OFF Market")
    assert (r["22"]["status"], r["22"]["broker_status_text"]) == ("active", "Has Accepted Offer 2024")


def test_inactive_phrases():
    r = _rows()
    for n, label in [("48", "ON HOLD"), ("19", "Temporarily Unavailable"), ("23", "Temporarily Off Market"),
                     ("123", "Inactive"), ("144", "Listing Cancelled"), ("107", "No Longer Available"),
                     ("112", "Not Available")]:
        assert (r[n]["status"], r[n]["broker_status_text"]) == ("inactive", label), n


def test_no_suffix_is_active():
    r = _rows()["146"]
    assert (r["status"], r.get("broker_status_text"), r["price"], r["state"]) == \
        ("active", None, 140000.0, "PA")


def test_price_forms():
    r = _rows()
    assert r["37"]["price"] == 110000.0      # "$110,00" typo
    assert r["95"]["price"] == 235000.0      # "-235,000" with no $
    assert r["94"]["price"] == 325000.0      # "$325k", not the "150K Net Profit" in the title
    assert r["112"]["price"] == 500000.0     # last amount, not "$400k Equipment"
    assert r["148"]["price"] is None


def test_locations():
    r = _rows()
    assert (r["5"]["city"], r["5"]["state"]) == ("Seymour", "CT")
    assert (r["99"]["city"], r["99"]["state"]) == ("Jenks", "OK")
    assert (r["143"]["city"], r["143"]["state"]) == ("Sussex", "DE")
    assert (r["98"]["state"]) == "CT"        # "Danbury" with no state
    assert (r["90"]["city"], r["90"]["state"]) == ("Brooklyn", "NY")
    assert r["133"]["state"] is None         # "Near Greenville": NC or SC, left blank


def test_types_and_cash_flow():
    r = _rows()
    assert r["1"]["business_type"] == "Laundromat"
    assert r["6"]["business_type"] == "Dry Cleaner"
    assert r["8"]["business_type"] == "Restaurant"
    assert r["111"]["business_type"] == "Gas Station"
    assert r["1"]["cash_flow"] == 70000.0


def test_non_listing_heading_ignored():
    assert parse_heading("Contact John today") is None
