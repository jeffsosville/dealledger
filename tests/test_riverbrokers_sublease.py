"""riverbrokers.com (2026-11-23): why V6 saw 72 of Jeff's 75, and the two fixes.

riverbrokers.com/commercial-listings serves 75 cards on one page (40 Business-only,
31 Business and Property, 4 Property-only). V6 got 72:
  * the 3 missing are "Zero Goodwill! ... Gas Station(s) for Sublease" cards, rejected
    as strong CRE ("for sublease") by ListingExtractor.from_card. The buyer takes over
    the premises lease; the business is what is for sale.
  * 10 of the 72 had the wrong price: the largest-$ rule read the title/blurb
    ("$104K Sales Last Month") instead of the card's own price box.
  * "Property-only" cards are out of scope (Jeff) and are skipped by the layout config.
Fixture: 8 of the 75 card <div>s, verbatim.
"""
import os
import sys

from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "riverbrokers_cards.html")
URL = "https://riverbrokers.com/commercial-listings"


def _extract():
    soup = BeautifulSoup(open(FIX, encoding="utf-8").read(), "html.parser")
    out = {}
    for el in soup.select("div.propery-style-6"):
        pid = el.find_parent(id=True)["id"]
        out[pid] = v6.ListingExtractor.from_card(el, URL, "river brokers")
    return out


def test_sublease_business_cards_are_listings_now():
    got = _extract()
    for pid in ("property-6059", "property-5976", "property-5974"):
        assert got[pid] is not None, pid
        assert "Sublease" in got[pid]["title"]


def test_prices_come_from_the_card_price_box():
    got = _extract()
    assert got["property-6129"]["asking_price"] == 20000      # not "$104K Sales" from the title
    assert got["property-6059"]["asking_price"] == 10000      # not "$60,000 per month" from the blurb
    assert got["property-5976"]["asking_price"] == 10000
    assert got["property-5974"]["asking_price"] == 20000
    assert got["property-6121"]["asking_price"] == 99000
    assert got["property-6037"]["asking_price"] == 1899000


def test_property_only_card_is_skipped_everything_else_kept():
    got = _extract()
    assert got["property-6127"] is None                        # "Property-only" land site
    for pid in ("property-6129", "property-6121", "property-6037", "property-6110",
                "property-6059", "property-5976", "property-5974"):
        assert got[pid] is not None, pid                       # incl. net-lease and shutdown (flagged, not dropped)


def test_gate_sublease_needs_business_evidence():
    # a bare space offering is still CRE; a business that happens to sublease is not
    assert v6.cre_gate_reason("Retail space for sublease, 1,800 SF. Asking Price: $2,500")
    assert v6.cre_gate_reason(
        "Zero Goodwill! Absentee-Owned Branded Gas Station for Sublease in Winfield, AL!") is None


def test_for_lease_is_still_strong_even_with_business_evidence():
    assert v6.cre_gate_reason("Liquor/grocery Store For Lease Gross Revenue: $700,000")
