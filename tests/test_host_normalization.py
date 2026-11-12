"""Host normalization + calhoun card extraction (W8, 2026-11-11).

calhouncompanies.com is reachable as bare and www; bare 301s to www and every
detail page carries <link rel=canonical href="https://www.calhouncompanies.com/...">.
stable_listing_id() keys on the URL, so both spellings must collapse to one
before the id is computed. Every other domain must be left exactly as it was
(principle 8: test the input that must pass unchanged, not just the bad case).
"""
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402

WWW = "https://www.calhouncompanies.com/business/fast-casual-franchise-restaurant-12025-cl"
BARE = "https://calhouncompanies.com/business/fast-casual-franchise-restaurant-12025-cl"

CARD = """
<div class="view-content">
<a about="/business/fast-casual-franchise-restaurant-12025-cl" class="node business teaser"
   href="/business/fast-casual-franchise-restaurant-12025-cl" rel="bookmark">
 <div class="business-info group">
  <h2 class="title">Fast Casual Franchise Restaurant - 12025 CL</h2>
  <div class="business-details group">
   <div class="field industry label-inline"><div class="field-label">Industry</div><div class="field-content">Food &amp; Beverage</div></div>
   <div class="field location label-inline"><div class="field-label">Location</div><div class="field-content">Minnesota</div></div>
   <div class="field business-short-desc label-inline"><div class="field-label">Category</div><div class="field-content">Specialty Restaurant</div></div>
  </div>
  <div class="value-details group">
   <div class="field price label-inline"><div class="field-label">Price</div><div class="field-content">$450,000</div></div>
   <div class="field gross-sales label-inline"><div class="field-label">Gross Sales</div><div class="field-content">$926,280</div></div>
   <div class="field cash-flow label-inline"><div class="field-label">Cash Flow</div><div class="field-content">$115,871</div></div>
  </div>
 </div>
</a>
<a class="node business teaser" href="/business/twin-cities-245d-hcbs-business-sale-12021sj">
 <div class="business-info group">
  <h2 class="title">Twin Cities 245D HCBS Business for Sale - 12021SJ</h2>
  <div class="business-details group">
   <div class="field industry label-inline"><div class="field-label">Industry</div><div class="field-content">Health Care</div></div>
   <div class="field location label-inline"><div class="field-label">Location</div><div class="field-content">Minnesota</div></div>
  </div>
  <div class="value-details group">
   <div class="field gross-sales label-inline"><div class="field-label">Gross Sales</div><div class="field-content">$929,497</div></div>
   <div class="field cash-flow label-inline"><div class="field-label">Cash Flow</div><div class="field-content">$408,273</div></div>
  </div>
 </div>
</a>
</div>
"""


def _cards(page_url):
    soup = BeautifulSoup(CARD, "html.parser")
    return [v6.ListingExtractor.from_card(el, page_url, "calhoun companies")
            for el in soup.select("div.business-info.group")]


def test_bare_and_www_collapse_to_www():
    assert v6.canonical_url(BARE) == WWW
    assert v6.canonical_url(WWW) == WWW
    assert v6.canonical_host("calhouncompanies.com") == "www.calhouncompanies.com"
    assert v6.canonical_host("www.calhouncompanies.com") == "www.calhouncompanies.com"


def test_same_id_from_either_host():
    a = v6.stable_listing_id("t", v6.canonical_url(BARE),
                             "https://calhouncompanies.com/find-a-business", "calhouncompanies.com")
    b = v6.stable_listing_id("t", v6.canonical_url(WWW),
                             "https://www.calhouncompanies.com/find-a-business", "www.calhouncompanies.com")
    assert a == b


def test_other_domains_unchanged():
    for u in ("https://www.tworld.com/listing/123",
              "https://tworld.com/listing/123",
              "https://restaurantrealty.com/listing/?listing=9",
              "https://www.example.com/a?b=1#c",
              "/relative/path", "", None):
        assert v6.canonical_url(u) == u
    for d in ("tworld.com", "www.tworld.com", "Example.COM", ""):
        assert v6.canonical_host(d) == d


def test_card_from_bare_page_gets_www_detail_url_and_labelled_price():
    for page in ("https://calhouncompanies.com/find-a-business?page=3",
                 "https://www.calhouncompanies.com/find-a-business?page=3"):
        first, no_price = _cards(page)
        assert first["url"] == WWW
        assert first["url_is_listing_specific"] is True
        assert first["asking_price"] == 450000      # Price, not Gross Sales
        assert first["revenue"] == 926280
        assert first["cash_flow"] == 115871
        # No Price field on the card: undisclosed, not the gross sales figure.
        assert no_price["asking_price"] is None
        assert no_price["revenue"] == 929497
        assert no_price["url"].startswith("https://www.calhouncompanies.com/business/")


def test_ids_identical_from_both_hosts():
    a = [c["id"] for c in _cards("https://calhouncompanies.com/find-a-business")]
    b = [c["id"] for c in _cards("https://www.calhouncompanies.com/find-a-business?page=7")]
    assert a == b and len(set(a)) == 2


def test_labelled_price_skips_qualified_labels():
    # Detail page: "Equipment Price" is not the ask.
    t = "Cash Flow $7,500,000 Equipment Price $1,000,000 Gross Sales $10,400,000"
    assert v6.labelled_asking_price(t, skip_qualified=True) is None
    # Card: the word before "Price" is the Category value, and the figure IS the ask.
    card = ("Category Power Equipment Price $1,500,000 Gross Sales $1,600,000 "
            "Cash Flow $283,000")
    assert v6.labelled_asking_price(card) == 1500000
    assert v6.labelled_asking_price("Asking Price: $199,000 Gross Sales $532,340") == 199000
    assert v6.labelled_asking_price("Price Reduced! Gross Sales $900,000") is None


def test_labelled_price_rule_is_opt_in():
    # Same card shape on a domain not in LABELLED_PRICE_DOMAINS keeps the old
    # largest-figure behaviour (no silent price change on other brokers).
    html = CARD.replace("/business/", "/listing/")
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select("div.business-info.group")[0]
    c = v6.ListingExtractor.from_card(el, "https://www.otherbroker.com/listings", "x")
    if c is not None:
        assert c["asking_price"] == 926280
