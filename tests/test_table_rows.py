"""Listing tables (job 3, table rows, 2026-11-12).

- a table row's Status COLUMN is read through the shared status module
  (kendallcapitalgroup.com, accountingfirmsold.com) - table_status_cell()
- a header row (<th> cells only) is never a listing
- PatternDetector Strategy 1c: the rows of a listing <table> are a candidate
  (keatepartners.com was NO_PATTERN), but not a fee schedule, a layout table,
  a footer table or a table under a "sold" heading (principle 8)
- another broker's cached BARE tag is never predicted (benchmarkbusinessgroup)
- an image link is not preferred over a detail page link
- marlingroup.com's ?pageno= is not part of a listing URL (principle 4)
"""
import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import (ListingExtractor, PatternDetector,  # noqa: E402
                                   _best_detail_link, drop_volatile_params,
                                   is_listing_element, predicted_beats_detected,
                                   table_status_cell)

try:
    import listing_status  # noqa: F401
    HAVE_STATUS = True
except Exception:
    HAVE_STATUS = False
needs_status = pytest.mark.skipif(not HAVE_STATUS, reason="listing_status module not present")


def _rows(html, sel="tr"):
    return BeautifulSoup(html, "html.parser").select(sel)


# ── kendallcapitalgroup.com: <th> header row with sort links, status first ──
KENDALL = "https://kendallcapitalgroup.com/pages/our-listings"
_K_ROWS = [("Sold", "Commercial Paving Company", "$5,950,000", "commercial-paving"),
           ("Active", "Buyer Engagement Manufacturing or Distribution", "$5,000,000", "buyer-engagement"),
           ("Under LOI", "Manufacturing Company KC", "$1,600,000", "manufacturing-kc"),
           ("Sold", "Sign and Service Company", "$1,550,000", "sign-service")]
KENDALL_HTML = (
    '<html><body><table class="table table-striped listing-table"><thead><tr>'
    '<th><a href="/pages/our-listings?direction=asc&sort=view_status">Status</a></th>'
    '<th><a href="/pages/our-listings?direction=asc&sort=link_title">Title</a></th>'
    '<th><a href="/pages/our-listings?direction=asc&sort=price">Price</a></th></tr></thead><tbody>'
    + "".join(f'<tr class="element_row{1000 + i}"><td>{st}</td>'
              f'<td><a href="/listings/{slug}?cac=show">{t}</a></td><td>{p}</td></tr>'
              for i, (st, t, p, slug) in enumerate(_K_ROWS))
    + '</tbody></table></body></html>')


@needs_status
def test_status_column_from_th_header():
    rows = _rows(KENDALL_HTML)[1:]
    got = [table_status_cell(r, "kendallcapitalgroup.com") for r in rows]
    assert got == [("sold", "Sold"), ("active", None), ("active", "Under LOI"), ("sold", "Sold")]


@needs_status
def test_status_column_sets_card_status():
    cards = [ListingExtractor.from_card(r, KENDALL, "Kendall") for r in _rows(KENDALL_HTML)]
    cards = [c for c in cards if c]
    assert len(cards) == 4                                   # header row not emitted
    assert [c["status"] for c in cards] == ["sold", "active", "active", "sold"]
    assert [c.get("broker_status_text") for c in cards] == ["Sold", None, "Under LOI", "Sold"]


def test_header_row_is_not_a_listing():
    header = _rows(KENDALL_HTML)[0]
    assert header.find("a")                                  # it links (column sorts) ...
    assert not is_listing_element(header, KENDALL)           # ... and is still not a listing


# ── accountingfirmsold.com: data-th cells, value + button in the cell ──────
AFS = "https://accountingfirmsold.com/listings"


def _afs_row(ref, price, status):
    return (f'<tr><td data-th="Listing Number">{ref}</td><td data-th="Location">Naples, FL</td>'
            f'<td data-th="Asking Price">{price}</td>'
            f'<td data-th="Description"><p>Tax and accounting practice, {ref}.</p></td>'
            f'<td data-th="Status">{status}<br/><a class="info-button" '
            f'href="mailto:x@accountingfirmsold.com?subject={ref}">Request Info</a></td></tr>')


AFS_HTML = ('<table class="rwd-table"><tr><th>Listing Number</th><th>Location</th>'
            '<th>Asking Price</th><th>Description</th><th>Status</th></tr>'
            + _afs_row("FL0914", "$335,000", "Available") + _afs_row("FL0915", "$200,000", "Sold")
            + _afs_row("FL0916", "$150,000", "Sale Pending") + _afs_row("FL0917", "$90,000", "On Hold")
            + '</table>')


@needs_status
def test_status_column_from_data_attribute():
    got = [table_status_cell(r, "accountingfirmsold.com") for r in _rows(AFS_HTML)[1:]]
    assert got == [("active", None), ("sold", "Sold"), ("active", "Sale Pending"),
                   ("inactive", "On Hold")]


@needs_status
def test_prose_cell_is_never_a_status():
    # "sold" in a description column, no Status column: nothing is read
    html = ('<table class="t"><tr><th>Business</th><th>Notes</th><th>Price</th></tr>'
            '<tr><td>Pizza Shop</td><td>Sold</td><td>$100,000</td></tr></table>')
    assert table_status_cell(_rows(html)[1], "x.com") is None
    # a status column whose value is not a known phrase: no evidence
    html2 = ('<table class="t"><tr><th>Name</th><th>Status</th><th>Price</th></tr>'
             '<tr><td>Pizza Shop</td><td>Owner retiring</td><td>$100,000</td></tr></table>')
    assert table_status_cell(_rows(html2)[1], "x.com") is None


@needs_status
def test_status_cell_only_for_table_rows():
    div = BeautifulSoup('<div class="card"><div>Sold</div></div>', "html.parser").div
    assert table_status_cell(div, "x.com") is None


@needs_status
def test_misaligned_columns_are_not_guessed():
    html = ('<table class="t"><tr><th>Status</th><th>Name</th><th>Price</th></tr>'
            '<tr><td colspan="2">Sold Pizza Shop</td><td>$100,000</td></tr></table>')
    assert table_status_cell(_rows(html)[1], "x.com") is None


# ── keatepartners.com: TablePress, live table + "Recent Businesses Sold" ───
KEATE = "https://keatepartners.com/business-listings"
_LIVE = [("S-2762", "Dry Cleaning Company", "$299,000", "Active", "s-2762"),
         ("S-2733", "Greater Dayton Area Auto Repair", "$359,000", "Active", "s-2733"),
         ("S-2755", "Cincinnati Hardscape Company", "$349,000", "Active", "s-2755"),
         ("R-2763", "Greater Cincinnati Specialty Ethnic Market", "$170,000", "Active", "r-2763")]
_SOLD = [("Cincinnati Flooring Company", "$385,000"), ("Greater Cincinnati Dry Cleaners", "$240,000"),
         ("Senior Services Business", "$117,000"), ("Coin Based Vending Route", "$60,000"),
         ("Dayton Fabrication Manufacturing Company", "$650,000"),
         ("Furniture Restoration Company", "$375,000")]
KEATE_HTML = (
    '<html><body><h2>Businesses For Sale</h2>'
    '<table id="tablepress-1" class="tablepress tablepress-id-1"><thead><tr class="row-1">'
    '<th>Listing ID</th><th>Description</th><th>Asking</th><th>Status</th></tr></thead><tbody>'
    + "".join(f'<tr class="row-{i + 2}"><td>{ref}</td>'
              f'<td><a href="https://www.keatepartners.com/business_listings/{slug}/">{t}</a></td>'
              f'<td>{p}</td><td>{st}</td></tr>' for i, (ref, t, p, st, slug) in enumerate(_LIVE))
    + '<tr class="row-6"><td>S-2765</td><td>Food Service Company</td><td></td><td>Coming Soon</td></tr>'
    '</tbody></table>'
    '<h2>Recent Businesses Sold</h2>'
    '<table id="tablepress-2" class="tablepress tablepress-id-2"><thead><tr class="row-1">'
    '<th>Description</th><th>Asking</th></tr></thead><tbody>'
    + "".join(f'<tr class="row-{i + 2}"><td>{t}</td><td>{p}</td></tr>' for i, (t, p) in enumerate(_SOLD))
    + '</tbody></table></body></html>')


def test_listing_table_detected_and_sold_section_skipped():
    p = PatternDetector.detect(KEATE_HTML, KEATE)
    assert p is not None
    assert p["container_selector"].startswith("table#tablepress-1 ")
    rows = _rows(KEATE_HTML, p["container_selector"])
    cards = [c for c in (ListingExtractor.from_card(r, KEATE, "Keate") for r in rows) if c]
    titles = [c["title"] for c in cards]
    assert len(cards) == 4
    assert not any("Flooring" in t for t in titles)          # sold table never read
    assert len({c["url"] for c in cards}) == 4


def test_sold_heading_table_alone_is_no_pattern():
    sold_only = KEATE_HTML.split("<h2>Recent Businesses Sold</h2>")[0].split("<h2>Businesses")[0]
    sold_only += KEATE_HTML.split("</table>")[1] + "</table></body></html>"
    assert "tablepress-1" not in sold_only and "tablepress-2" in sold_only
    p = PatternDetector.detect(sold_only, KEATE)
    assert p is None or "tablepress-2" not in p["container_selector"]


def test_fee_schedule_table_is_not_a_listing_table():
    # morganandwestfield.com's fee table: 19 rows of figures, no business named
    fee = ('<table class="has-fixed-layout"><tr><th>Purchase Price</th><th>Custom Package</th>'
           '<th>Standard Package</th></tr>'
           + "".join(f'<tr><td>${n},000,000</td><td>${n * 8}0,000</td><td>${n * 11}0,000</td></tr>'
                     for n in range(1, 10)) + '</table>')
    assert v6._table_row_candidates(BeautifulSoup(fee, "html.parser")) == []


def test_layout_and_footer_tables_are_not_candidates():
    inner = ('<table class="inner"><tr><td>Pizza Shop Ann Arbor Michigan</td><td>$100,000</td>'
             '<td><a href="/listing/1">View</a></td></tr></table>')
    layout = ('<table class="layout">' + "".join(f'<tr><td>Row {i} holder text here {inner}</td></tr>'
                                                for i in range(4)) + '</table>')
    footer = ('<footer><table class="links">' + "".join(
        f'<tr><td><a href="/listing/{i}">Pizza Shop Ann Arbor Michigan {i}</a></td><td>$10{i},000</td></tr>'
        for i in range(5)) + '</table></footer>')
    soup = BeautifulSoup(f'<html><body>{layout}{footer}</body></html>', "html.parser")
    sels = [s for s, _ in v6._table_row_candidates(soup)]
    assert not any("layout" in s or "inner" in s or "links" in s for s in sels)


# ── PatternCache.predict(): a bare tag is never adopted ────────────────────
def test_bare_tag_prediction_never_wins():
    detected = {"container_selector": "div.businesses-node", "score": 10}
    assert not predicted_beats_detected({"container_selector": "tr", "score": 30}, detected)
    assert not predicted_beats_detected({"container_selector": "div", "score": 30}, detected)
    assert not predicted_beats_detected({"container_selector": "li.menu-item", "score": 30}, detected)
    assert not predicted_beats_detected(None, detected)
    # today's rule for everything else is unchanged
    assert predicted_beats_detected({"container_selector": "div.card", "score": 30}, detected)
    assert not predicted_beats_detected({"container_selector": "div.card", "score": 9}, detected)


# ── image links ────────────────────────────────────────────────────────────
def test_detail_page_preferred_over_image_link():
    card = BeautifulSoup(
        '<div class="businesses-node"><a href="/businesses/9f31/image1/Photo_A.jpg"><img src="a.jpg"/></a>'
        '<h2><a href="/business-for-sale/absentee-owned-large-truck-wash">Truck Wash</a></h2></div>',
        "html.parser").div
    assert _best_detail_link(card) == "/business-for-sale/absentee-owned-large-truck-wash"


def test_image_only_card_keeps_its_link():
    card = BeautifulSoup('<tr><td><a href="/wp-content/uploads/2013/11/Private-Preschool.jpg">'
                         '<img src="x.jpg"/></a></td></tr>', "html.parser").tr
    assert _best_detail_link(card) == "/wp-content/uploads/2013/11/Private-Preschool.jpg"


# ── marlingroup.com ?pageno= ───────────────────────────────────────────────
def test_pageno_dropped_for_marlingroup_only():
    u = "https://www.marlingroup.com/restaurants-for-sale-portland/?pid=459&pageno=2"
    assert drop_volatile_params(u) == "https://www.marlingroup.com/restaurants-for-sale-portland/?pid=459"
    assert drop_volatile_params(u.replace("&pageno=2", "")) == u.replace("&pageno=2", "")
    other = "https://example.com/listing/?pid=459&pageno=2"
    assert drop_volatile_params(other) == other
    assert drop_volatile_params(None) is None
