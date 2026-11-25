"""Tests for scrapers/rv_cards_a.py (2026-11-24, rv-cards-a): card fields read by label.
Each reader has a must-fail case (the old bug) and must-pass cases (principle 8)."""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import rv_cards_a as r  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "rv")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as fh:
        return fh.read()


def _row(base_url, name, title=None, detail_url=None, asking_price=None, cash_flow=None,
         revenue=None, city=None, state=None, description=None):
    return {"id": detail_url or title, "title": title, "asking_price": asking_price,
            "cash_flow": cash_flow, "revenue": revenue, "state": state,
            "url": detail_url or base_url, "description": description}


def _status(title, domain=None):
    t = (title or "").upper()
    return ("active", "UNDER CONTRACT") if t.startswith("UNDER CONTRACT") else None


def _state(loc):
    loc = loc or ""
    for st in ("GA", "MA", "RI", "TX", "NJ", "PA", "CT", "NY"):
        if f", {st}" in loc or loc.endswith(" " + st) or loc == st:
            return st
    return "TX" if "Texas" in loc else ("GA" if "Georgia" in loc else None)


CTX = dict(make_row=_row, state_from_location=_state, norm_url=lambda u: u, title_status=_status)


class Fetcher:
    def __init__(self, pages):
        self.pages = pages

    def _fetch_http(self, url, **kw):
        return self.pages[url]


# ---- scope ----------------------------------------------------------------
def test_scope_excludes_the_mls_and_other_pages():
    assert r.in_scope("https://b3brokers.com/businesses-for-sale/")
    assert r.in_scope("https://coastalbizbrokers.com/featured-businesses-for-sale/")
    assert not r.in_scope("https://coastalbizbrokers.com/all-listings/")      # NEBBA Plus MLS
    assert not r.in_scope("https://b3brokers.com/")
    assert r.in_scope("https://www.biz-match.com/listings")
    assert r.in_scope("https://cbb-tristate.com/places/")
    assert r.read_index("https://example.com/listings", "", "x", None, CTX) is None


# ---- b3brokers.com ----------------------------------------------------------
def test_b3_price_is_the_price_label_and_total_sales_is_revenue():
    rows, drops = r.read_index("https://b3brokers.com/businesses-for-sale/",
                               _read("b3_cards.html"), "b3", None, CTX)
    by = {x["source_listing_id"]: x for x in rows}
    # the old bug: Total Sales ($2,641,553) was stored as the asking price
    assert by["1398"]["asking_price"] == 3_195_000 and by["1398"]["revenue"] == 2_641_553
    assert by["1728"]["asking_price"] == 137_000 and by["1728"]["revenue"] == 175_000
    assert by["1462"]["asking_price"] == 85_000 and by["1462"]["revenue"] == 142_000
    assert all(x["asking_price"] != x["revenue"] for x in rows)


def test_b3_cre_cards_gated_businesses_kept():
    rows, drops = r.read_index("https://b3brokers.com/businesses-for-sale/",
                               _read("b3_cards.html"), "b3", None, CTX)
    ids = {x["source_listing_id"] for x in rows}
    assert len(rows) == 14 and len(drops) == 2
    assert {"2088", "1700"}.isdisjoint(ids)             # Nettles Industrial, Augusta Rd C-2
    assert "2089" in ids                                # South American restaurant with Real Estate
    assert rows[0]["revenue"] and rows[0]["source_listing_id"] == "1398"


def test_b3_under_contract_stays_active_with_label():
    rows, _ = r.read_index("https://b3brokers.com/businesses-for-sale/",
                           _read("b3_cards.html"), "b3", None, CTX)
    uc = [x for x in rows if x["title"].startswith("UNDER CONTRACT")]
    assert len(uc) == 3
    assert all(x["status"] == "active" and x["broker_status_text"] == "UNDER CONTRACT" for x in uc)


def test_cre_gate_unit():
    assert r.is_cre_card("160 NETTLES INDUSTRIAL BLVD CRE For Sale", "N/A", "")
    assert r.is_cre_card("High-Visibility Commercial Site on Augusta Rd | C-2 Zoned", "N/A", "Retail")
    # must still pass
    assert not r.is_cre_card("Established South American Restaurant with Real Estate", "N/A", "Restaurant and Food")
    assert not r.is_cre_card("Retail Store, Zoned Commercial, Owner Retiring", "$900,000", "Retail")
    assert not r.is_cre_card("Gas Station with Real Estate", "N/A", "Retail")
    assert not r.is_cre_card("Established Spa, 5 acres", "N/A", "Beauty and Personal Care")


# ---- coastalbizbrokers.com ----------------------------------------------------
def test_coastal_drops_license_keeps_under_contract_with_label():
    rows, drops = r.read_index("https://coastalbizbrokers.com/featured-businesses-for-sale/",
                               _read("coastal_cards.html"), "c", None, CTX)
    assert len(rows) == 16 and [d[0] for d in drops] == ["Liquor License"]
    assert "19971" not in {x["source_listing_id"] for x in rows}
    uc = [x for x in rows if x.get("broker_status_text") == "UNDER CONTRACT"]
    assert len(uc) >= 4 and all(x["status"] == "active" for x in uc)
    sail = next(x for x in rows if x["source_listing_id"] == "19947")
    assert sail["asking_price"] == 85_000 and sail["cash_flow"] == 50_000
    assert sail["revenue"] is None


def test_coastal_cash_flow_na_is_null_not_a_neighbour():
    rows, _ = r.read_index("https://coastalbizbrokers.com/featured-businesses-for-sale/",
                           _read("coastal_cards.html"), "c", None, CTX)
    auto = next(x for x in rows if x["source_listing_id"] == "19960")   # Cash Flow: N/A
    assert auto["cash_flow"] is None and auto["asking_price"] == 1_650_000


def test_license_only_unit():
    assert r.is_license_only("Liquor License", "", "")
    assert not r.is_license_only("Liquor Store W/RE", "", "")
    assert not r.is_license_only("Package Liquor Store with License", "", "")


# ---- biz-match.com --------------------------------------------------------------
def test_bizmatch_line_labelled_regexes():
    assert r.parse_bizmatch_line("Price: $3,200,000 Sales Revenue: $285,000 Net Profit: $170,000") == \
        (3_200_000, 285_000, 170_000)
    # Undisclosed / Not applicable are null, never filled from the price
    assert r.parse_bizmatch_line("Price: $8,950,000 Sales Revenue: Undisclosed Net Profit: Undisclosed") == \
        (8_950_000, None, None)
    assert r.parse_bizmatch_line("Price: $195,000 Sales Revenue: Not applicable Net Profit: $50,000") == \
        (195_000, None, 50_000)
    # revenue above price must not become the price (Smoke & Vape)
    assert r.parse_bizmatch_line("Price: $395,000 Sales Revenue: $1,000,000 Net Profit: $150,000")[0] == 395_000


def test_bizmatch_page_never_reads_description_dollars():
    html = _read("bizmatch_p1.html")
    rows, _ = r.read_index("https://www.biz-match.com/listings", html, "b",
                           Fetcher({"https://www.biz-match.com/listings-2": "<html></html>"}), CTX)
    assert len(rows) == 20
    hosp = next(x for x in rows if x["title"].startswith("Exceptional Texas Gulf Coast"))
    assert hosp["asking_price"] == 8_950_000 and hosp["revenue"] is None and hosp["cash_flow"] is None
    vape = next(x for x in rows if x["title"].startswith("Established Smoke and Vape"))
    assert vape["asking_price"] == 395_000 and vape["revenue"] == 1_000_000
    # a description that says "$75K+ Inventory" is not cash flow
    html2 = html.replace("Net Profit: </strong>Undisclosed", "Net Profit: </strong>Undisclosed", 1)
    html2 = html2.replace('<dd class="description">', '<dd class="description">$75K+ Inventory. ')
    rows2, _ = r.read_index("https://www.biz-match.com/listings", html2, "b",
                            Fetcher({"https://www.biz-match.com/listings-2": "<html></html>"}), CTX)
    assert [x["cash_flow"] for x in rows2] == [x["cash_flow"] for x in rows]


# ---- cbb-tristate.com ---------------------------------------------------------------
def _cbb_rows():
    pages = {"https://cbb-tristate.com/places/page/2/": _read("cbb_p2.html")}
    return r.read_index("https://cbb-tristate.com/places/", _read("cbb_p1.html"), "cbb",
                        Fetcher(pages), CTX)


def test_cbb_title_is_heading_not_category_label():
    rows, _ = _cbb_rows()
    assert len(rows) == 20
    titles = [x["title"] for x in rows]
    assert not any(t in ("Gas Station with C-Store", "Gas Station", "Truck Stop", "Favorite") for t in titles)
    assert len(set(x["url"] for x in rows)) == 20            # one row per detail page
    assert "Citgo Gas Station(Luzerne County)PA" in titles
    cit = next(x for x in rows if x["title"].startswith("Citgo"))
    assert cit["source_listing_id"] == "001143" and cit["asking_price"] == 200_000
    assert cit["cash_flow"] == 80_837
    assert "Category: Gas Station with C-Store" in cit["description"]
    assert "Site Type:" in cit["description"]


def test_cbb_odd_price_stored_as_published():
    rows, _ = _cbb_rows()
    odd = next(x for x in rows if x["source_listing_id"] == "001140")
    assert odd["asking_price"] == 1_140                      # flagged for Jeff, not "fixed"


def test_cbb_state_from_trailing_code():
    ctx = SimpleNamespace(**CTX)
    assert r._cbb_state("Citgo Gas Station(Luzerne County)PA", ctx) == "PA"
    assert r._cbb_state("Truck stop Gloucester County NJ", ctx) == "NJ"
    assert r._cbb_state("Exxon gas station 3 in 1(Lackawanna County PA)", ctx) == "PA"
