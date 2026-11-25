"""Tests for scrapers/cr_card_fields.py (2026-11-23, w4-card-fields).
Each reader gets a must-fail case (the old bug) and a must-pass case (principle 8)."""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import cr_card_fields as c  # noqa: E402


def _row(base_url, name, title=None, detail_url=None, asking_price=None, cash_flow=None,
         revenue=None, city=None, state=None, description=None):
    return {"id": detail_url or title, "title": title, "asking_price": asking_price,
            "cash_flow": cash_flow, "revenue": revenue, "state": state,
            "url": detail_url or base_url, "description": description}


CTX = dict(make_row=_row, state_from_location=lambda loc: "IL" if "Illinois" in (loc or "") else None,
           norm_url=lambda u: u, title_from_slug=lambda u: u.rsplit("/", 1)[-1],
           title_status=lambda t, d=None: None)


class FakeFetcher:
    def __init__(self, pages=None):
        self.pages = pages or {}

    def _fetch_http(self, url, **kw):
        if url in self.pages:
            return self.pages[url]
        raise RuntimeError("no page " + url)


# ---- money ---------------------------------------------------------------
def test_money_parses_and_rejects():
    assert c.money("$1,575,000") == 1_575_000
    assert c.money("1,295,000") == 1_295_000
    assert c.money("$4.72M") == 4_720_000
    assert c.money("$5,500,00") is None          # malformed grouping, never guessed
    assert c.money("-") is None
    assert c._fin("$7") is None                  # Alliant "Gross Sale: $7" typo
    assert c._fin("1") is None                   # ABB placeholder
    assert c._fin("$9,438,646") == 9_438_646


# ---- americanbusinessbrokers.com -------------------------------------------
ABB = """<table id="oDivTable">
<tr id="L1X1"><th><a href="/details.asp?LID=1" title="x">ABB005-B57 - Illinois Steel Distributor w/ Building - Business For Sale</a><br>Central Illinois</th>
<td>1,575,000</td><td>1,575,000</td><td>1</td><td>&nbsp;</td><td>&nbsp;</td></tr>
<tr id="D1X1"><th colspan="6"><p>The 2025 sales were over $2,900,000.</p></th></tr>
<tr id="L2X2"><th><a href="/details.asp?LID=2">ABB005-B99 - Call Us Business</a><br>Central Illinois</th>
<td>Call</td><td>&nbsp;</td><td>2,500,000</td><td>400,000</td><td>&nbsp;</td></tr>
<tr id="D2X2"><th colspan="6"><p>Prose.</p></th></tr></table>"""


def test_abb_price_is_listing_price_column_only():
    rows, drops = c.read_abb("https://americanbusinessbrokers.com/display-national.asp", ABB, "abb", None, SimpleNamespace(**CTX))
    assert len(rows) == 2
    a, b = rows
    assert a["asking_price"] == 1_575_000          # not the $2.9M in the description
    assert a["revenue"] is None                    # Gross Sales "1" is a placeholder
    assert a["title"] == "Illinois Steel Distributor w/ Building"
    assert a["source_listing_id"] == "ABB005-B57"
    assert b["asking_price"] is None and b["revenue"] == 2_500_000 and b["cash_flow"] == 400_000


# ---- alliantbrokers.com ----------------------------------------------------
def _alliant(title, price, gross):
    return f"""<a class="single-grid" href="https://alliantbrokers.com/listing/{title[:5]}/"><div class="single-grid-content">
<h2 class="post-title">{title}</h2><p><b>Tennessee</b></p><p>desc</p></div>
<div class="price_main_wrap"><p><span>Price:</span> {price}</p><p><span>Gross Sale:</span> {gross}</p>
<p><span>Cash Flow:</span> $100,000</p></div></a>"""


def test_alliant_price_label_and_gross_typo():
    html = _alliant("Alabama HVAC", "$1,150,000", "$2,112,807") + _alliant("Fencing", "$3,200,000", "$7") \
        + _alliant("Boat UNDER CONTRACT", "$1,350,000", "$1,729,070")
    rows, _ = c.read_alliant("https://alliantbrokers.com/businesses-for-sale-4", html, "a", None, SimpleNamespace(**CTX))
    assert [r["asking_price"] for r in rows] == [1_150_000, 3_200_000, 1_350_000]
    assert rows[0]["revenue"] == 2_112_807
    assert rows[1]["revenue"] is None            # "$7" typo


# ---- enlign.com ------------------------------------------------------------
def _enlign(lid, title, price, status=""):
    st = f'<span data-slot="status">{status}</span>' if status else ""
    return f"""<article class="listing-card-item"><div>{st}
<span data-slot="internal-id-value">{lid}</span><h3 data-slot="title"><a data-slot="title-link" href="https://enlign.com/listings/{lid.lower()}/">{title}</a></h3>
<p data-slot="price">{price}</p><p data-slot="location">NC</p>
<ul><li class="listing-card-financial"><span class="listing-card-financial-label">Annual Sales</span><span class="listing-card-financial-value">$5,987,029</span></li></ul></div></article>"""


def test_enlign_price_line_mtd_and_coming_soon():
    html = _enlign("TILE1", "Tile", "$1,125,000") + _enlign("DOORS1", "Doors", "Market to Determine") \
        + _enlign("HVAC7", "Hvac", "Market to Determine", "Coming Soon") \
        + '<article class="listing-card-item"><template></template></article>'
    rows, drops = c.read_enlign("https://enlign.com/listings", html, "e", None, SimpleNamespace(**CTX))
    assert [r["source_listing_id"] for r in rows] == ["TILE1", "DOORS1"]
    assert rows[0]["asking_price"] == 1_125_000 and rows[0]["revenue"] == 5_987_029
    assert rows[1]["asking_price"] is None and rows[1]["price_not_disclosed"] is True
    assert [d[0] for d in drops] == ["Hvac"]


def test_adjust_stated_subtracts_dropped_coming_soon():
    t = SimpleNamespace(stated={"count": 12, "source": "stated", "text": "12 listings"}, _cands=("x",))
    c.LAST_DROPS["enlign.com"] = 3
    c.adjust_stated(t, "enlign.com")
    assert t.stated["count"] == 9 and t.stated["source"] == "stated" and t._cands is None
    # other domains and counted sources are untouched
    t2 = SimpleNamespace(stated={"count": 12, "source": "stated"}, _cands=("x",))
    c.LAST_DROPS["other.com"] = 3
    c.adjust_stated(t2, "other.com")
    assert t2.stated["count"] == 12 and t2._cands == ("x",)


# ---- ubibiz.com ------------------------------------------------------------
def test_ubibiz_price_last_paragraph_only():
    assert c.ubibiz_price(["Seller spent over $500,000 before opening.", "149,900 owner may carry for a qualified Buyer.", "© 2020 UBI"]) == 149_900
    assert c.ubibiz_price(["x", "95000  or offer"]) == 95_000
    assert c.ubibiz_price(["x", "770, 000 or ofer + inventory"]) == 770_000
    assert c.ubibiz_price(["Seller spent over $500,000 before opening up the restaurant in a big remodel."]) is None
    assert c.ubibiz_price(["No price here, call us."]) is None


def test_ubibiz_reader_follows_wix_detail_links_and_title():
    base = "https://ubibiz.com/buy-a-business"
    idx = '<a href="https://www.ubibiz.com/buy-a-business-1/boba">x</a><a href="/buy-a-business">i</a><a href="/sell-a-business-1">s</a>'
    det = ("<title>Boba Shop</title><h1>Get More Info</h1><h1>Boba Shop</h1><p>Boba Shop</p>"
           "<p>Great location.</p><p>95000  or offer</p><p>© 2020 UBI</p>")
    f = FakeFetcher({"https://www.ubibiz.com/buy-a-business-1/boba": det})
    rows, _ = c.read_ubibiz(base, idx, "u", f, SimpleNamespace(**CTX))
    assert len(rows) == 1 and rows[0]["title"] == "Boba Shop" and rows[0]["asking_price"] == 95_000


# ---- greenandco: error-page guard -------------------------------------------
def test_is_error_page():
    assert c.is_error_page("<html><head><title>403 - Forbidden</title></head><body><h1>403 - Forbidden</h1></body></html>")
    assert c.is_error_page("<title>Access Denied</title><p>blocked</p>")
    assert c.is_error_page("<title>Just a moment...</title>")
    assert c.is_error_page("")
    real = "<title>Beach Optical</title><h1>Beach Optical</h1><p>" + "A real listing page. " * 120 + "</p>"
    assert not c.is_error_page(real)
    # a short but real page whose title is not an error phrase must pass (principle 8)
    assert not c.is_error_page("<title>Hair Salon</title><h1>Hair Salon</h1><p>Asking Price: $50,000</p>")
    assert not c.is_error_page("<title>Route 403 Pizza</title><h1>Route 403 Pizza</h1><p>For sale</p>")


# ---- krbb.com ---------------------------------------------------------------
def test_krbb_title_suffix():
    assert c.strip_price_suffix("Eastside Dive Bar Price $110,000.00") == "Eastside Dive Bar"
    assert c.strip_price_suffix("Price Right Pizza") == "Price Right Pizza"
    cards = [{"title": "Printing Business Price $340,000.00"}, {"title": "Other"}]
    assert c.finalize_cards("www.krbb.com", cards) == 1 and cards[0]["title"] == "Printing Business"
    cards2 = [{"title": "X Price $5.00"}]
    assert c.finalize_cards("other.com", cards2) == 0 and cards2[0]["title"] == "X Price $5.00"


# ---- lionbusinessadvisors.com ----------------------------------------------
def test_lion_pages_from_nav_not_hardcoded():
    home = ('<a href="/business-for-sale-texas/">Adjusted SDE: Up To $1M</a>'
            '<a href="/adjusted-ebitda-1m/">Adjusted EBITDA $1M+</a><a href="/about/">About</a>')
    pages = c._lion_pages("https://lionbusinessbrokers.com/", home, FakeFetcher())
    assert pages == ["https://lionbusinessbrokers.com/business-for-sale-texas/",
                     "https://lionbusinessbrokers.com/adjusted-ebitda-1m/"]
    sm = "<loc>https://lionbusinessbrokers.com/adjusted-ebitda-1m/</loc><loc>https://lionbusinessbrokers.com/contact/</loc>"
    got = c._lion_pages("https://lionbusinessbrokers.com/", "<p>none</p>",
                        FakeFetcher({"https://lionbusinessbrokers.com/sitemap.xml": sm}))
    assert got == ["https://lionbusinessbrokers.com/adjusted-ebitda-1m/"]


def _lion_card(num, lead, right, link=True):
    a = f'<a href="https://lionbusinessbrokers.com/listing-{num}/">Title {num}</a>' if link else ""
    return f"""<section class="elementor-top-section"><section class="elementor-inner-section">
<p>{lead}{a}</p><p>{right.replace(" SDE", "<br>SDE")}<br>Listing #{num}</p></section></section>"""


def test_lion_cards_status_price_rules():
    page = (_lion_card(1, "UNDER LOI: ", "Listed at: $975,000 SDE: $245,000")
            + _lion_card(2, "", "Listed at: OPEN SDE: $525,000")
            + _lion_card(3, "Coming Soon: ", "Listed at: $675,000 SDE: 175,000")
            + _lion_card(4, "IP Acquisition Opportunity | thing ", "Listed at: $1,200,000"))
    f = FakeFetcher({"https://lionbusinessbrokers.com/business-for-sale-texas/": page})
    home = '<a href="/business-for-sale-texas/">Adjusted SDE: Up To $1M</a>'
    f.pages["https://lionbusinessbrokers.com/"] = home
    rows, drops = c.read_lion("https://lionbusinessbrokers.com/", home, "l", f, SimpleNamespace(**CTX))
    by = {r["source_listing_id"]: r for r in rows}
    assert set(by) == {"1", "2"}
    assert by["1"]["asking_price"] == 975_000 and by["1"]["status"] == "active" and by["1"]["broker_status_text"] == "UNDER LOI"
    assert by["2"]["asking_price"] is None and by["2"]["price_not_disclosed"] is True
    assert len(drops) == 2


# ---- read_index dispatch -----------------------------------------------------
def test_read_index_unregistered_domain_is_none_and_failure_is_loud(capsys):
    assert c.read_index("https://example.com/x", "", "n", None, CTX) is None
    rows, drops = c.read_index("https://arkansasbusinessbrokers.com/business-listings", "not json", "n", FakeFetcher(), CTX)
    assert rows == [] and "FAILED" in capsys.readouterr().out


# ---- arkansasbusinessbrokers.com ---------------------------------------------
def test_arkansas_json_collection():
    import json
    base = "https://www.arkansasbusinessbrokers.com"
    index = {"pagination": None, "items": [
        {"fullUrl": "/business-listings/a1", "categories": ["Retail"], "excerpt": "<p><strong>SALE PENDING!</strong> x</p>"},
        {"fullUrl": "/business-listings/b2", "categories": ["Retail", "Real Estate"], "excerpt": ""},
        {"fullUrl": "/business-listings/c3", "categories": ["Hotel"], "excerpt": ""}]}

    def item(title, body):
        return json.dumps({"item": {"title": title, "body": body}})
    f = FakeFetcher({
        base + "/business-listings?format=json": json.dumps(index),
        base + "/business-listings/a1?format=json": item("Grocery", "<p>Type: Retail</p><p>Location: Ozark, AR 72949</p><p>Price: $750,000</p>"),
        base + "/business-listings/b2?format=json": item("Retail Space with Real Estate", "<p>Price: $2,040,000</p>"),
        base + "/business-listings/c3?format=json": item("Motel", "<p>Location: Eureka Springs, AR 72632</p>")})
    rows, drops = c.read_arkansas(base + "/business-listings", "<html>", "ark", f, SimpleNamespace(**CTX))
    assert [r["title"] for r in rows] == ["Grocery", "Motel"]
    assert rows[0]["asking_price"] == 750_000 and rows[0]["status"] == "active" and rows[0]["broker_status_text"] == "SALE PENDING"
    assert rows[1]["asking_price"] is None             # no Price line: never inferred
    assert [d[0] for d in drops] == ["Retail Space with Real Estate"]
