"""cr-dealstudio (2026-11-25): Deal Studio card reader in scrapers/cr_dealstudio.py.
Fixture = trimmed rpmshopsales.com cards (3 ACTIVE + 1 UNDER CONTRACT), one
mainascent-style card whose Price sits in a div.sde next to an SDE field, and a
synthetic COMING SOON card."""
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import cr_dealstudio as ds  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

HTML = (Path(__file__).parent / "fixtures" / "dealstudio" / "rpm_cards.html").read_text()
URL = "https://www.rpmshopsales.com/businesses-for-sale/"


def rows():
    return ds.handler_dealstudio(URL, HTML, "rpm", None)


def test_positive_counts_labels_prices_ids():
    r = rows()
    by = {x["title"]: x for x in r}
    assert len(r) == 5            # 4 rpm + 1 mainascent-style; Coming Soon not emitted
    assert not any("coming-soon" in x["url"] for x in r)
    uc = [x for x in r if x.get("broker_status_text") == "UNDER CONTRACT"]
    assert len(uc) == 1 and uc[0]["status"] == "active"
    assert by["Houston Area Auto Repair with Property"]["asking_price"] == 1600000.0
    assert by["Houston Area Auto Repair with Property"]["source_listing_id"] == "2364736"
    assert all(x["url"].startswith("https://www.rpmshopsales.com/listing/") or "/listing/" in x["url"] for x in r)


def test_price_is_labelled_price_not_sde():
    r = [x for x in rows() if "steel" in x["url"].lower() or "mainascent" in x["url"]]
    assert r and r[0]["asking_price"] == 4300000.0 and r[0]["asking_price"] != 337463.0


def test_sold_badge_is_sold():
    html = HTML.replace("ACTIVE", "SOLD", 1).replace("activeButton", "soldButton", 1)
    out = ds.handler_dealstudio(URL, html, "rpm", None)
    assert [x["status"] for x in out].count("sold") == 1


def test_identity_ignores_price():
    a = {x["url"]: x["id"] for x in rows()}
    html = HTML.replace("$1,600,000", "$1,400,000")
    b = {x["url"]: x["id"] for x in ds.handler_dealstudio(URL, html, "rpm", None)}
    assert a == b


def test_negative_non_deal_studio_page_does_not_engage():
    plain = HTML.replace("deal-studio.com", "example.com").replace("Deal Studio", "Someone")
    assert not ds.is_deal_studio(plain)
    assert ds.handler_dealstudio(URL, plain, "x", None) is None
    # footer credit but no card grid -> nothing to read
    assert ds.handler_dealstudio(URL, "<html>Site Design by Deal Studio</html>", "x", None) is None


def test_registered_in_v6_and_pager_follow():
    assert any(h is ds.handler_dealstudio for _s, h in v6.API_HANDLERS)
    pg = '<a href="/businesses-for-sale/?wpv_view_count=1&amp;wpv_paged=2">2</a><a href="/other/?wpv_paged=3">x</a>'
    assert ds.next_page_urls(pg, URL) == ["https://www.rpmshopsales.com/businesses-for-sale/?wpv_view_count=1&wpv_paged=2"]


def _card(extra, price='<span class="priceDescriptionValue">$500,000</span>'):
    return ('<html>deal-studio.com<div class="listingBox"><div class="listingTitle">'
            '<a href="https://x.com/listing/a/">A Shop</a></div><div class="listingContent">'
            + extra + '<div class="listingPrice">' + price + '</div></div></div></html>')


def test_card_financial_labels_positive_and_never_cross_assigned():
    html = _card('<div class="s"><span class="descriptionName">SDE:</span><span class="descriptionValue">$120,000</span></div>'
                 '<div class="g"><span class="descriptionName">Gross Revenue:</span><span class="descriptionValue">$1,500,000</span></div>'
                 '<div class="e"><span class="descriptionName">EBITDA:</span><span class="descriptionValue">$99,000</span></div>')
    r = ds.handler_dealstudio("https://x.com/businesses-for-sale/", html, "x", None)[0]
    assert (r["cash_flow"], r["revenue"], r["asking_price"]) == (120000.0, 1500000.0, 500000.0)


def test_financial_na_is_none_and_ebitda_not_used():
    html = _card('<div class="s"><span class="descriptionName">SDE:</span><span class="descriptionValue">N/A</span></div>'
                 '<div class="e"><span class="descriptionName">EBITDA:</span><span class="descriptionValue">$99,000</span></div>')
    r = ds.handler_dealstudio("https://x.com/businesses-for-sale/", html, "x", None)[0]
    assert r["cash_flow"] is None and r["revenue"] is None


def test_detail_fill_when_card_lacks_financials_keeps_card_price(monkeypatch):
    class F:
        def _fetch_http(self, url, use_proxy=False):
            return "<html><body>Asking Price $9,999,999 SDE $77,000 Gross Revenue $800,000</body></html>"
    monkeypatch.setitem(ds._H, "enrich", v6.ListingExtractor.enrich_from_detail)
    monkeypatch.setitem(ds._H, "delay", (0, 0))
    r = ds.handler_dealstudio("https://x.com/businesses-for-sale/", _card(""), "x", F())[0]
    assert r["cash_flow"] == 77000.0 and r["revenue"] == 800000.0
    assert r["asking_price"] == 500000.0          # card's labelled Price wins over the page
