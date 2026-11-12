"""Zebra-striped tables: both row classes are listings (commercialroutesforsale.com)."""
import hashlib
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
from dealledger_scraper_v6 import (ListingExtractor, PatternDetector,  # noqa: E402
                                   is_listing_element, widen_striped_rows)

BASE = "https://www.commercialroutesforsale.com/RouteList.aspx"
ROUTES = [  # (id, type, total price, down, weekly net, city, state)
    (6251, "WISE SNACKS", "$219,000", "$219,000", "$2,000", "Brooklyn", "NY"),
    (6250, "Utz &amp; Bachman", "$195,000", "$90,000", "$1,700", "Queens", "NY"),
    (6249, "Pepperidge Farm Bread", "$175,000", "$80,000", "$1,400", "Atlanta", "GA"),
    (6248, "TastyKake", "$199,000", "$89,000", "$1,500", "Voorhees", "NJ"),
    (6239, "Pepperidge Farm Cookie", "$610,000", "$190,000", "$1,900", "Manalapan", "NJ"),
    (6236, "Flowers Bread Route", "$330,000", "$190,000", "$2,000", "Trexlertown", "PA"),
]


def _row(cls, rid, rtype, price, down, net, city, st, desc=None):
    desc = desc or (f"Net {net} a week after all expenses.....This {rtype} route for sale "
                    f"delivers to towns around {city}.....Please call (800) 769-0927.")
    return (f'<tr class="{cls}" onClick="location.href=\'RouteListDetail.aspx?RouteID={rid}\'">'
            f'<td class="td-Image"><img src="x.jpg"/></td>'
            f'<td data-title="Route Number">{rid}</td><td data-title="Route Type">{rtype}</td>'
            f'<td data-title="Total Price">{price}</td><td data-title="Down Payment">{down}</td>'
            f'<td data-title="Weekly Net">{net}</td><td data-title="City">{city}</td>'
            f'<td data-title="State">{st}</td><td class="td-description">{desc}</td></tr>')


def _page(extra_rows=""):
    rows = "".join(_row("even" if i % 2 == 0 else "odd", *r) for i, r in enumerate(ROUTES))
    placeholder = _row("odd", 6107, "FedEx Home &amp; Ground", "&nbsp;", "&nbsp;", "&nbsp;",
                       "&nbsp;", "&nbsp;",
                       desc="We have several other FedEx routes available.  Please call for more details.")
    return ('<html><body><table><tr><th>Route Number</th><th>Route Type</th><th>Total Price</th></tr>'
            f'{rows}{placeholder}{extra_rows}</table></body></html>')


def _cards(html, selector):
    soup = BeautifulSoup(html, "html.parser")
    return [c for c in (ListingExtractor.from_card(el, BASE, "Commercial Routes")
                        for el in soup.select(selector)) if c]


def test_detector_picks_one_stripe_widen_adds_the_other():
    html = _page()
    pat = PatternDetector.detect(html, BASE)
    assert pat["container_selector"] in ("tr.even", "tr.odd")
    wide = widen_striped_rows(html, pat["container_selector"], BASE)
    assert set(s.strip() for s in wide.split(",")) == {"tr.even", "tr.odd"}


def test_all_routes_extracted_once_placeholder_dropped():
    html = _page()
    cards = _cards(html, widen_striped_rows(html, "tr.even", BASE))
    urls = [c["url"] for c in cards]
    assert len(cards) == len(ROUTES) == len(set(urls))
    assert not any("RouteID=6107" in u for u in urls)
    # id is sha256(detail url)[:16] - the same identity already stored for these rows
    for c in cards:
        assert c["url"].startswith("https://www.commercialroutesforsale.com/RouteListDetail.aspx?RouteID=")
        assert c["id"] == hashlib.sha256(c["url"].encode()).hexdigest()[:16]
    # Total Price is the asking price, never the down payment or weekly net
    by_id = {c["url"].rsplit("=", 1)[1]: c for c in cards}
    assert by_id["6239"]["asking_price"] == 610_000
    assert by_id["6250"]["asking_price"] == 195_000


def test_cookie_route_is_a_listing():
    # "Cookie" in a product name is not a cookie banner (was a nav word).
    el = BeautifulSoup(_row("even", *ROUTES[4]), "html.parser").select_one("tr")
    assert is_listing_element(el, BASE)


def test_cookie_banner_still_rejected():
    el = BeautifulSoup('<div class="c">Cookie Policy - we use cookies to improve this site. '
                       '<a href="/listing/1">More</a> $100,000</div>', "html.parser").select_one("div")
    assert not is_listing_element(el, BASE)


def test_pager_row_not_added():
    pager = ('<tr class="pager"><td colspan="9"><a href="RouteList.aspx?page=1">1</a> '
             '<a href="RouteList.aspx?page=2">2</a></td></tr>')
    wide = widen_striped_rows(_page(pager), "tr.even", BASE)
    assert "tr.pager" not in wide


def test_non_tr_selector_unchanged():
    html = ('<div class="grid">' + "".join(
        f'<div class="{c}"><a href="/listing/{i}">Bakery in Austin TX number {i}</a> $250,000</div>'
        for i, c in enumerate(["card", "card", "card", "card featured"])) + '</div>')
    assert widen_striped_rows(html, "div.card", BASE) == "div.card"


def test_single_class_table_unchanged():
    rows = "".join(_row("item", *r) for r in ROUTES)
    html = f"<table>{rows}</table>"
    assert widen_striped_rows(html, "tr.item", BASE) == "tr.item"
