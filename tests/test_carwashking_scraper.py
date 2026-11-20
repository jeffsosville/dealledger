import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from carwashking_scraper import parse_home, place  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "carwashking_home.html")
HOME_IDS = ["1028", "1027", "1026", "1022", "1021", "1020", "1012", "1011",
            "1010", "1009", "1008", "1003", "1025"]       # homepage, 2026-11-20


def _rows():
    with open(FIX, encoding="utf-8") as f:
        return parse_home(f.read())


def _id(r):
    return r["listing_url"].split("/carwash/")[1].split("/")[0]


def test_homepage_set_is_the_13_current_cards():
    rows = _rows()
    assert [_id(r) for r in rows] == HOME_IDS
    assert len({r["listing_url"] for r in rows}) == 13


def test_fields():
    r = next(r for r in _rows() if _id(r) == "1003")
    assert r["title"] == "Package of 3 High Performing Flex Serve Washes"
    assert r["price"] == 19_500_000
    assert r["state"] == "VA" and r["status"] == "active"
    assert r["listing_url"].startswith("https://carwashking.com/carwash/1003/car-wash-for-sale/")
    assert all(r["price"] and r["state"] for r in _rows())


def test_place():
    assert place("GA1028", "1028") == ("GA", "GA")
    assert place("AL & TN1021", "1021") == ("AL & TN", "AL")
    assert place("KY 421011025", "1025") == ("KY 42101", "KY")
    assert place("Georgia948", "948") == ("Georgia", None)


def test_sold_and_lease_cards_skipped():
    html = ('<li class="item" id="itemid-965"><div class="feature-title"><h6>'
            '<a href="carwash/965/car-wash-for-sale/SOLD-X/FL">SOLD: Exceptional Express</a>'
            '</h6></div></li>'
            '<li class="item" id="itemid-961"><div class="feature-title"><h6>'
            '<a href="carwash/961/car-wash-for-sale/X/FL">Brand New Express Tunnel For Lease in FL!</a>'
            '</h6></div></li>'
            '<li class="item" id="itemid-959"><div class="feature-title"><h6>'
            '<a href="carwash/959/car-wash-for-sale/X/IN">Brand New Express Tunnels Being Sold Turnkey in IN!</a>'
            '</h6></div></li>')
    assert [_id(r) for r in parse_home(html)] == ["959"]


def test_listing_key_ignores_slug():
    from run_specialized import listing_key
    a = "https://carwashking.com/carwash/1027/car-wash-for-sale/Established-Express-Tunnel/AL"
    b = "https://carwashking.com/carwash/1027/car-wash-for-sale/Established-Express-Tunnel-AL"
    assert listing_key(a) == listing_key(b) == "carwashking:1027"
    assert listing_key("https://example.com/carwash/1027/x") == "https://example.com/carwash/1027/x"
