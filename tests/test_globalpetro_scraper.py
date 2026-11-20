"""globalpetroadvisors.com parser (scrapers/globalpetro_scraper.py).

Fixture: the <main> of https://globalpetroadvisors.com/listings/ as served on
2026-11-20 — 9 cards: 6 in the main list (one a 24-acre land site), one
office-condo project, two Under Contract (a 10-acre fuel site and a terminal
property). Expected in scope: 5 businesses.
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from globalpetro_scraper import parse_index, place  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "globalpetro_listings.html")
B = "https://globalpetroadvisors.com/listings/"


def _rows():
    return parse_index(open(FIX, encoding="utf-8").read())


def test_counts_match_the_page():
    rows, stats = _rows()
    assert stats == {"cards": 9, "active": 5, "out_of_scope": 4}
    assert len({r["listing_url"] for r in rows}) == 5


def test_in_scope_urls_exact():
    rows, _ = _rows()
    assert sorted(r["listing_url"] for r in rows) == sorted(B + s for s in (
        "supermarket-and-retail-plaza-central-appalachia-2648345/",
        "grocery-with-property-and-2-rentals-eastern-kentucky-2374014/",
        "gas-station-with-property-cleveland-area-2304981/",
        "print-ship-and-mailbox-franchise-columbus-suburbs-2681875/",
        "convenience-store-and-drive-thru-with-property-ashland-oh-2645338/",
    ))


def test_land_condos_terminal_excluded():
    rows, _ = _rows()
    urls = " ".join(r["listing_url"] for r in rows)
    for bad in ("development-site", "fuel-site", "office-condos", "terminal-property"):
        assert bad not in urls


def test_fields():
    rows, _ = _rows()
    by = {r["listing_url"].rsplit("/", 2)[-2]: r for r in rows}
    gas = by["gas-station-with-property-cleveland-area-2304981"]
    assert (gas["price"], gas["city"], gas["state"], gas["business_type"], gas["status"]) == \
        (920000.0, "East Cleveland", "OH", "Gas Station", "active")
    cs = by["convenience-store-and-drive-thru-with-property-ashland-oh-2645338"]
    assert (cs["price"], cs["city"], cs["state"]) == (389000.0, "Ashland", "OH")   # "Â·" separator
    assert by["print-ship-and-mailbox-franchise-columbus-suburbs-2681875"]["city"] == "Westerville"
    assert by["supermarket-and-retail-plaza-central-appalachia-2648345"]["state"] == "VA"
    assert all("broker_status_text" not in r for r in rows)


def test_place():
    assert place("Southwest Virginia (Central Appalachia)") == (None, "VA")
    assert place("Eastern Kentucky") == (None, "KY")
    assert place("Charleston, West Virginia") == ("Charleston", "WV")
    assert place("Westerville, Ohio") == ("Westerville", "OH")


def _card(title, line, badge=""):
    em = f'<em class="g-badge uc">{badge}</em>' if badge else ""
    slug = title.lower().replace(" ", "-").replace(",", "")
    return f'<li><a href="/listings/{slug}-1/">{em}<b>{title}</b><span>{line}</span><small>x</small></a></li>'


def test_under_contract_business_kept_active_with_label():
    html = ('<main><header class="g-hero"></header><ul class="g-lcards">'
            + _card("Gas station with property, Toledo", "Toledo, Ohio · $1,200,000") + '</ul>'
            '<h2 class="g-sub">Under contract</h2><ul class="g-lcards">'
            + _card("Gas station with property, Akron", "Akron, Ohio · Under contract", "Under Contract")
            + _card("Truck stop, Dayton", "Dayton, Ohio · Under contract", "Pending")
            + '</ul></main>')
    rows, stats = parse_index(html)
    assert stats == {"cards": 3, "active": 3}
    uc = [r for r in rows if r.get("broker_status_text")]
    assert sorted(r["broker_status_text"] for r in uc) == ["Pending", "Under Contract"]
    assert all(r["status"] == "active" and r["price"] is None for r in uc)


def test_lease_and_coming_soon_excluded():
    html = ('<main><ul class="g-lcards">'
            + _card("Gas station, Miami", "Miami, Florida · $500,000", "Coming Soon")
            + _card("C-store, Tampa", "Tampa, Florida · $9,000/mo", "For Lease")
            + _card("Gas station, Orlando", "Orlando, Florida · $700,000")
            + '</ul><h2 class="g-sub">For lease</h2><ul class="g-lcards">'
            + _card("Gas station, Ocala", "Ocala, Florida · $4,000/mo") + '</ul></main>')
    rows, stats = parse_index(html)
    assert [r["title"] for r in rows] == ["Gas station, Orlando"]
    assert stats["out_of_scope"] == 3
