"""petroleumrealty.net one-page Weebly listings (T1 2026-11-20). Fixture: the live
/listings.html captured 2026-11-20 (6 property blocks, 1 of them entitled land)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from petroleumrealty_scraper import parse_blocks, parse_page, _location, _money  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "petroleumrealty_listings.html")


def _html():
    with open(FIX, encoding="utf-8") as f:
        return f.read()


def _rows():
    return parse_page(_html(), log=lambda *_: None)


def test_blocks_and_emitted_count():
    blocks = parse_blocks(_html())
    assert len(blocks) == 6                    # the intro heading has no Asking Price
    rows = _rows()
    assert len(rows) == 5                      # ADELANTO COMMERCIAL DEVELOPMENT (land) not emitted
    assert not any("ADELANTO" in r["title"] for r in rows)


def test_titles_are_headings_not_bullets():
    titles = {r["title"] for r in _rows()}
    assert "RIVERSIDE PRIME FREEWAY GAS STATION" in titles
    assert "INDEPENDENT GAS STATION" in titles
    assert not any(t.lower().startswith(("approximately", "potential")) for t in titles)


def test_identity_is_heading_slug_unique():
    urls = [r["listing_url"] for r in _rows()]
    assert len(set(urls)) == len(urls) == 5
    assert all(u.startswith("https://petroleumrealty.net/listings.html#pr-") for u in urls)
    assert "https://petroleumrealty.net/listings.html#pr-riverside-prime-freeway-gas-station" in urls


def test_price_location_status():
    by = {r["title"]: r for r in _rows()}
    r = by["RIVERSIDE PRIME FREEWAY GAS STATION"]
    assert r["price"] == 6_100_000 and r["city"] == "Riverside" and r["state"] == "CA"
    t = by["BARSTOW ARCO ampm TRUCK STOP"]
    assert t["price"] == 7_700_000 and t["broker_status_text"] == "Submit a Backup Offer"
    assert t["status"] == "active"
    h = by["INDEPENDENT GAS STATION"]
    assert h["city"] == "Hemet" and h["state"] == "CA" and h["price"] == 3_850_000
    assert by["RANCHO CUCAMONGA ARCO ampm"]["city"] == "Rancho Cucamonga"
    assert all("broker_status_text" not in r for k, r in by.items() if k != "BARSTOW ARCO ampm TRUCK STOP")


def test_helpers():
    assert _money("$3.45M — Real Estate Included") == 3_450_000
    assert _money("$650,000") == 650_000
    assert _money("Call") is None
    assert _location("2191 W Main St, Barstow, CA 92311") == ("Barstow", "CA")
    assert _location("nowhere") == (None, None)


def test_registered_as_specialized_not_generic():
    import dealledger_scraper_v6 as v6
    assert "petroleumrealty.net" in v6.SPECIALIZED_DOMAINS
    assert v6.is_blocked("www.petroleumrealty.net")
