"""gasstationsusa.com /properties/ cards (T1 2026-11-20). Fixture: the two live
pages captured 2026-11-20, trimmed to the card markup (14 cards)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from gasstationsusa_scraper import parse_cards, to_rows, has_next  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "gasstationsusa_properties.html")


def _pages():
    with open(FIX, encoding="utf-8") as f:
        return f.read().split("<!-- PAGE BREAK -->")


def _cards():
    return [c for p in _pages() for c in parse_cards(p)]


def _rows():
    return to_rows(_cards(), log=lambda *_: None)


def test_card_count_and_pagination():
    p1, p2 = _pages()
    assert len(parse_cards(p1)) == 10 and len(parse_cards(p2)) == 4
    assert has_next(p1) and not has_next(p2)


def test_cta_and_land_not_emitted():
    rows = _rows()
    titles = {r["title"] for r in rows}
    assert len(rows) == 11
    assert not any("Selling a Gas Station" in t for t in titles)
    assert "#1948 Daytona Beach Vacant Lot" not in titles
    assert "#1951 Gas Station / Commercial Lot for Sale" not in titles


def test_status_sold_and_pending():
    by = {r["title"]: r for r in _rows()}
    assert by["#2361 Orlando Circle K"]["status"] == "sold"
    assert by["#2380 Central FL Marathon"]["status"] == "sold"
    assert by["#2387 West Florida BP"]["status"] == "active"
    assert by["#2387 West Florida BP"]["broker_status_text"] == "Sale Pending"
    assert by["#2392 Jacksonville Arco"]["broker_status_text"] == "Contract Pending"
    active = [r for r in by.values() if r["status"] == "active"]
    assert len(active) == 9


def test_price_is_the_price_box_not_a_bullet():
    by = {r["title"]: r for r in _rows()}
    assert by["#2394 Palm Beach County"]["price"] == 499_000
    assert by["#2375 Florida Travel Plaza"]["price"] == 15_000_000
    assert all(r["state"] == "FL" for r in by.values())


def test_identity_is_detail_url():
    urls = [r["listing_url"] for r in _rows()]
    assert len(set(urls)) == len(urls)
    assert all(u.startswith("https://gasstationsusa.com/properties/single/") for u in urls)


def test_registered_as_specialized_not_generic():
    import dealledger_scraper_v6 as v6
    assert v6.is_blocked("gasstationsusa.com")
