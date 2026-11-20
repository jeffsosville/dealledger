"""buygasstations.com GoDaddy text blocks (T1 2026-11-20). Fixtures: the page text
lines of both live pages captured 2026-11-20."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from buygasstations_scraper import parse_blocks, to_rows, PAGES  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures")


def _rows(name, page):
    with open(os.path.join(FIX, f"buygasstations_{name}.txt"), encoding="utf-8") as f:
        return to_rows(parse_blocks(f.read().split("\n")), page, log=lambda *_: None)


def test_business_only_page():
    rows = _rows("business_only", PAGES[0])
    assert len(rows) == 9
    assert sum(r["status"] == "active" for r in rows) == 8
    by = {r["listing_url"].split("#dl-")[1]: r for r in rows}
    assert by["alb-08-p"]["price"] == 1_000_000          # V6 stored 3,450,000 (a slideshow caption)
    assert by["alb-08-p"]["title"] == "MOBIL GAS STATION BUSINESS ONLY FOR SALE"
    assert by["alh-mobil-business-package"]["title"].startswith("PACKAGE OF 3 MOBIL / 7-ELEVEN")
    assert by["alh-213-p"]["status"] == "sold"
    assert by["alh-669b2"]["city"] == "Hialeah" and by["alh-669b2"]["state"] == "FL"
    assert not any("LEASE" in r["title"] for r in rows)


def test_with_property_page_skips_land_lease_and_non_gas():
    rows = _rows("with_property", PAGES[1])
    titles = [r["title"] for r in rows]
    assert len(rows) == 7 and all(r["status"] == "active" for r in rows)
    assert not any(("LOT" in t or "STRIP" in t or "BUILDING" in t) for t in titles)
    assert all("PROPRTY" not in t for t in titles)
    upon = next(r for r in rows if r["listing_url"].endswith("#dl-alhm-272"))
    assert upon["price"] is None and upon["price_text"] == "Price upon request"


def test_identity_is_broker_ref_unique():
    rows = _rows("business_only", PAGES[0]) + _rows("with_property", PAGES[1])
    urls = [r["listing_url"] for r in rows]
    assert len(set(urls)) == len(urls) == 16
    assert all("#dl-" in u for u in urls)


def test_registered_as_specialized_not_generic():
    import dealledger_scraper_v6 as v6
    assert v6.is_blocked("buygasstations.com")
