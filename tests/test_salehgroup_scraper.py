"""salehgroup.com parser (scrapers/salehgroup_scraper.py).

Fixture: 24 of the 281 items served by https://api.muzopilot.com/public/listings/saleh-group
on 2026-11-23, picked for scope edge cases. 6 must be skipped (shopping plaza, apartments,
residential development, beer & wine licences); the rest are in scope, several because they
share words with the skipped ones (plaza, property, real estate, development).
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from salehgroup_scraper import DETAIL_URL, parse_items, scope, sg_number  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "salehgroup_api.json")
SKIPPED = {"11857", "11838", "11788", "11745", "11754", "11740"}


def _items():
    return json.load(open(FIX, encoding="utf-8"))["data"]


def _rows():
    return parse_items(_items())


def test_exactly_the_real_estate_cards_are_skipped():
    rows, info = _rows()
    assert {s["sg"] for s in info["skipped"]} == SKIPPED
    assert info["cards"] == 24 and info["kept"] == 18 == len(rows)
    assert all(s["reason"] for s in info["skipped"])


def test_in_scope_cards_with_similar_words_pass():
    rows, _ = _rows()
    titles = " | ".join(r["title"] for r in rows)
    for must in ("Truck Stop Plaza and Restaurant", "Gas Station C Store and Plaza",
                 "Convenient Food Mart Liquor and Plaza", "Fleet Convenience Food Mart and Plaza Property",
                 "Branded Gas Station C- Store Property", "Vacant Convenience Food Mart business assets",
                 "Liquor and Smoke Shop with Property", "Branded Hotel and Suites"):
        assert must in titles, must


def test_borderline_kept_and_flagged_for_jeff():
    _, info = _rows()
    assert {f["sg"] for f in info["flagged"]} == {"11865", "11725", "11727"}


def test_every_row_has_its_own_detail_url():
    rows, _ = _rows()
    urls = {r["listing_url"] for r in rows}
    assert len(urls) == len(rows)
    assert all(u.startswith("https://salehgroup.com/listing?id=") for u in urls)
    assert DETAIL_URL.format(id="cmsj49vpv0024hoj7ogg6qoam") in urls


def test_prices():
    rows, _ = _rows()
    by = {r["title"].split()[1]: r for r in rows}
    assert by["12025"]["price"] == 1200000.0 and by["12025"]["price_text"] is None      # "$1.20M"
    assert by["11981"]["price"] == 65000.0                                              # "$65K"
    assert by["11882"]["price"] is None and by["11882"]["price_text"] == "Inquire"      # not disclosed


def test_sg_ref_spellings():
    ids = {sg_number(i) for i in _items() if i["id"] in (
        "cmsj49vpv0024hoj7ogg6qoam", "lst_2707b987aed3b08e644221ba",
        "lst_dup_mr87eshf_959869", "lst_wpimp_19702")}
    assert ids == {"11771", "11765", "11711", "11680"}


def test_rows_active_with_state():
    rows, _ = _rows()
    assert all(r["status"] == "active" and r["state"] for r in rows)


def test_prose_revenue_is_not_a_number():
    it = {"id": "x1", "sgNumber": "SG 99999", "title": "SG 99999 Ohio Gas Station", "assetClass": "Gas Station",
          "askingPrice": 100000, "grossSales": "Over $150,000", "sde": None, "state": "OH"}
    rows, _ = parse_items([it])
    assert rows[0]["revenue"] is None and rows[0]["cash_flow"] is None


def test_duplicate_sg_number_collapses():
    a = {"id": "a", "sgNumber": "SG 90001", "title": "SG 90001 Ohio Gas Station", "assetClass": "Gas Station", "state": "OH"}
    b = dict(a, id="b", sgNumber="sg 90001")
    rows, info = parse_items([a, b])
    assert len(rows) == 1 and info["dupes"] == 1


def test_scope_plain_cases():
    assert scope({"title": "SG 1 Ohio Hotel", "assetClass": "Hotel, Motel"})[0]
    assert not scope({"title": "SG 2 Ohio Apartments Complex", "assetClass": "Apartment Building"})[0]
