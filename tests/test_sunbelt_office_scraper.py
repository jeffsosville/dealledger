"""sunbeltmidwest.com parser (scrapers/sunbelt_office_scraper.py).

Fixture: 10 of the 125 records of the __NEXT_DATA__ feed served 2026-11-23
(bodies trimmed), plus the page's "125 Total ... Results" line. Prices checked
against the live detail pages the same day.
"""
import copy
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import sunbelt_office_scraper as so  # noqa: E402
from price_disclosure import disclosure_fields  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "sunbeltmidwest_index.html")
D = "sunbeltmidwest.com"
B = "https://sunbeltmidwest.com/buy-a-business/search-businesses-for-sale/"


def _parsed():
    return so.SunbeltOfficeScraper(D).parse(open(FIX, encoding="utf-8").read())


def _by_id():
    return {r["listing_url"].split("/")[-1].split("?")[0]: r for r in _parsed()["rows"]}


def test_count_and_stated_total():
    p = _parsed()
    assert p["stated"] == 125 and p["array_len"] == 10
    assert len(p["rows"]) == 10 and p["dropped"] == []


def test_identity_keeps_prefix_and_trims_stray_space():
    rows = _by_id()
    assert {"59555", "P59694", "P3-2135", "1960430"} <= set(rows)
    assert rows["P59694"]["listing_url"] == B + "P59694?slug=search-businesses-for-sale"


def test_known_prices():
    r = _by_id()
    assert r["59555"]["price"] == 10500000
    assert r["59406"]["price"] == 4000000            # 3,439,500 + 560,500 included
    assert r["58875"]["price"] == 445000             # 175,000 + 270,000 included
    assert r["58092"]["price"] == 99000              # real estate 350,000 NOT included
    assert r["60005"]["price"] == 749000


def test_property_led_row_uses_real_estate_price():
    assert _by_id()["57481"]["price"] == 795000
    assert _by_id()["1960430"]["price"] == 2550000


def test_not_disclosed_rows_carry_the_brokers_words():
    r = _by_id()
    for lid, words in (("58862", "Undisclosed. Will review IOIs."), ("P59694", "Not Disclosed")):
        assert r[lid]["price"] is None and r[lid]["price_text"] == words
        d = disclosure_fields(r[lid])
        assert d["price_not_disclosed"] is True and d["broker_price_text"] == words
    assert disclosure_fields(r["59555"])["price_not_disclosed"] is False


def test_revenue_and_cashflow_not_price():
    r = _by_id()["59555"]
    assert r["revenue"] == 15813973 and r["cash_flow"] == 2601585
    assert _by_id()["58862"]["cash_flow"] == 1300000     # EBITDA fallback


def test_leased_property_is_not_a_status():
    # realEstateStatus "Leased" (60005, 59555) must not drop or alter the row.
    assert _by_id()["60005"]["status"] == "active" and "59555" in _by_id()


def _rec(**kw):
    rec = json.loads(json.dumps(_FIRST))
    rec.update(kw)
    return rec


_FIRST = json.loads(open(FIX, encoding="utf-8").read().split('type="application/json">')[1]
                    .split("</script>")[0])["props"]["pageProps"]["page"]["pageBuilder"][1]["allBusiness"][0]


def test_status_markers_in_title():
    sold = so.to_rows([_rec(listingID="1", slug="1", title="SOLD")], D, "a")["rows"][0]
    assert sold["status"] == "sold" and sold["broker_status_text"] == "SOLD"
    uc = so.to_rows([_rec(listingID="2", slug="2", title="Great Bakery - Under Contract")], D, "a")["rows"][0]
    assert uc["status"] == "active" and uc["broker_status_text"] == "Under Contract"
    assert so.to_rows([_rec(listingID="3", slug="3", title="Coming Soon")], D, "a")["dropped"] == ["3"]


def test_ordinary_titles_untouched():
    for t in ("Sold-out events company with pending expansion", "Pending Permits Dealer"):
        row = so.to_rows([_rec(listingID="9", slug="9", title=t)], D, "a")["rows"][0]
        assert row["status"] == "active" and "broker_status_text" not in row


def test_missing_block_fails_loud():
    html = '<script id="__NEXT_DATA__" type="application/json">{"props":{"pageProps":{"page":{"pageBuilder":[{}]}}}}</script>'
    try:
        so.SunbeltOfficeScraper(D).parse(html)
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_block_found_by_key_not_index():
    nd = {"props": {"pageProps": {"page": {"pageBuilder": [{"allBusiness": [copy.deepcopy(_FIRST)]}, {}]}}}}
    assert len(so.find_all_business(nd)) == 1
