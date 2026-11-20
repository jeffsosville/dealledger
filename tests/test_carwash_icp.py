"""carwashbrokers.com via its public ICP canister query (2026-11-13). Offline: a captured reply is the fixture."""
import base64
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import icp_query  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402
from carwashbrokers_scraper import CaffeineIcpListingsScraper  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "carwashbrokers_listlistings.b64")


def _raw():
    return base64.b64decode(open(FIX).read())


def test_decode_all_twenty_listings():
    rows = icp_query.candid_decode(_raw(), CaffeineIcpListingsScraper.FIELD_NAMES)[0]
    assert len(rows) == 20
    r13 = next(r for r in rows if r["id"] == 13)
    assert r13["title"] == "Car Wash FLEX / Full Service" and r13["askingPrice"] == 3600000
    assert r13["state"] == "California" and "fullServiceTunnel" in r13["washType"]


def test_parse_keeps_own_listings_drops_sample_set_and_land():
    out, skipped, n = CaffeineIcpListingsScraper.parse(_raw(), "acct")
    assert n == 20 and len(out) == 7 and skipped == {"stock_photo": 12, "land": 1}
    urls = {l["listing_url"] for l in out}
    assert "https://www.carwashbrokers.com/listings/13" in urls and "https://www.carwashbrokers.com/listings/20" not in urls
    first = next(l for l in out if l["listing_url"].endswith("/listings/13"))
    assert first["price"] == 3600000.0 and first["state"] == "CA" and first["city"] == "Los Angeles"
    # all kept rows are url-keyed (identity = md5(url)), never title/price
    assert len({l["listing_id"] for l in out}) == 7


def test_parse_can_keep_the_sample_set():
    out, skipped, _ = CaffeineIcpListingsScraper.parse(_raw(), "acct", exclude_stock_photos=False)
    assert len(out) == 19 and skipped["stock_photo"] == 0       # only the land parcel is dropped


def test_cbor_roundtrip_and_principal():
    obj = {"content": {"request_type": "query", "arg": b"\x01\x02", "n": 70000, "ok": True}}
    assert icp_query.cbor_dec(icp_query.cbor_enc(obj))[0] == obj
    assert len(icp_query.principal_bytes("vhfrl-xqaaa-aaaao-qqexq-cai")) == 10
    assert icp_query.principal_bytes("ryjl3-tyaaa-aaaaa-aaaba-cai") == bytes([0, 0, 0, 0, 0, 0, 0, 2, 1, 1])


def test_registered_as_specialized_not_generic():
    assert "carwashbrokers.com" in v6.SPECIALIZED_DOMAINS
