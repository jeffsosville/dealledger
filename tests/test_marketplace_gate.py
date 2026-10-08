"""Offline check: a broker card that links out to a marketplace listing is dropped.
Run: python3 tests/test_marketplace_gate.py  (no network, no Supabase)."""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
sys.argv = sys.argv[:1]
import dealledger_scraper_v6 as v6


def test_marketplace_urls():
    assert v6.is_marketplace_url("https://www.bizbuysell.com/business-opportunity/bar-pub/2462149/")
    assert v6.is_marketplace_url("https://us.businessesforsale.com/us/carwash.aspx")
    assert v6.is_marketplace_url("http://www.loopnet.com/Listing/14351098/")
    # Real brokers whose domains merely contain a marketplace word stay in.
    assert not v6.is_marketplace_url("https://businessesforsalehamptonroads.com/listing/1")
    assert not v6.is_marketplace_url("https://gulfcoastbizbuysell.com/a")
    assert not v6.is_marketplace_url("https://papadop.com/listings/bar-pub")


if __name__ == "__main__":
    test_marketplace_urls()
    print("marketplace gate: ok")
