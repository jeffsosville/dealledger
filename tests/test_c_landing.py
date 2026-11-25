"""c-landing (2026-11-25): domain keys of the six 'no active rows under own domain' crawls,
and the non-US coastaladvisors.ca removal from the specialized run."""
import csv
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import run_specialized as rs  # noqa: E402


def test_coastaladvisors_not_registered():
    path = os.path.join(os.path.dirname(__file__), "..", "data", "wp_rest_brokers.csv")
    with open(path) as f:
        doms = {(r["domain"] or "").strip() for r in csv.DictReader(f)}
    assert "coastaladvisors.ca" not in doms
    assert "wp_coastaladvisors_ca" not in rs.BROKERS
    # neighbours untouched
    assert "ubcutah.com" in doms and "certifiedbb.com" in doms


def test_bodner_rows_land_on_execbb_not_registry_domain():
    url = "https://www.execbb.com/listingdetail.asp?listingid=44836130SXS4"
    assert rs.derive_broker_domain(url) == "execbb.com"
    assert rs.domain_breakdown([{"listing_url": url}]) == {"execbb.com": 1}
    assert rs.BROKERS["bodner"]["domain"] == "bodnergroup.com"  # run-total key, by design


def test_carwashking_bare_host_and_id_key():
    a = "https://carwashking.com/carwash/1028/car-wash-for-sale/Express-Car-Wash/GA"
    b = "https://carwashking.com/carwash/1028/car-wash-for-sale/Express-Car-Wash-GA"
    assert rs.derive_broker_domain(a) == rs.BROKERS["carwashking"]["domain"] == "carwashking.com"
    assert rs.listing_key(a, "carwashking.com") == rs.listing_key(b, "carwashking.com") == "carwashking:1028"
