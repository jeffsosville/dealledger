"""Single-vertical broker default (T1 2026-11-20)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from broker_vertical import broker_default_vertical, title_vertical, BROKER_DEFAULT_VERTICAL  # noqa: E402


def test_default_applies_when_title_is_silent():
    assert broker_default_vertical("carwashtraders.com", "Wildcatter Wash | Dilley") == "car_wash"
    assert broker_default_vertical("www.petroleumrealty.net", "RIVERSIDE PRIME FREEWAY GAS STATION") == "gas_station"
    assert broker_default_vertical("gasstationsusa.com", "#2394 Palm Beach County") == "gas_station"
    assert broker_default_vertical("https://staxre.com/listings/x/", "Travel Center — Booneville, MS") == "gas_station"


def test_title_that_names_the_default_keeps_it():
    assert broker_default_vertical("carwashtraders.com", "Big Splash Car Wash - Colorado Springs") == "car_wash"
    assert broker_default_vertical("staxre.com", "Mobil Gas Station & C-Store") == "gas_station"


def test_title_that_says_otherwise_wins():
    # a car wash at a gas broker is a car wash; a pet wash title steps aside
    assert broker_default_vertical("thegasbroker.com", "Highland ARCO ampm & Express Car Wash") is None
    assert broker_default_vertical("carwashtraders.com", "Butner's Auto Spa | Truck & Pet Wash") is None
    assert broker_default_vertical("petroleumrealty.net", "Profitable Bookkeeping Business") is None


def test_other_brokers_untouched():
    for d in ("reputedbrokerage.com", "autocenter-sales.com", "tworld.com", "", None,
              "notcarwashtraders.com", "carwashtraders.com.evil.example"):
        assert broker_default_vertical(d, "Express Car Wash") is None


def test_title_vertical_matches_db_rule_order():
    assert title_vertical("Gas Station & Car Wash") == "car_wash"     # car wash first, as in SQL
    assert title_vertical("Liquor Store & Gas Station") == "gas_station"
    assert title_vertical("Turnkey Opportunity") is None


def test_slugs_are_site_slugs():
    assert set(BROKER_DEFAULT_VERTICAL.values()) == {"car_wash", "gas_station"}
