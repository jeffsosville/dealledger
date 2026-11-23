import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scrapers"))
import dealledger_scraper_v6 as v6


def read(text):
    return v6.labelled_asking_price(text, skip_qualified=True, allow_purchase=True)


def test_ctc_purchase_price_is_the_ask():
    assert read("Sale, Seller Walk Away Purchase Price $2,156,100 Gross Income ~$2.5M") == 2156100


def test_ctc_partial_stake_is_not_the_ask():
    assert read("Partnership Purchase Price $539,000 (1/4 partnership for sale) Gross Income ~$3,400,000") is None


def test_ctc_no_price_published():
    assert read("Sale, Seller Walk Away Gross Income $2.5M+ Medicaid") is None


def test_purchase_stays_skipped_for_other_brokers():
    assert v6.labelled_asking_price("Purchase Price $2,156,100", skip_qualified=True) is None


def test_other_qualifiers_stay_skipped_for_ctc():
    assert read("Equipment Price $100,000 Purchase Price $900,000") == 900000


def test_revenue_only_brokers_have_no_price():
    assert v6.labelled_asking_price("Home care agency $65M+ in revenue") is None
    assert v6.labelled_asking_price("Gross Revenue : $2.16M Exam Rooms : 4") is None


def test_domains_are_opted_in():
    assert {"stoneridgepartners.com", "psbroker.com", "ctc-associates.com"} <= v6.LABELLED_PRICE_DOMAINS
    assert v6.PURCHASE_PRICE_DOMAINS == {"ctc-associates.com"}
