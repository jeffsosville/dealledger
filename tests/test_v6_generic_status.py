"""V6 generic card status through scrapers/listing_status.py (job 3, 2026-11-12).

Decision 6: Under Contract / Sale Pending / Pending / Under LOI / Off Market are
ACTIVE with the broker's words kept (broker_status_text); only SOLD is sold.
Decision 5: Coming Soon / For Lease are not emitted. Decision 11: the any-case
"Sold!" title rule stays limited to TITLE_STATUS_MARKER_DOMAINS.
Sample titles are real rows from data/snapshots/2026-11-11/listings.json
(j3-v6status-measure.csv)."""
import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import (  # noqa: E402
    ListingExtractor, badge_status, card_status, restatus_from_title, title_status)

pytest.importorskip("listing_status")

DROP = v6._LS_DROP


# ── title markers: the under-contract family flips sold -> active + label ────
@pytest.mark.parametrize("title,label", [
    ("IN CONTRACT - SALE PENDING ! Granite, Marble, Quartz Kitchen Fabricating", "SALE PENDING"),
    ("PENNSYLVANIA SKC-1562 UNDER CONTRACT", "UNDER CONTRACT"),
    ("TEXAS SBA-2381 UNDER CONTRACT", "UNDER CONTRACT"),
    ("Aluminum Fabrication and Installation Business - Under Contract", "Under Contract"),
    ("New York, NY ATM Route – $439,000.00 – Under Contract", "Under Contract"),
    ("Under contract-65 Year Old Profitable Quality Bakery", "Under contract"),
    ("**UNDER CONTRACT**Metro Atlanta Provider of IDD, Community Living", "UNDER CONTRACT"),
    ("UNDER CONTRACT…Established Specialty Products Business", "UNDER CONTRACT"),
    ("Under Contract! Turnkey Pet Resort in Booming Austin Market", "Under Contract"),
    ("Very Profitable Home Health Care (Under Contract)", "Under Contract"),
    ("Sale Pending: High Country Candles — Established Hand-Poured Candle Business", "Sale Pending"),
    ("SALE PENDING!!! \"C33\" Liquor License, OPPORTUNITY!  Bloomfield, NJ", "SALE PENDING"),
    ("LD & HD Towco in Canada (Sale Pending)", "Sale Pending"),
    ("10 FedEx Ground P&D Routes - Tupelo, MS - SALE PENDING", "SALE PENDING"),
    ("Public and Private Sector IT Consulting Company for Sale MN SALE PENDING", "SALE PENDING"),
    ("UNDER LOI -Home Care Agency (LHCSA) – NYC & Westchester", "UNDER LOI"),
    ("Glass Tempering and Fabrication -(LOI)", "LOI"),
    ("Florist – Profitable, Established High-Volume $189,000 - Off Market", "Off Market"),
    ("Hip Fitness Trend Flagship Location $1399,000.00 / Off Market", "Off Market"),
    ("Colorado B2B Manufacturing Business (Temporarily Off Market)", "Temporarily Off Market"),
    ("Established Juice Company  TEMPORARILY OFF MARKET", "TEMPORARILY OFF MARKET"),
    ("Restaurant -30 years in Business Temporarily Off Market Turnkey KEEP or CONVERT",
     "Temporarily Off Market"),
    ("Contract Pending-Media & Advertising Business – $617K SDE – Home-Based", "Contract Pending"),
    ("UNDER AGREEMENT! CNC Machine Shop #15720", "UNDER AGREEMENT"),
    ("Lucrative Industrial Construction Company (Pending)", "Pending"),
    ("PENDING - HVAC & Refrigeration Company + Real Estate! - Michigan Business Broker", "PENDING"),
    ("Under LOI · No longer accepting bids", "Under LOI"),
])
def test_under_contract_family_is_active_with_verbatim_label(title, label):
    assert title_status(title, "example-broker.com") == ("active", label)


# ── SOLD stays sold (HEAD's rule: all caps, "(sold …)", "– Sold" suffix) ─────
@pytest.mark.parametrize("title", [
    "Red Wing Resort (SOLD)",
    "WigWam Resort on Lake Of The Woods (SOLD)",
    "SOLD - Turnkey Dental Laboratory | California",
    "Route for Sale Arizona – Sierra Vista *SOLD*",
    "Sacramento Pastry Route for Sale – SOLD",
    "SOLD - Established Manufacturing & Fabrication Company",
    "Under Contract/ Landmark West Hftd Pizzeria (SOLD)",       # most final wins
    "Diner on Main – sold",
])
def test_sold_stays_sold(title):
    st = title_status(title, "example-broker.com")
    assert st is not None and st[0] == "sold"


# ── decision 11: any-case leading/trailing "Sold!" only at the allowlist ─────
ANYCASE = [
    "Sold! Distribution of unconventional gift products",
    "Sold: Retail Pharmacy in Miami Florida",
    "Sold…Law firm For Sale",
    "Sold ….Lighting Business Ease of Operation",
    "Chicago ATM Route, $210,000 – Sold!",
    "Delray Beach Commercial Retail Development Site  – Sold for $4,500,000 $ 4,500,000.00",
]


@pytest.mark.parametrize("title", ANYCASE)
def test_anycase_sold_off_for_other_brokers(title):
    assert title_status(title, "montana406businessbrokers.com") is None


@pytest.mark.parametrize("title", ANYCASE)
def test_anycase_sold_switch(title, monkeypatch):
    monkeypatch.setattr(v6, "ANYCASE_SOLD_MARKERS", True)
    assert title_status(title, "montana406businessbrokers.com")[0] == "sold"


def test_anycase_sold_on_for_allowlisted_domain():
    assert title_status("Sold! internal medicine", "www.healthcarebizsales.com")[0] == "sold"


# ── decision 5: Coming Soon / For Lease markers -> DROP ─────────────────────
@pytest.mark.parametrize("title,label", [
    ("Coming Soon - Established Profitable Commercial Cleaning Business", "Coming Soon"),
    ("Legacy Car Wash | Coming Soon", "Coming Soon"),
    ("LEASED – LowDown Brewery + Kitchen", "LEASED"),
    ("Former Mellow Mushroom – Delray Beach – LEASED", "LEASED"),
    ("Restaurant Space – For Lease", "For Lease"),
])
def test_drop_markers(title, label):
    assert title_status(title, "example-broker.com") == (DROP, label)


# ── not markers: ordinary words in a title (principle 8: must pass) ─────────
@pytest.mark.parametrize("title", [
    "Closed Gas Station Opportunity In Tyler, TX",
    "40+ Year LI Remodeling Company | $1.18M Revenue | $330K Pending Project | Owner Retiring",
    "Recently Closed Suffolk Supermarket - $1.5MM Build Out - 23K Sq Ft - New Lease",
    "Off Sale Liquor Store - South Metro - Pending Offer",
    "Fast Casual North County Healthy Restaurant Sold As-Is or Convert it",
    "Two aerospace and defense precision shops, sold together with their building",
    "Montessori School Sold",
    "Retail Income Below. 100% Leased Ground Floor | $550,000",
    "Pawn Shop Business-only near McDonough, GA with $132K Yearly Net Profit! Closed Sundays!",
    "Soil Testing Business Coming Soon Price $0.00",
    "Commercial Real Estate For Lease in Florida",
    "Atlanta GA Food Hall for Sale – Multi Million Dollar Buildout – Fully Leased – Keep or Convert",
    "Vietnamese Restaurant Greater Corpus Christi Area Closed Weekends For Sale",
    "Between 300 to 600 transactions take place in our state. From the largest deal in 2025 "
    "of the landmark Crystal sold for $110 Million to The Mom & Pop Cafe for $50,000",
    "KOA Journey Bemidji",
    "Patent Pending Kitchen Gadget Brand",
    "",
])
def test_not_a_marker(title):
    assert title_status(title, "example-broker.com") is None


# ── badges: looks_sold's detection, mapped through the module ───────────────
def _el(html):
    return BeautifulSoup(html, "html.parser").find("div")


@pytest.mark.parametrize("html,want", [
    ('<div><span class="badge">Under Contract</span></div>', ("active", "Under Contract")),
    ('<div><span class="status-label">SALE PENDING</span></div>', ("active", "SALE PENDING")),
    ('<div><span class="ribbon">Off Market</span></div>', ("active", "Off Market")),
    ('<div><span class="badge">SOLD</span></div>', ("sold", "SOLD")),
    ('<div class="listing sold"><p>x</p></div>', ("sold", "sold")),
    ('<div class="listing under-contract"><p>x</p></div>', ("active", "under-contract")),
    ('<div><img alt="Sold" src="x.png"></div>', ("sold", "Sold")),
    ('<div><span class="badge">Sold</span><span class="label">Under Contract</span></div>',
     ("sold", "Sold")),
    ('<div><span class="badge">Featured</span></div>', None),
])
def test_badge_status(html, want):
    assert badge_status(_el(html), "example-broker.com") == want


def test_card_status_badge_beats_title_when_more_final():
    el = _el('<div><span class="badge">SOLD</span></div>')
    assert card_status(el, "Pizza Shop - Under Contract", "example-broker.com")[0] == "sold"
    el = _el('<div><span class="badge">Under Contract</span></div>')
    assert card_status(el, "Pizza Shop", "example-broker.com") == ("active", "Under Contract")


def test_card_status_allowlisted_title_marker_decides_alone():
    el = _el('<div><span class="badge">SOLD</span></div>')
    assert card_status(el, "UNDER CONTRACT: Pharmacy", "healthcarebizsales.com") == (
        "active", "UNDER CONTRACT:")


@pytest.mark.parametrize("title", ["Under Contract Listings", "OFF-Market Listing"])
def test_status_heading_keeps_todays_status(title):
    # a section heading scraped as a card must not become an active listing
    el = _el("<div><p>x</p></div>")
    want = "sold" if v6.is_sold_or_pending(title) else "active"
    assert card_status(el, title, "example-broker.com") == (want, None)


def test_legacy_rule_without_module(monkeypatch):
    monkeypatch.setattr(v6, "_ls_lookup", None)
    el = _el('<div><span class="badge">Under Contract</span></div>')
    assert card_status(el, "Pizza Shop", "example-broker.com") == ("sold", None)
    assert card_status(_el("<div></div>"), "Pizza - SALE PENDING", "x.com") == ("sold", None)
    assert card_status(_el("<div></div>"), "Pizza Shop", "x.com") == ("active", None)


# ── from_card end to end ────────────────────────────────────────────────────
BASE = "https://www.example-broker.com/listings/"


def _card(title, badge="", slug="pizza-shop-123"):
    return BeautifulSoup(f"""
<div class="listing-card">
  <h3><a href="https://www.example-broker.com/listing/{slug}">{title}</a></h3>
  {badge}
  <p>Asking Price: $250,000 Cash Flow: $80,000 Location: Tampa, FL.
     Established restaurant with loyal customers and trained staff.</p>
</div>""", "html.parser").find("div")


def test_from_card_under_contract_title_is_active_with_label():
    ListingExtractor.status_drops = []
    r = ListingExtractor.from_card(_card("Pizza Restaurant in Tampa - Under Contract"),
                                   BASE, "Example Broker")
    assert r["status"] == "active" and r["broker_status_text"] == "Under Contract"


def test_from_card_sold_badge_is_sold():
    r = ListingExtractor.from_card(
        _card("Pizza Restaurant in Tampa", '<span class="badge">SOLD</span>'), BASE, "Example Broker")
    assert r["status"] == "sold"


def test_from_card_plain_card_unchanged():
    r = ListingExtractor.from_card(_card("Pizza Restaurant in Tampa"), BASE, "Example Broker")
    assert r["status"] == "active" and "broker_status_text" not in r


def test_from_card_coming_soon_not_emitted_and_counted():
    ListingExtractor.status_drops = []
    r = ListingExtractor.from_card(
        _card("Coming Soon - Established Commercial Cleaning Business", slug="cs-1"),
        BASE, "Example Broker")
    assert r is None
    assert ListingExtractor.status_drops == [
        ("Coming Soon - Established Commercial Cleaning Business", "Coming Soon")]


# ── restatus after a title replacement (mnresortsalesnorth.com) ─────────────
def test_restatus_mnresort_card_title_replaced_by_detail_h1():
    row = {"title": "Red Wing Resort (SOLD)", "status": "active",
           "broker_domain": "mnresortsalesnorth.com"}
    assert restatus_from_title(row)["status"] == "sold"
    assert row["broker_status_text"] == "SOLD"


def test_restatus_never_reopens_sold_or_drops():
    row = {"title": "Pizza - Under Contract", "status": "sold", "broker_domain": "x.com"}
    assert restatus_from_title(row) == {"title": "Pizza - Under Contract", "status": "sold",
                                        "broker_domain": "x.com"}
    row = {"title": "Car Wash | Coming Soon", "status": "active", "broker_domain": "x.com"}
    assert restatus_from_title(row)["status"] == "active"


def test_restatus_labels_active_under_contract():
    row = {"title": "OFFICE EQUIPMENT SALES & MAINTENANCE (UNDER CONTRACT)",
           "status": "active", "broker_domain": "excellenssolutions.com"}
    restatus_from_title(row)
    assert row["status"] == "active" and row["broker_status_text"] == "UNDER CONTRACT"


def test_restatus_respects_decision_11():
    row = {"title": "Sold! Distribution of unconventional gift products", "status": "active",
           "broker_domain": "oibmn.com"}
    assert restatus_from_title(row)["status"] == "active"


def test_restatus_legacy_without_module(monkeypatch):
    monkeypatch.setattr(v6, "_ls_lookup", None)
    row = {"title": "Red Wing Resort (SOLD)", "status": "active"}
    assert restatus_from_title(row)["status"] == "sold"


# ── composition with j3-lbaweb_status_field (card_status_field) ─────────────
def _field_card(title, value, slug="field-1"):
    return BeautifulSoup(f"""
<div class="listing-card">
  <h3><a href="https://www.example-broker.com/listing/{slug}">{title}</a></h3>
  <span class="tag">Status: <em>{value}</em></span>
  <p>Asking Price: $250,000 Cash Flow: $80,000 Location: Tampa, FL.
     Established restaurant with loyal customers and trained staff.</p>
</div>""", "html.parser").find("div")


def test_status_field_beats_title_marker():
    r = ListingExtractor.from_card(_field_card("Pizza Restaurant in Tampa (SOLD)", "Under Contract"),
                                   BASE, "Example Broker")
    assert r["status"] == "active" and r["broker_status_text"] == "Under Contract"
    assert r["_status_from_field"] is True
    r = ListingExtractor.from_card(_field_card("Pizza Restaurant - Under Contract", "Sold", "f2"),
                                   BASE, "Example Broker")
    assert r["status"] == "sold" and r["broker_status_text"] == "Sold"


def test_plain_active_field_adds_nothing():
    r = ListingExtractor.from_card(_field_card("Pizza Restaurant - SALE PENDING", "Available", "f3"),
                                   BASE, "Example Broker")
    assert r["status"] == "active" and r["broker_status_text"] == "SALE PENDING"
    assert "_status_from_field" not in r


def test_restatus_skips_field_decided_row():
    row = {"title": "Red Wing Resort (SOLD)", "status": "active", "broker_status_text": "Pending",
           "_status_from_field": True, "broker_domain": "x.com"}
    assert restatus_from_title(row)["status"] == "active"
    assert row["broker_status_text"] == "Pending"
