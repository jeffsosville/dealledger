"""Offline checks for the discovery gate in agents/discover_backlog.py.
Run: python3 -m pytest tests/test_discovery_gate.py  (no network, no Supabase)."""
import os, sys, types
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents"))
import discover_backlog as db


class _R:
    def __init__(self, text, url):
        self.text, self.url, self.status_code = text, url, 200


def _ev(html, final="https://acme.com/listings"):
    dv = types.SimpleNamespace(get_page=lambda u, timeout=15: _R(html, final))
    return db.page_evidence(dv, "https://acme.com/listings", "acme.com")


BROKER = " ".join(f"<div>Established HVAC company. Asking $9{i}0,000 Cash flow $2{i}0,000 "
                  f"SDE. Business for sale listing</div>" for i in range(8))
REALTOR = " ".join(f"<div>{i}12 Oak St $45{i},000 3 beds 2 baths 1,800 sq ft MLS #{i} "
                   f"for sale listing</div>" for i in range(8))
CRE = " ".join(f"<div>Retail space for lease $2{i}/SF NNN, 4.{i} acres zoned commercial, "
               f"office for sale $1,{i}00,000 listing</div>" for i in range(8))
RESTAURANT_WITH_RE = " ".join(f"<div>Restaurant with real estate, 2,500 sq ft, asking $5{i}0,000, "
                              f"cash flow $1{i}0,000, business for sale listing</div>" for i in range(8))


def test_broker_auto_promotes():
    assert db.gate_failures(_ev(BROKER), 5, None, None) == []


def test_business_with_real_estate_still_promotes():
    assert db.gate_failures(_ev(RESTAURANT_WITH_RE), 5, None, None) == []


def test_realtor_is_proposed():
    assert any("real estate" in f for f in db.gate_failures(_ev(REALTOR), 5, None, None))


def test_cre_is_proposed():
    assert any("real estate" in f for f in db.gate_failures(_ev(CRE), 5, None, None))


def test_off_domain_is_proposed():
    fails = db.gate_failures(_ev(BROKER, "https://other.com/x"), 5, None, None)
    assert any("leaves the domain" in f for f in fails)


def test_parked_is_proposed():
    fails = db.gate_failures(_ev("<h1>Coming soon</h1> listing for sale $1 $2 $3"), 5, None, None)
    assert any("parked" in f for f in fails)


def test_few_prices_is_proposed():
    fails = db.gate_failures(_ev("listing for sale asking $100,000"), 0, None, None)
    assert any("prices" in f for f in fails)


ASSOC = BROKER + (" <a>Membership Pricing</a> <a>Board of Directors</a> "
                  "<a>Continuing Education</a> <a>TCA Constitution and Bylaws</a>")


def test_association_name_is_proposed():
    fails = db.gate_failures(_ev(BROKER), 5, None, None, domain="chirotexas.org",
                             company="Texas Chiropractic Association classifieds")
    assert any("association" in f for f in fails)


def test_association_page_is_proposed():
    fails = db.gate_failures(_ev(ASSOC), 5, None, None, domain="chirotexas.org")
    assert any("association" in f for f in fails)


def test_broker_named_business_brokers_still_promotes():
    assert db.gate_failures(_ev(BROKER), 5, None, None, domain="acme.com",
                            company="Acme Business Brokers") == []


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for t in tests:
        t()
    print(f"discovery gate: {len(tests)} checks passed")


# 2026-11-03: focuscfo (/blog, 0 parsed) and merceradvisors (tax article) were
# auto-promoted. These paths must now be proposed; real index pages still pass.
BAD_PATHS = ["/blog", "/blog/", "/taxes/selling-real-estate-consider-these-financial-planning-opportunities/",
             "/engagements", "/sba-loans", "/business-concepts-we-teach.html",
             "/alcoholic-beverage-vendors-license-cost-florida", "/consulting-advisory",
             "/tools/inventory-value-estimator/", "/residential-listings",
             "/Personal/Lending-comparison/mortgage-loan-comparison/reo-listings"]
GOOD_PATHS = ["/listings/", "/franchises-for-sale", "/buy-a-business/", "/buyers",
              "/dental-transition/dental-practices-for-sale", "/viewlistings",
              "/jacksonville-business-forSale.asp", "/routes-for-sale/", "/active-listings",
              "/listings.php", "/services/businesses-for-sale/",
              "/home/companies-available-for-mergers-acquisitions-investments"]


def test_content_and_service_paths_are_proposed():
    for p in BAD_PATHS:
        assert db.path_failures(p, p, 0), p



def test_content_paths_fail_even_when_listings_parsed():
    # merceradvisors had 4 items parsed off a tax article.
    for p in ["/blog", "/taxes/selling-real-estate-x/", "/sba-loans", "/residential-listings"]:
        assert db.path_failures(p, p, 10), p


def test_listing_index_paths_pass_even_with_nothing_parsed():
    for p in GOOD_PATHS:
        assert db.path_failures(p, p, 0) == [], p


def test_trailing_slash_does_not_matter():
    assert db.path_failures("/blog/", "/blog", 0)


def test_past_deals_pages_are_proposed_even_with_listings_parsed():
    # syedbrokerage.capital: 40 items parsed off /performance (past deals).
    for p in ["/performance", "/track-record/", "/closed-deals", "/sold", "/recently-sold/",
              "/recent-transactions", "/tombstones"]:
        assert db.path_failures(p, p, 40), p


def test_sold_in_a_listings_slug_is_not_a_past_deals_page():
    for p in ["/listings/", "/businesses-for-sale/", "/soldier-field-cafe-for-sale"]:
        assert db.path_failures(p, p, 40) == [], p
