"""Transworld + We Sell Restaurants listing status (job 3, decisions 5 + 6).

Transworld: /api/listings carries no status field, so the status is a MARKER
the office writes into the heading. We Sell Restaurants: the feed's
`bstatuslist` field, plus For Lease (category id 96 / a "for lease" headline)
not emitted. Principle 8: rows with no status signal must come out exactly as
before (status 'active', no label, same URL, same listing_key).
No network: the scrapers' page fetchers are replaced with canned feed records.
"""
import hashlib
import importlib
import sys

import pytest

import specialized_scrapers as ss
from specialized_scrapers import (STATUS_DROP, TransworldScraper,
                                  WeSellRestaurantsScraper, title_status_marker)

try:
    import listing_status  # noqa: F401
    HAVE_MODULE = True
except Exception:
    HAVE_MODULE = False


# ── title markers (Transworld headings, verbatim from the 11/12 feed) ──────
@pytest.mark.parametrize("heading,label", [
    ("UNDER CONTRACT: Construction Management Service in TriState", "UNDER CONTRACT"),
    ("UNDER CONTRACT Senior Home Health Care", "UNDER CONTRACT"),
    ("UNDER CONTRACT-Window Coverings and Outdoor Living Products", "UNDER CONTRACT"),
    ("UNDER CONTRACT 35-yr. Chimney Fireplace Sales Service Repair Biz", "UNDER CONTRACT"),
    ("Commercial Plumbing Co w/ Strong Profit Margins - UNDER CONTRACT", "UNDER CONTRACT"),
    ("(UNDER CONTRACT) Commercial Facility Services Business IL", "UNDER CONTRACT"),
    ("Cloud & AI Services Platform | Under LOI", "Under LOI"),
    ("Pending: Pool Route in Phoenix", "Pending"),
    ("SALE PENDING - Dental Lab", "SALE PENDING"),
    ("Coffee Shop (Under Contract)", "Under Contract"),
    ("*Under LOI* Starbucks WPS Coffee Shop: Semi-Absentee", "Under LOI"),
    ("UNDER CONTRACT -ACCEPTING OFFERS Home Energy Efficiency Business!", "UNDER CONTRACT"),
])
def test_under_contract_markers_stay_active_with_label(heading, label):
    assert title_status_marker(heading) == ("active", label)


@pytest.mark.parametrize("heading", [
    "SOLD: Profitable HVAC Company",
    "Profitable HVAC Company - SOLD",
    "[Sold] Profitable HVAC Company",
])
def test_sold_markers(heading):
    st, label = title_status_marker(heading)
    assert st == "sold" and label.lower() == "sold"


def test_coming_soon_marker_is_drop():
    assert title_status_marker("COMING SOON: Landscaping Company")[0] == STATUS_DROP


# Principle 8: real titles that contain the words but carry no status.
@pytest.mark.parametrize("heading", [
    "Master Licensor Bagel Operation! 20 Stores! $80 Mill Bagels Sold!",
    "Turnkey HVAC & Sheet Metal Contractor with $5M Contract Pipeline",
    "DFW Contract Logistics Operator – Recurring Delivery Model",
    "Turn Key Electronics Contract Manufacture with Huge Potential",
    "NC's Premier Catering Brand, Sold With Real Estate, Mobile Fleet",
    "SoCal High End Cabinet Business with Simulation Platform Contract",
    "Asset Sale!! Fayette County Beauty Supply Sold at Inventory Cost",
    "Insulation Contractor Significant Growth, Res & Comm Contracts",
    "Closed QSR for Sale in Newnan, GA — Highly Motivated Seller!",
    "Pending Patent Medical Device Company",
    "Pending Indoor Baseball & Softball Training Platform",   # ambiguous: left active
    "Turnkey Manhattan Wine & Spirits Shop – Long-Term Lease",
    "Lease Takeover Opportunity: 7-Fig Fast-Cas BBQ | Open or Rebrand",
    "Established Thai Restaurant | $135K SDE | Lease Through 2031",
    "CLOSED-LOOP RECYCLING COMPANY",
    "UNDER CONTRACT MANUFACTURING SERVICES",      # all-caps title: not a marker
    "MAKE OFFERS! - Greek Restaurant & Bar - Liquor License Included",
    "Highly Profitable - Electrical Contractor - Over 20 years!!",
    "",
    None,
])
def test_no_marker_no_status(heading):
    assert title_status_marker(heading) is None
    assert TransworldScraper.heading_status(heading) == ("active", None)


# ── Transworld scrape(): canned feed, no network ───────────────────────────
TW_ITEMS = [
    {"heading": "Family-Owned Plumbing Contractor", "slug": "Family-Owned-Plumbing-Contractor",
     "categories": '["Plumbing"]', "location": "Dallas, TX", "price": 950000,
     "seller_discretionary_earnings": 310000},
    {"heading": "UNDER CONTRACT: Insurance Consulting! 80% Recurring Revenue! ",
     "slug": "UNDER-CONTRACT:-Insurance-Consulting-80-Recurring-", "categories": None,
     "location": "Tampa, FL", "price": 400000, "seller_discretionary_earnings": 0},
    {"heading": "SOLD - Pizza Shop", "slug": "SOLD-Pizza-Shop", "categories": None,
     "location": "Austin, TX", "price": 0, "seller_discretionary_earnings": 0},
    {"heading": "COMING SOON: Car Wash", "slug": "COMING-SOON-Car-Wash", "categories": None,
     "location": "Mesa, AZ", "price": 0, "seller_discretionary_earnings": 0},
]


def _tw_scrape(items):
    s = TransworldScraper.__new__(TransworldScraper)      # no bootstrap (network)
    s._fetch_page = lambda page: list(items) if page == 1 else []
    return s.scrape("28148", max_pages=1, verbose=True)


def test_transworld_statuses_and_drop(capsys):
    out = _tw_scrape(TW_ITEMS)
    by_title = {l["title"]: l for l in out}
    assert len(out) == 3                                   # Coming Soon not emitted
    assert "COMING SOON: Car Wash" not in by_title
    uc = by_title["UNDER CONTRACT: Insurance Consulting! 80% Recurring Revenue! "]
    assert uc["status"] == "active" and uc["broker_status_text"] == "UNDER CONTRACT"
    assert uc["status_source"] == "title_marker"
    sold = by_title["SOLD - Pizza Shop"]
    assert sold["status"] == "sold"
    log = capsys.readouterr().out
    assert "status DROP: 1 rows not emitted" in log


def test_transworld_unmarked_row_identical_to_before():
    plain = _tw_scrape(TW_ITEMS[:1])[0]
    url = "https://www.tworld.com/agents/aaronbrownlee/listings/family-owned-plumbing-contractor"
    expected = ss.format_listing(
        url=url, broker_account="28148", title="Family-Owned Plumbing Contractor",
        business_type="Plumbing", price=950000.0, price_text="$950,000",
        location="Dallas, TX", city="Dallas", state="TX", cash_flow=310000.0)
    assert plain == expected                       # status 'active', no new keys
    assert "broker_status_text" not in plain
    assert plain["listing_id"] == hashlib.md5(url.encode()).hexdigest()


def test_transworld_marker_does_not_change_url_or_title():
    out = {l["title"]: l for l in _tw_scrape(TW_ITEMS)}
    uc = out["UNDER CONTRACT: Insurance Consulting! 80% Recurring Revenue! "]
    assert uc["listing_url"] == ("https://www.tworld.com/agents/aaronbrownlee/listings/"
                                 "under-contract:-insurance-consulting-80-recurring-")


# ── We Sell Restaurants ────────────────────────────────────────────────────
def _wsr(id_, status="Available", head="Turnkey Pizza Restaurant for Sale", cat=None,
         price=150000):
    return {"id": id_, "bheadlinead": head, "bcity": "Charlotte", "bstate": "NC",
            "bsaleprice": price, "bstatuslist": status, "slug_url": f"slug-{id_}",
            "listing_category": cat, "listing_bat": None}


@pytest.mark.parametrize("raw,expected", [
    ("Available", ("active", None)),
    ("Available not Advertised", ("active", None)),   # not a phrase: active, no label
    ("In Contract", ("active", "In Contract")),        # feed alias of under contract
    ("LOI", ("active", "LOI")),
    ("", ("active", None)),
    (None, ("active", None)),
    ("Sold", ("sold", "Sold")),
])
def test_wsr_feed_status(raw, expected):
    assert WeSellRestaurantsScraper.record_status(_wsr(1, status=raw)) == expected


@pytest.mark.parametrize("rec,label", [
    (_wsr(2, "Available not Advertised", "Pineville Restaurant for Lease - Drive-Thru!",
          {"id": 96, "name": "Restaurant for Lease"}, 0), "Restaurant for Lease"),
    (_wsr(3, "Available not Advertised", "Tampa 4,114 SF Restaurant for Lease, 10 Year Term",
          None, None), "for Lease"),
    (_wsr(4, "LOI", "Bring Own Concept!! Huge Restaurant for lease!", None, 0), "for lease"),
    # category 96 decides even without the words in the headline
    (_wsr(5, "Available", "Hills City Center Opportunity", {"id": 96, "name": "Restaurant for Lease"},
          0), "Restaurant for Lease"),
])
def test_wsr_for_lease_is_drop(rec, label):
    assert WeSellRestaurantsScraper.record_status(rec) == (STATUS_DROP, label)


@pytest.mark.parametrize("head", [
    "Restaurant Building for Sale or Lease!",
    "Restaurant for Sale or Lease! Owner Will Remodel to Suit!",
    "Turnkey Restaurant Opportunity with Long-Term Lease in Ellicott City",
    "Profitable Lake Mary Pizza Shop for Sale with Long Lease!",
    "Poke Restaurant in Marietta Simply take over lease. Keep or Convert.",
    "Closed QSR for Sale in Newnan, GA — Highly Motivated Seller!",
    "Cafe for Sale in Charlotte: Turn-Key, Low Rent, High Traffic!",
])
def test_wsr_sale_listings_mentioning_lease_stay(head):
    # btype id 96 alone is not used: real sales carry it too
    rec = dict(_wsr(6, "Available", head), btype="339,97,104,96")
    assert WeSellRestaurantsScraper.record_status(rec) == ("active", None)


def _wsr_scrape(records, capsys=None):
    s = WeSellRestaurantsScraper.__new__(WeSellRestaurantsScraper)
    page = {"data": records, "last_page": 1, "total": len(records), "per_page": 51}
    s._fetch_page = lambda n: page if n == 1 else None
    return s.scrape("2900", max_pages=1, verbose=True)


def test_wsr_scrape_counts_and_labels(capsys):
    recs = [_wsr(10), _wsr(11, "In Contract"), _wsr(12, "LOI"),
            _wsr(13, "Available not Advertised", "Miami Restaurant Space for Lease, 4,100 SF!", None, 0),
            _wsr(14, "Available not Advertised", "Sarasota Restaurant Space for Lease", None, 0)]
    out = _wsr_scrape(recs)
    ids = [l["listing_url"].rsplit("/", 1)[1] for l in out]
    assert ids == ["10", "11", "12"]
    assert [l["status"] for l in out] == ["active"] * 3
    assert [l.get("broker_status_text") for l in out] == [None, "In Contract", "LOI"]
    log = capsys.readouterr().out
    assert "status DROP: 2 rows not emitted (for Lease: 2)" in log
    assert "feed_total=5" in log


def test_wsr_unmarked_row_identical_to_before():
    out = _wsr_scrape([_wsr(20)])[0]
    url = "https://www.wesellrestaurants.com/restaurant-for-sale/slug-20/20"
    assert out == ss.format_listing(
        url=url, broker_account="2900", title="Turnkey Pizza Restaurant for Sale",
        price=150000.0, location="Charlotte, NC", city="Charlotte", state="NC")


# ── writer: listing_key / ids unchanged; DROP never written; sold explicit ──
def test_listing_key_unchanged_for_both_domains():
    import run_specialized as rs
    for url in ("https://www.tworld.com/agents/aaronbrownlee/listings/x-y",
                "https://www.wesellrestaurants.com/restaurant-for-sale/slug-20/20"):
        assert rs.listing_key(url, rs.derive_broker_domain(url)) == url


class _Resp:
    def __init__(self, code=201, body=None):
        self.status_code, self._body, self.text, self.ok = code, body or [], "", True

    def json(self):
        return self._body


def test_writer_drop_guard_and_explicit_sold(monkeypatch):
    import run_specialized as rs
    posts = []
    existing = {}

    class FakeHTTP:
        @staticmethod
        def get(url, headers=None, params=None, timeout=None):
            return _Resp(200, [{"id": i} for i in existing])

        @staticmethod
        def post(url, headers=None, json=None, timeout=None):
            posts.append(json)
            return _Resp(201)

    monkeypatch.setattr(rs, "http_requests", FakeHTTP)
    base = "https://www.tworld.com/agents/aaronbrownlee/listings/"
    rows = [ss.format_listing(url=base + "a", broker_account="28148", title="Plumbing Contractor Dallas"),
            dict(ss.format_listing(url=base + "b", broker_account="28148",
                                   title="UNDER CONTRACT: Insurance Consulting"),
                 broker_status_text="UNDER CONTRACT"),
            ss.format_listing(url=base + "c", broker_account="28148", title="SOLD - Pizza Shop Austin",
                              status="sold"),
            ss.format_listing(url=base + "d", broker_account="28148", title="Car Wash Mesa Arizona",
                              status="DROP")]
    for r in rows:
        existing[f"spec:{hashlib.md5(r['listing_url'].encode()).hexdigest()[:16]}"] = 1
    n = rs.upsert_listings(rows, display_name="Transworld Business Advisors")
    sent = [r for batch in posts for r in batch]
    assert n == 3 and len(sent) == 3
    assert all(r.get("status") != "DROP" for r in sent)
    by_url = {r["url"]: r for r in sent}
    assert by_url[base + "c"]["status"] == "sold"          # explicit -> sent
    assert "status" not in by_url[base + "a"]              # existing active row keeps its status
    assert "status" not in by_url[base + "b"]              # under contract: active, not explicit


# ── without the shared module: the local fallback gives the same answers ────
def test_fallback_without_module(monkeypatch):
    monkeypatch.setitem(sys.modules, "listing_status", None)   # import -> ImportError
    fresh = importlib.reload(ss)
    try:
        assert fresh._st_lookup is None
        assert fresh.title_status_marker("UNDER CONTRACT: Senior Home Care") == ("active", "UNDER CONTRACT")
        assert fresh.title_status_marker("Contractor with $5M Contract Pipeline") is None
        assert fresh.WeSellRestaurantsScraper.record_status(_wsr(1, "In Contract")) == ("active", "In Contract")
        assert fresh.WeSellRestaurantsScraper.record_status(
            _wsr(2, "Available", "Restaurant Space for Lease")) == (fresh.STATUS_DROP, "for Lease")
        assert fresh.WeSellRestaurantsScraper.record_status(_wsr(3)) == ("active", None)
    finally:
        monkeypatch.undo()
        importlib.reload(ss)
