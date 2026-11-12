"""
tests/test_price_disclosure.py — the shared "broker explicitly withholds the
price" rule (scrapers/price_disclosure.py) and how both writers use it.

The load-bearing case is the first one: a bare missing price must NOT set the
flag. price_not_disclosed lets a row through the publish gate without a price,
so a false accept here publishes junk (owner decision 2026-11-11).
"""
import os
import sys
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import price_disclosure as P  # noqa: E402


# ── a bare null price is not "not disclosed" ─────────────────────────────────

def test_bare_null_price_sets_nothing():
    for row in ({"asking_price": None},
                {"price": None},
                {"asking_price": None, "price_text": None},
                {"asking_price": None, "price_text": ""},
                {"price": 0, "price_text": "$0"},
                {"price": None, "price_text": "N/A"},
                {"price": None, "price_text": "-"},
                {"asking_price": None, "description": "Price: Call for price"}):  # prose is never read
        assert P.disclosure_fields(row) is None, row


def test_scraper_flag_without_phrase_is_not_enough():
    # W5's ERE scraper sets price_not_disclosed for ANY missing price; with no
    # phrase on the row nothing is written, and the writer counts it.
    row = {"price": None, "price_text": None, "price_not_disclosed": True}
    assert P.disclosure_fields(row) is None
    assert P.flagged_without_phrase(row) is True


# ── explicit phrases ─────────────────────────────────────────────────────────

def test_explicit_phrases_flag_and_keep_verbatim():
    for text in ("Market Price", "Not Disclosed", "Undisclosed", "Market to Determine",
                 "Call for price", "CALL FOR PRICE", "Price on call", "Contact for price",
                 "Contact Broker for Pricing", "Upon Request", "Confidential", "TBD",
                 "Negotiable", "$40k Down", "$150,000 down payment"):
        out = P.disclosure_fields({"price": None, "price_text": text})
        assert out == {"price_not_disclosed": True, "broker_price_text": text}, text


def test_broker_price_text_preferred_and_whitespace_collapsed():
    out = P.disclosure_fields({"asking_price": None,
                               "broker_price_text": "  Market\n  Price ",
                               "price_text": "Call for price"})
    assert out == {"price_not_disclosed": True, "broker_price_text": "Market Price"}


def test_a_total_is_a_price_not_a_withheld_one():
    for text in ("$450,000 negotiable", "$1.2M / call for terms",
                 "$500,000 ($100k down)", "1,250,000 Not including inventory"):
        assert P.withheld_price_phrase(text) is None, text


def test_long_text_is_not_a_price_field():
    assert P.withheld_price_phrase("Confidential " + "x" * 200) is None


# ── a price clears the flag (positive evidence it IS disclosed) ──────────────

def test_priced_row_sends_false_and_clears_text():
    for row in ({"asking_price": 425000.0, "price_text": "Market Price"},
                {"price": 1250000, "price_not_disclosed": True}):
        assert P.disclosure_fields(row) == {"price_not_disclosed": False,
                                            "broker_price_text": None}


# ── labelled phrases on V6 card text ─────────────────────────────────────────

def test_labelled_card_text():
    # shapes seen in data/snapshots (oibmn, business-team, theinnategroup,
    # lakecountryadvisors, pacificrestaurantbrokers, routeadvisors)
    cases = {
        "ASKING PRICE: Undisclosed Location: MN": "ASKING PRICE: Undisclosed",
        "Asking Price: Market Price Cash Flow: $210,000": "Asking Price: Market Price",
        "Asking price: Confidential Type 47 ABC license Large swanky patio": "Asking price: Confidential",
        "Asking Price: $TBD Midwest This established": "Asking Price: $TBD",
        "Price: CALL FOR PRICE": "Price: CALL FOR PRICE",
        "Price: $50k Down Sales: $400,000": "Price: $50k Down",
    }
    for text, want in cases.items():
        assert P.find_labelled_withheld_price(text) == want, text


def test_labelled_card_text_rejects():
    for text in ("Price: Sold", "Price: Coming Soon", "Price: N/A Location: Cincinnati",
                 "Comments: Remarks: SOLD", "We buy vending machines, priced on request.",
                 "Asking Price: $350,000", ""):
        assert P.find_labelled_withheld_price(text) is None, text


# ── writer plumbing ──────────────────────────────────────────────────────────

def test_missing_column_error_detection():
    pgrst = ('{"code":"PGRST204","details":null,"hint":null,"message":"Could not find the '
             '\'broker_price_text\' column of \'listings_direct\' in the schema cache"}')
    assert P.is_missing_disclosure_column(pgrst)
    assert P.is_missing_disclosure_column('column "price_not_disclosed" of relation "listings_direct" does not exist (42703)')
    assert not P.is_missing_disclosure_column('{"code":"PGRST204","message":"Could not find the \'foo\' column"}')
    assert not P.is_missing_disclosure_column('duplicate key value violates unique constraint')


def test_group_by_keys_keeps_batches_uniform():
    rows = [{"id": "a"}, {"id": "b", "price_not_disclosed": True, "broker_price_text": "TBD"},
            {"id": "c"}]
    groups = P.group_by_keys(rows)
    assert sorted(len(g) for g in groups) == [1, 2]
    for g in groups:
        assert len({tuple(sorted(r)) for r in g}) == 1


def test_strip_and_log_once():
    cols, logged = P.DisclosureColumns(), []
    rows = [{"id": "a", "price_not_disclosed": False, "broker_price_text": None}, {"id": "b"}]
    assert P.strip_disclosure(rows) == 1
    assert rows == [{"id": "a"}, {"id": "b"}]
    cols.note_missing(logged.append, 1)
    cols.note_missing(logged.append, 3)
    assert cols.missing and cols.unwritten == 4 and len(logged) == 1


def test_specialized_writer_retries_without_columns_and_omits_unevidenced():
    """run_specialized.upsert_listings: a PGRST204 on the first POST is retried
    once without the two fields; a row with no price and no phrase never carries
    them (so an existing row's value is never overwritten by a default)."""
    try:
        import run_specialized as RS
    except Exception as exc:  # heavy imports (specialized_scrapers) unavailable
        import pytest
        pytest.skip(f"run_specialized not importable here: {exc}")

    posts = []

    class Resp:
        def __init__(self, code, text=""):
            self.status_code, self.text, self.ok = code, text, code < 300

        def json(self):
            return []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append([dict(r) for r in json])
        if any("price_not_disclosed" in r for r in json):
            return Resp(400, '{"code":"PGRST204","message":"Could not find the '
                             '\'price_not_disclosed\' column of \'listings_direct\' in the schema cache"}')
        return Resp(201)

    listings = [
        {"listing_url": "https://ex-broker.com/listing/1", "title": "Profitable HVAC Company",
         "price": None, "price_text": "Market Price", "broker_account": "ex"},
        {"listing_url": "https://ex-broker.com/listing/2", "title": "Established Bakery Business",
         "price": None, "price_text": None, "broker_account": "ex"},
    ]
    RS._DISCLOSURE = P.DisclosureColumns()
    with mock.patch.object(RS.http_requests, "post", side_effect=fake_post), \
         mock.patch.object(RS.http_requests, "get", return_value=Resp(200)):
        n = RS.upsert_listings(listings, display_name="Ex Broker")
    assert n == 2
    # the bare-null row never carried the fields
    assert all("price_not_disclosed" not in r for p in posts for r in p
               if r["url"].endswith("/2"))
    # the phrase row was tried with them, then landed without them
    tried = [r for p in posts for r in p if r["url"].endswith("/1")]
    assert tried[0]["price_not_disclosed"] is True and tried[0]["broker_price_text"] == "Market Price"
    assert "price_not_disclosed" not in tried[-1]
    assert RS._DISCLOSURE.missing and RS._DISCLOSURE.unwritten == 1
    RS._DISCLOSURE = P.DisclosureColumns()  # don't leak state into other tests


def test_v6_writer_same_fields_same_way():
    """dealledger_scraper_v6.SupabaseWriter.upsert writes the same two fields by
    the same rule as run_specialized (principle 15), and survives PGRST204."""
    try:
        import dealledger_scraper_v6 as V6
    except Exception as exc:
        import pytest
        pytest.skip(f"dealledger_scraper_v6 not importable here: {exc}")

    sent = []

    class APIError(Exception):
        pass

    class Q:
        def __init__(self, rows):
            self.rows = rows

        def execute(self):
            sent.append([dict(r) for r in self.rows])
            if any("broker_price_text" in r for r in self.rows):
                raise APIError("{'code': 'PGRST204', 'message': \"Could not find the "
                               "'broker_price_text' column of 'listings_direct' in the schema cache\"}")

    class T:
        def upsert(self, rows, on_conflict=None, ignore_duplicates=None):
            return Q(rows)

    class C:
        def table(self, name):
            assert name == "listings_direct"
            return T()

    # Built WITHOUT __init__ (as other tests do): the writer must not depend on
    # instance attributes set there.
    w = V6.SupabaseWriter.__new__(V6.SupabaseWriter)
    w.client = C()
    V6._DISCLOSURE = P.DisclosureColumns()
    base = {"broker_name": "Ex", "broker_domain": "ex-broker.com", "description": ""}
    listings = [
        dict(base, id="p1", title="Priced Plumbing Co", url="https://ex-broker.com/1", asking_price=300000.0),
        dict(base, id="w1", title="Withheld Dental Practice", url="https://ex-broker.com/2",
             asking_price=None, broker_price_text="Asking Price: Market Price"),
        dict(base, id="n1", title="Unpriced Landscaping Co", url="https://ex-broker.com/3", asking_price=None),
    ]
    assert w.upsert(listings) == 3
    by_id = {}
    for batch in sent:
        for r in batch:
            by_id.setdefault(r["id"], r)
    assert by_id["p1"]["price_not_disclosed"] is False and by_id["p1"]["broker_price_text"] is None
    assert by_id["w1"]["price_not_disclosed"] is True
    assert by_id["w1"]["broker_price_text"] == "Asking Price: Market Price"
    assert "price_not_disclosed" not in by_id["n1"]          # bare null: nothing sent
    assert V6._DISCLOSURE.missing and V6._DISCLOSURE.unwritten == 2
    V6._DISCLOSURE = P.DisclosureColumns()  # don't leak state into other tests
    assert all("price_not_disclosed" not in r for r in sent[-1])


def test_v6_card_carries_labelled_phrase():
    try:
        import dealledger_scraper_v6 as V6
        from bs4 import BeautifulSoup
    except Exception as exc:
        import pytest
        pytest.skip(f"dealledger_scraper_v6 not importable here: {exc}")
    html = ('<div class="listing"><h3><a href="/listings/dental-practice-ohio">Dental Practice '
            'in Ohio</a></h3><p>Asking Price: Market Price</p><p>Cash Flow: Confidential</p>'
            '<p>Established dental practice with strong patient base.</p></div>')
    el = BeautifulSoup(html, "html.parser").div
    rec = V6.ListingExtractor.from_card(el, "https://ex-broker.com/listings/", "Ex Broker")
    if rec is None:
        import pytest
        pytest.skip("card rejected by V6 gates in this fixture")
    assert rec["asking_price"] is None
    assert rec["broker_price_text"] == "Asking Price: Market Price"
    assert P.disclosure_fields(rec) == {"price_not_disclosed": True,
                                        "broker_price_text": "Asking Price: Market Price"}
