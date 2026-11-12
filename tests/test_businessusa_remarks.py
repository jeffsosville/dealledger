"""
businessusa.biz: the index table's Remarks column carries the broker's own
status. SOLD -> status 'sold' (explicit, principle 14); "Under Contract" ->
active with the label kept verbatim; anything else -> active with the remark
kept as a note. The id is keyed on the Listing # only (principle 4).
"""
import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import dealledger_scraper_v6 as v6  # noqa: E402

BASE = "https://businessusa.biz/business-for-sale"

HEAD = """<table class="table table-bordered table-hover dt-responsive"><thead><tr>
<th><a href="?sort_by=listing_number">Listing #</a></th><th>Type</th><th>County</th>
<th>State</th><th>Price</th><th>Annual</th><th>Remarks</th></tr></thead><tbody>"""


def _row(num, remark, price="$ 299,000.00", annual="$ 2,632,000.00",
         host="businessusa.biz"):
    return (f'<tr><td width="9%"><a href="https://{host}/business/{num}">\n'
            f'   {num}\n</a></td><td>Gas Station</td><td>Amherst</td><td>VA</td>'
            f'<td>\n\n {price}</td><td>\n {annual}</td>'
            f'<td width="20%"><p>{remark}</p></td></tr>')


def _page(*rows):
    return ("<html><body><p>Showing <b>%d</b> of %d businesses.</p>%s%s"
            "</tbody></table></body></html>" % (len(rows), len(rows), HEAD, "".join(rows)))


def _parse(html):
    return v6.parse_businessusa_rows(html, BASE, "businessusa.biz")


def _one(remark):
    rows = _parse(_page(_row(9000, remark)))
    assert len(rows) == 1
    return rows[0]


def test_sold_is_explicit_sold_with_verbatim_text():
    for remark in ("SOLD", "sold", "Sold"):
        r = _one(remark)
        assert r["status"] == "sold"
        assert r["broker_status_text"] == remark
        assert r["status_source"] == "index_remarks"
        assert r["broker_note"] is None


def test_under_contract_stays_active_with_label():
    r = _one("Under Contract")
    assert r["status"] == "active"
    assert r["broker_status_text"] == "Under Contract"
    assert r["broker_note"] is None


def test_free_note_is_active_note():
    note = "Gas/C-Store/Atm/Lotto,\xa0 Net\xa0$250k"
    r = _one(note)
    assert r["status"] == "active"
    assert r["broker_status_text"] is None
    assert r["broker_note"] == "Gas/C-Store/Atm/Lotto, Net $250k"
    # A note that merely contains the word is not a SOLD marker.
    r = _one("Sold out of inventory, Net $150K")
    assert r["status"] == "active" and r["broker_note"].startswith("Sold out")


def test_blank_remark_is_active_with_nothing_carried():
    r = _one("")
    assert r["status"] == "active"
    assert r["broker_status_text"] is None and r["broker_note"] is None


def test_columns_map_price_and_annual():
    r = _one("SOLD")
    assert r["asking_price"] == 299000.0      # Price column, not the larger Annual
    assert r["revenue"] == 2632000.0
    assert r["state"] == "VA"
    assert r["url"] == "https://businessusa.biz/business/9000"


def test_id_keyed_on_listing_number_only():
    expected = hashlib.sha256(
        b"https://businessusa.biz/business/9000").hexdigest()[:16]
    ids = {_one(rm)["id"] for rm in ("SOLD", "Under Contract", "a note", "")}
    assert ids == {expected}
    # price change does not change the id
    r = _parse(_page(_row(9000, "SOLD", price="$ 1.00", annual="$ 5.00")))[0]
    assert r["id"] == expected
    # the page host (www vs bare) does not change the id
    r = _parse(_page(_row(9000, "SOLD", host="www.businessusa.biz")))[0]
    assert r["id"] == expected and r["url"] == "https://businessusa.biz/business/9000"
    # a different Listing # is a different id
    assert _one("SOLD")["id"] != _parse(_page(_row(9001, "SOLD")))[0]["id"]


def test_handler_paginates_and_stops_at_displayed_total():
    p1 = _page(_row(1, "SOLD"), _row(2, "Under Contract")).replace(
        "<b>2</b> of 2", "<b>3</b> of 3")
    p2 = _page(_row(3, "note"))
    calls = []

    class Resp:
        status_code = 200

        def __init__(self, text):
            self.text = text

    class Sess:
        def get(self, u, timeout=None):
            calls.append(u)
            return Resp(p2)

    class Fetcher:
        session = Sess()

    rows = v6._handler_businessusa(BASE, p1, "businessusa.biz", Fetcher())
    assert [r["broker_listing_ref"] for r in rows] == ["1", "2", "3"]
    assert [r["status"] for r in rows] == ["sold", "active", "active"]
    assert calls == [BASE + "?page=2"]


def test_handler_ignores_other_hosts():
    html = _page(_row(1, "SOLD"))
    assert v6._handler_businessusa("https://example.com/x", html, "x", None) is None


def test_registered_and_reaches_extract_via_api():
    assert ("businessusa.biz", v6._handler_businessusa) in v6.API_HANDLERS
    html = _page(_row(1, "SOLD"), _row(2, ""))
    rows = v6.extract_via_api(BASE, html, "businessusa.biz", None)
    assert {r["status"] for r in rows} == {"sold", "active"}


def test_state_cell_variants():
    assert v6._businessusa_state("VA", "Amherst") == "VA"
    assert v6._businessusa_state("Virginia", "Fairfax") == "VA"
    assert v6._businessusa_state("Washington DC", "Washington DC") == "DC"
    assert v6._businessusa_state("DC Metro", "DC Metro") == "DC"
    assert v6._businessusa_state("Northern VA", "Northern VA") == "VA"
    assert v6._businessusa_state("Fairfax", "VA") == "VA"     # swapped columns
    assert v6._businessusa_state("", "") is None


def test_writer_sends_sold_as_explicit_status():
    sent = []

    class Q:
        def upsert(self, rows, **kw):
            sent.extend(rows)
            return self

        def execute(self):
            return None

    class Client:
        def table(self, name):
            assert name == "listings_direct"
            return Q()

    w = v6.SupabaseWriter.__new__(v6.SupabaseWriter)
    w.client = Client()
    rows = _parse(_page(_row(1, "SOLD"), _row(2, "Under Contract"), _row(3, "")))
    assert w.upsert(rows) == 3
    by = {r["url"].rsplit("/", 1)[1]: r for r in sent}
    assert by["1"]["status"] == "sold"
    assert by["2"]["status"] == "active" and by["3"]["status"] == "active"
    # the broker's label is written (j3-broker_status_text_writers, decision 6)
    # only on rows that carry one; the free note is still not a DB column
    assert by["1"]["broker_status_text"] == "SOLD" and "broker_note" not in by["1"]
    assert by["2"]["broker_status_text"] == "Under Contract"
    assert by["1"]["broker_status_seen_at"] == by["1"]["last_seen"]
    assert "broker_status_text" not in by["3"] and "broker_status_seen_at" not in by["3"]


# ── job 3: shared status policy (scrapers/listing_status.py, decisions 5 + 6) ──
def test_under_contract_family_active_with_label():
    import importlib.util
    if importlib.util.find_spec("listing_status") is None:
        import pytest
        pytest.skip("listing_status not applied (job 3)")
    for remark in ("Pending", "Sale Pending", "Under LOI", "Accepted Offer", "Off-Market",
                   "UNDER CONTRACT!"):
        r = _one(remark)
        assert r["status"] == "active" and r["broker_status_text"] == remark
        assert r["broker_note"] is None
    assert _one("Sold!")["status"] == "sold"


def test_coming_soon_and_for_lease_remarks_not_emitted():
    import importlib.util
    if importlib.util.find_spec("listing_status") is None:
        import pytest
        pytest.skip("listing_status not applied (job 3)")
    dropped = []
    rows = v6.parse_businessusa_rows(
        _page(_row(1, "Coming Soon"), _row(2, "SOLD"), _row(3, "For Lease")), BASE,
        "businessusa.biz", dropped)
    assert [r["broker_listing_ref"] for r in rows] == ["2"]
    assert dropped == ["Coming Soon", "For Lease"]
