"""
tests/test_broker_status_text.py — the broker's verbatim status label
(decision 6: "Under Contract" stays ACTIVE, the words go in
listings_direct.broker_status_text) reaches the table through BOTH writers, the
same way, only with evidence.

Load-bearing cases:
  * a row without a label sends neither column — merge-duplicates would
    otherwise null a stored label on every crawl (principle 14);
  * the columns may not exist yet: PGRST204 -> retried without them, logged
    once, counted (principle 17);
  * V6 and run_specialized write the same two field names (principle 15).
"""
import os
import sys
from unittest import mock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import optional_columns as OC  # noqa: E402
import price_disclosure as P  # noqa: E402

NOW = "2026-11-12T14:23:00+00:00"
LABEL_KEYS = ("broker_status_text", "broker_status_seen_at")


def _pgrst204(col):
    return ('{"code":"PGRST204","details":null,"hint":null,"message":"Could not find the '
            f"'{col}' column of 'listings_direct' in the schema cache\"}}")


# ── the field builder ────────────────────────────────────────────────────────

def test_label_sent_only_with_evidence():
    assert OC.status_label_fields({"broker_status_text": "Under Contract"}, NOW) == {
        "broker_status_text": "Under Contract", "broker_status_seen_at": NOW}


def test_no_label_sends_nothing():
    for row in ({}, {"status": "active"}, {"status": "sold"},
                {"broker_status_text": None}, {"broker_status_text": ""},
                {"broker_status_text": "   \n\xa0 "}, {"broker_status_text": 0},
                {"description": "Status: Under Contract"},      # prose is never read
                {"broker_label": "Under Contract"}):            # other keys are not the label
        assert OC.status_label_fields(row, NOW) is None, row


def test_label_verbatim_whitespace_collapsed_and_capped():
    out = OC.status_label_fields({"broker_status_text": "  Sale\n  Pending!\xa0"}, NOW)
    assert out["broker_status_text"] == "Sale Pending!"          # case + punctuation kept
    long = OC.status_label_fields({"broker_status_text": "Under Contract " + "x" * 300}, NOW)
    assert len(long["broker_status_text"]) <= OC.MAX_STATUS_LABEL == 120


def test_labels_from_the_shared_status_module_pass_through():
    try:
        from listing_status import apply_status
    except Exception as exc:
        pytest.skip(f"listing_status not present: {exc}")
    row = apply_status({"status": "active"}, "Under Contract!")
    assert row["status"] == "active"
    assert OC.status_label_fields(row, NOW)["broker_status_text"] == "Under Contract!"
    plain = apply_status({"status": "active"}, "For Sale")
    assert OC.status_label_fields(plain, NOW) is None             # active, no label


# ── the shared mechanism ─────────────────────────────────────────────────────

def test_missing_column_detection_is_per_group():
    lab, dis = OC.new_status_label_columns(), P.DisclosureColumns()
    assert lab.matches(_pgrst204("broker_status_text"))
    assert lab.matches(_pgrst204("broker_status_seen_at"))
    assert lab.matches('column "broker_status_text" of relation "listings_direct" does not exist (42703)')
    assert not lab.matches(_pgrst204("broker_price_text"))
    assert not dis.matches(_pgrst204("broker_status_text"))
    assert OC.missing_group(_pgrst204("broker_status_seen_at"), (dis, lab)) is lab
    assert OC.missing_group("duplicate key value violates unique constraint", (dis, lab)) is None
    lab.missing = True                       # already handled -> not a reason to retry again
    assert OC.missing_group(_pgrst204("broker_status_text"), (dis, lab)) is None


def test_disclosure_is_an_instance_of_the_shared_mechanism():
    # principle 15: one optional-column mechanism, not one per feature
    assert isinstance(P.DisclosureColumns(), OC.OptionalColumns)
    assert P.group_by_keys is OC.group_by_keys


def test_strip_log_once_and_count():
    lab, logged = OC.new_status_label_columns(), []
    rows = [{"id": "a", "broker_status_text": "Pending", "broker_status_seen_at": NOW}, {"id": "b"}]
    assert lab.strip_if_missing(rows) == 0 and "broker_status_text" in rows[0]   # not missing yet
    lab.note_missing(logged.append, lab.strip(rows))
    lab.note_missing(logged.append, 2)
    assert rows == [{"id": "a"}, {"id": "b"}]
    assert lab.missing and lab.unwritten == 3 and len(logged) == 1
    assert "broker_status_text" in logged[0] and "j3-broker_status_text.sql" in logged[0]
    assert "3 row(s)" in lab.summary()


# ── V6 writer ────────────────────────────────────────────────────────────────

class _FakeV6Client:
    """supabase-py shape: client.table(...).upsert(rows, ...).execute().
    `missing` = columns this fake database does not have."""

    def __init__(self, missing=()):
        self.sent, self.missing = [], set(missing)

    def table(self, name):
        assert name == "listings_direct"
        client = self

        class Q:
            def __init__(self, rows):
                self.rows = rows

            def execute(self):
                client.sent.append([dict(r) for r in self.rows])
                for col in sorted(client.missing):
                    if any(col in r for r in self.rows):
                        raise Exception("{'code': 'PGRST204', 'message': \"Could not find the "
                                        f"'{col}' column of 'listings_direct' in the schema cache\"}}")

        class T:
            def upsert(self, rows, on_conflict=None, ignore_duplicates=None):
                return Q(rows)

        return T()


def _v6():
    try:
        import dealledger_scraper_v6 as V6
    except Exception as exc:
        pytest.skip(f"dealledger_scraper_v6 not importable here: {exc}")
    return V6


def _v6_listings():
    base = {"broker_name": "Ex", "broker_domain": "ex-broker.com", "description": ""}
    return [
        dict(base, id="uc", title="Pending HVAC Company", url="https://ex-broker.com/1",
             asking_price=300000.0, status="active", broker_status_text="Under Contract"),
        dict(base, id="plain", title="Plain Bakery Business", url="https://ex-broker.com/2",
             asking_price=250000.0, status="active"),
        dict(base, id="sold", title="Closed Dental Practice", url="https://ex-broker.com/3",
             asking_price=None, status="sold", broker_status_text="SOLD!"),
    ]


def _first_by_id(sent):
    out = {}
    for batch in sent:
        for r in batch:
            out.setdefault(r["id"], r)
    return out


def test_v6_writer_sends_label_only_with_evidence():
    V6 = _v6()
    w = V6.SupabaseWriter.__new__(V6.SupabaseWriter)
    w.client = _FakeV6Client()
    V6._DISCLOSURE, V6._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()
    try:
        assert w.upsert(_v6_listings()) == 3
        by_id = _first_by_id(w.client.sent)
        assert by_id["uc"]["broker_status_text"] == "Under Contract"
        assert by_id["uc"]["broker_status_seen_at"] == by_id["uc"]["last_seen"]   # same `now`
        assert by_id["sold"]["broker_status_text"] == "SOLD!"
        assert not any(k in by_id["plain"] for k in LABEL_KEYS)                  # nothing sent
        for batch in w.client.sent:                                              # uniform POSTs
            assert len({tuple(sorted(r)) for r in batch}) == 1
        assert not V6._STATUS_LABEL.missing
    finally:
        V6._DISCLOSURE, V6._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()


def test_v6_writer_retries_without_missing_label_column():
    V6 = _v6()
    w = V6.SupabaseWriter.__new__(V6.SupabaseWriter)
    w.client = _FakeV6Client(missing={"broker_status_text", "broker_status_seen_at"})
    V6._DISCLOSURE, V6._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()
    try:
        assert w.upsert(_v6_listings()) == 3                  # nothing lost
        assert V6._STATUS_LABEL.missing and V6._STATUS_LABEL.unwritten == 2
        assert not V6._DISCLOSURE.missing                     # the other group is untouched
        # every row landed, the last attempt for each carried no label
        last = {}
        for batch in w.client.sent:
            for r in batch:
                last[r["id"]] = r
        assert set(last) == {"uc", "plain", "sold"}
        assert all(not any(k in r for k in LABEL_KEYS) for r in last.values())
        # the next upsert in the same process does not even try
        n_before = len(w.client.sent)
        assert w.upsert(_v6_listings()[:1]) == 1
        assert len(w.client.sent) == n_before + 1
        assert V6._STATUS_LABEL.unwritten == 3
    finally:
        V6._DISCLOSURE, V6._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()


def test_v6_writer_both_groups_missing():
    V6 = _v6()
    w = V6.SupabaseWriter.__new__(V6.SupabaseWriter)
    w.client = _FakeV6Client(missing={"broker_status_text", "price_not_disclosed",
                                      "broker_price_text", "broker_status_seen_at"})
    V6._DISCLOSURE, V6._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()
    try:
        rows = _v6_listings()
        rows[2]["broker_price_text"] = "Asking Price: Market Price"   # disclosure on the sold row
        assert w.upsert(rows) == 3
        assert V6._STATUS_LABEL.missing and V6._DISCLOSURE.missing
    finally:
        V6._DISCLOSURE, V6._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()


# ── run_specialized writer ───────────────────────────────────────────────────

def _rs():
    try:
        import run_specialized as RS
    except Exception as exc:  # heavy imports (specialized_scrapers) unavailable
        pytest.skip(f"run_specialized not importable here: {exc}")
    return RS


class _Resp:
    def __init__(self, code, text="", rows=None):
        self.status_code, self.text, self.ok = code, text, code < 300
        self._rows = rows or []

    def json(self):
        return self._rows


def _spec_listings():
    return [
        {"listing_url": "https://ex-broker.com/listing/1", "title": "Profitable HVAC Company",
         "price": 400000, "status": "active", "broker_status_text": "Sale Pending",
         "broker_account": "ex"},
        {"listing_url": "https://ex-broker.com/listing/2", "title": "Established Bakery Business",
         "price": 250000, "status": "active", "broker_account": "ex"},
    ]


def _run_spec(RS, missing=(), existing_ids=()):
    posts = []

    def fake_post(url, headers=None, json=None, timeout=None):
        posts.append([dict(r) for r in json])
        for col in missing:
            if any(col in r for r in json):
                return _Resp(400, _pgrst204(col))
        return _Resp(201)

    def fake_get(url, headers=None, params=None, timeout=None):
        return _Resp(200, rows=[{"id": i} for i in existing_ids])

    with mock.patch.object(RS.http_requests, "post", side_effect=fake_post), \
         mock.patch.object(RS.http_requests, "get", side_effect=fake_get):
        n = RS.upsert_listings(_spec_listings(), display_name="Ex Broker")
    return n, posts


def test_specialized_writer_sends_label_only_with_evidence():
    RS = _rs()
    RS._DISCLOSURE, RS._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()
    try:
        n, posts = _run_spec(RS)
        assert n == 2
        by_url = {r["url"]: r for p in posts for r in p}
        uc, plain = by_url["https://ex-broker.com/listing/1"], by_url["https://ex-broker.com/listing/2"]
        assert uc["broker_status_text"] == "Sale Pending"
        assert uc["broker_status_seen_at"] == uc["last_seen"]
        assert not any(k in plain for k in LABEL_KEYS)
        for p in posts:
            assert len({tuple(sorted(r)) for r in p}) == 1
    finally:
        RS._DISCLOSURE, RS._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()


def test_specialized_existing_row_keeps_label_without_status_evidence():
    """An existing row with an under-contract label: status 'active' is not
    evidence (principle 14 status guard pops it), but the label IS and is sent."""
    RS = _rs()
    RS._DISCLOSURE, RS._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()
    try:
        import hashlib
        key = RS.listing_key("https://ex-broker.com/listing/1", "ex-broker.com")
        uid = f"spec:{hashlib.md5(key.encode()).hexdigest()[:16]}"
        n, posts = _run_spec(RS, existing_ids=[uid])
        assert n == 2
        row = [r for p in posts for r in p if r["id"] == uid][0]
        assert "status" not in row and "first_seen" not in row
        assert row["broker_status_text"] == "Sale Pending"
    finally:
        RS._DISCLOSURE, RS._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()


def test_specialized_writer_retries_without_missing_label_column():
    RS = _rs()
    RS._DISCLOSURE, RS._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()
    try:
        n, posts = _run_spec(RS, missing=("broker_status_seen_at",))
        assert n == 2                                         # nothing lost, no split-to-singles
        tried = [r for p in posts for r in p if r["url"].endswith("/1")]
        assert tried[0]["broker_status_text"] == "Sale Pending"
        assert not any(k in tried[-1] for k in LABEL_KEYS)
        assert RS._STATUS_LABEL.missing and RS._STATUS_LABEL.unwritten == 1
        assert not RS._DISCLOSURE.missing
        assert len(posts) == 3                                # labelled POST, its retry, plain POST
    finally:
        RS._DISCLOSURE, RS._STATUS_LABEL = P.DisclosureColumns(), OC.new_status_label_columns()


# ── both writers agree (principle 15) ────────────────────────────────────────

def test_both_writers_agree_on_field_names():
    V6, RS = _v6(), _rs()
    assert V6.status_label_fields is RS.status_label_fields is OC.status_label_fields
    assert V6._STATUS_LABEL.keys == RS._STATUS_LABEL.keys == LABEL_KEYS
    assert [g.keys for g in V6._optional_groups()] == [g.keys for g in RS._optional_groups()]

    V6._STATUS_LABEL, RS._STATUS_LABEL = OC.new_status_label_columns(), OC.new_status_label_columns()
    try:
        w = V6.SupabaseWriter.__new__(V6.SupabaseWriter)
        w.client = _FakeV6Client()
        w.upsert(_v6_listings()[:1])
        v6_keys = {k for b in w.client.sent for r in b for k in r if k.startswith("broker_status")}
        _, posts = _run_spec(RS)
        rs_keys = {k for p in posts for r in p for k in r if k.startswith("broker_status")}
        assert v6_keys == rs_keys == set(LABEL_KEYS)
    finally:
        V6._STATUS_LABEL, RS._STATUS_LABEL = OC.new_status_label_columns(), OC.new_status_label_columns()


def test_label_keys_match_the_migration():
    """The SQL proposal adds exactly these two columns (checked against the
    staged migration when it is reachable from this checkout)."""
    here = os.path.dirname(__file__)
    for p in (os.path.join(here, "..", "claude", "staging", "2026-11-12", "j3-broker_status_text.sql"),
              os.path.join(here, "..", "sql", "2026-11-12_broker_status_text.sql")):
        if os.path.exists(p):
            sql = open(p).read()
            for k in LABEL_KEYS:
                assert f"add column if not exists {k}" in sql, k
            assert f"<= {OC.MAX_STATUS_LABEL}" in sql
            return
    pytest.skip("migration file not in this checkout")
