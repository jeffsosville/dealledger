"""Offline check: ninbb.com's Base44 Listing feed -> V6 rows.
Run: python3 tests/test_ninbb.py  (no network, no Supabase)."""
import hashlib, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
sys.argv = sys.argv[:1]
import dealledger_scraper_v6 as v6

BASE = "https://www.ninbb.com/search_results"
SHELL = ('<html><head><script>const appId = "6aa876573c7d88aaa19a8c77";</script>'
         '<meta name="base44-analytics" content="on"></head><body><div id="root"></div></body></html>')


def _h(s):
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def item(i, **kw):
    it = {"id": f"6aab{i:020x}", "slug": f"listing-{i}", "listing_headline": f"Business {i}",
          "listing_status": "Active", "published": True, "is_training": False,
          "is_sample": False, "asking_price": 500000.0, "asking_price_public": True,
          "revenue_public": 1200000.0, "cash_flow_public": 250000.0,
          "hidden_fields": ["employees", "year_established"],
          "listing_region": "Allegheny County, PA", "listing_state": "PA",
          "listing_summary_public": "A summary.", "bd_filename": None}
    it.update(kw)
    return it


class Resp:
    def __init__(self, data, code=200):
        self.status_code, self._d = code, data

    def json(self):
        return self._d


class Session:
    """Serves `items` sorted by id with limit/skip, like the Base44 API."""
    def __init__(self, items, code=200, dup=False):
        self.items, self.code, self.dup, self.calls = items, code, dup, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        rows = sorted(self.items, key=lambda x: x["id"])
        s, n = params["skip"], params["limit"]
        page = rows[s:s + n]
        if self.dup and s > 0:            # unstable paging: page 2 repeats page 1
            page = rows[:len(page)]
        return Resp(page, self.code)


class Fetcher:
    def __init__(self, session):
        self.session = session


def run(items, **kw):
    sess = Session(items, **kw)
    return v6._handler_base44(BASE, SHELL, "ninbb.com", Fetcher(sess)), sess


def test_app_id_and_endpoint():
    rows, sess = run([item(1)])
    url, params = sess.calls[0]
    assert url == "https://www.ninbb.com/api/apps/6aa876573c7d88aaa19a8c77/entities/Listing"
    assert params["sort"] == "id"                      # unique key, stable paging
    assert "marketing_notes" not in params["fields"]   # only public fields requested
    assert v6._handler_base44(BASE, "<html>no app</html>", "x", Fetcher(sess)) is None


def test_public_filter_matches_site():
    items = [item(1), item(2, listing_status="Under LOI"), item(3, listing_status="Sale Pending"),
             item(4, listing_status="Withdrawn"), item(5, listing_status="Sold"),
             item(6, published=False), item(7, is_training=True), item(8, listing_status="Draft")]
    rows, _ = run(items)
    assert sorted(r["title"] for r in rows) == ["Business 1", "Business 2", "Business 3"]
    assert {r["broker_status"] for r in rows} == {"Active", "Under LOI", "Sale Pending"}


def test_priced_row():
    (r,), _ = run([item(1)])
    assert r["asking_price"] == 500000.0 and r["price_not_disclosed"] is False
    assert r["revenue"] == 1200000.0 and r["cash_flow"] == 250000.0
    assert r["state"] == "PA" and r["city"] is None
    assert r["url"] == "https://ninbb.com/listings/listing-1"
    assert r["url_is_listing_specific"] is True
    assert r["broker_domain"] == "www.ninbb.com"
    assert r["id"] == _h("https://ninbb.com/listings/listing-1")


def test_no_price_is_not_disclosed():
    rows, _ = run([item(1, asking_price_public=False, asking_price=2_500_000.0),
                   item(2, asking_price=None),
                   item(3, asking_price=0),
                   item(4, hidden_fields=["asking_price"])])
    assert len(rows) == 4
    for r in rows:
        # The feed's private asking price must never leak into the row.
        assert r["asking_price"] is None, r
        assert r["price_not_disclosed"] is True
        assert r["revenue"] == 1200000.0      # other public figures still kept


def test_hidden_fields_respected():
    (r,), _ = run([item(1, hidden_fields=["revenue", "cash_flow"])])
    assert r["revenue"] is None and r["cash_flow"] is None and r["asking_price"] == 500000.0


def test_identity_legacy_and_stable():
    bd = "united-states/allegheny-county/restaurants-bars/established-turnkey-restaurant-event-venue-%E2%80%93-absentee-run"
    a, _ = run([item(1, bd_filename=bd)])
    # Same id the old Brilliant Directories crawl gave this listing.
    legacy = "https://www.ninbb.com/united-states/allegheny-county/restaurants-bars/established-turnkey-restaurant-event-venue-–-absentee-run"
    assert a[0]["id"] == _h(legacy)
    assert a[0]["url"] == "https://ninbb.com/listings/listing-1"
    # Price changes never change identity (principle 4).
    b, _ = run([item(1, bd_filename=bd, asking_price=123456.0)])
    assert b[0]["id"] == a[0]["id"]
    # A new-site listing with no bd_filename is keyed on its detail URL.
    c, _ = run([item(2)])
    assert c[0]["id"] == _h("https://ninbb.com/listings/listing-2")


def test_paging_and_count():
    items = [item(i) for i in range(1, 1201)]
    rows, sess = run(items)
    assert len(rows) == 1200 and len({r["id"] for r in rows}) == 1200
    assert [c[1]["skip"] for c in sess.calls] == [0, 500, 1000]


def test_unstable_paging_refuses_partial_set():
    items = [item(i) for i in range(1, 801)]
    rows, _ = run(items, dup=True)
    assert rows is None


def test_http_error_and_foreign_schema():
    assert run([item(1)], code=403)[0] is None
    assert run([{"id": "x", "name": "not a listing"}])[0] is None


def test_registered_and_dispatched():
    assert ("base44", v6._handler_base44) in v6.API_HANDLERS
    sess = Session([item(1), item(2, asking_price_public=False)])
    out = v6.extract_via_api(BASE, SHELL, "ninbb.com", Fetcher(sess))
    assert len(out) == 2 and sum(r["price_not_disclosed"] for r in out) == 1


if __name__ == "__main__":
    n = 0
    for k, f in list(globals().items()):
        if k.startswith("test_") and callable(f):
            f(); n += 1
    print(f"ninbb base44 feed: {n} tests ok")


# ── job 3: Under LOI / Sale Pending -> active + broker_status_text (decision 6) ──
def test_public_statuses_active_with_label():
    rows = {r["broker_status"]: r for r in (
        v6._base44_row(item(i, listing_status=st), "https://www.ninbb.com/", "ninbb")
        for i, st in ((1, "Active"), (2, "Under LOI"), (3, "Sale Pending")))}
    assert {r["status"] for r in rows.values()} == {"active"}
    assert "broker_status_text" not in rows["Active"]
    assert rows["Under LOI"]["broker_status_text"] == "Under LOI"
    assert rows["Sale Pending"]["broker_status_text"] == "Sale Pending"
