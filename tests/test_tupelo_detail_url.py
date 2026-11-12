"""Tupelo SMB widget: per-listing detail URL (?listingId=<feed id>), the id
change it causes, and the feed's page-based pagination.

Evidence for the URL form (crm.tupelosmb.com/widgets/marketplace/index.js):
the card's click handler is `n({listingId:e.id})` where n is react-router's
useSearchParams setter (navigate("?"+params), browser history), and the
router component renders the detail view when `n.get("listingId")` is set.
The listing feed is paged with `take`/`page` (the widget sends take=10 and
page=N); the API ignores `skip`.
"""
import hashlib
import os
import sys
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import dealledger_scraper_v6 as v6  # noqa: E402

PAGE = '<tupelo-marketplace organization-id="cmabcdefghijklmnopqrstuv" />'
ORG = "cmabcdefghijklmnopqrstuv"


def _old_id(domain, title):
    return hashlib.sha256(f"{domain}|{title.strip().lower()}".encode()).hexdigest()[:16]


def _url_id(url):
    return hashlib.sha256(url.encode()).hexdigest()[:16]


class _Resp:
    def __init__(self, data, status=200):
        self.status_code, self._data = status, data

    def json(self):
        return self._data


class _FakeTupeloAPI:
    """Models the real API: honours take + page (1-based), ignores skip,
    honours statuses only if `honour_status`. `ignore_page` models an API
    that returns page 1 forever."""

    def __init__(self, items, honour_status=True, ignore_page=False):
        self.items, self.honour_status, self.ignore_page = items, honour_status, ignore_page
        self.calls = []
        self.session = self

    def get(self, url, timeout=None):
        self.calls.append(url)
        q = parse_qs(urlparse(url).query)
        assert q["organizationId"] == [ORG]
        assert "skip" not in q
        take = int(q.get("take", ["10"])[0])
        page = 1 if self.ignore_page else int(q.get("page", ["1"])[0])
        pool = self.items
        if self.honour_status and "statuses" in q:
            pool = [i for i in pool if i["status"] in q["statuses"]]
        chunk = pool[(page - 1) * take: page * take]
        return _Resp({"listings": chunk, "totalCount": len(pool)})


def _item(n, status="ACTIVE", with_id=True):
    it = {"headline": f"Listing {n}", "status": status, "askingPrice": 100000 + n,
          "cashFlow": 50000, "revenue": 300000,
          "locationStringShort": "Dallas, TX"}
    if with_id:
        it["id"] = f"cm{n:022d}"
    return it


# ── URL builder ───────────────────────────────────────────────────────────

def test_detail_url_drops_index_query_and_fragment():
    u = v6._tupelo_detail_url(
        "https://1800bizbroker.com/business-for-sale/?statuses=ACTIVE#top", "cmabc123")
    assert u == "https://1800bizbroker.com/business-for-sale/?listingId=cmabc123"


def test_detail_url_keeps_path_as_given():
    assert (v6._tupelo_detail_url("https://1800bizbroker.com/business-for-sale", "cmx")
            == "https://1800bizbroker.com/business-for-sale?listingId=cmx")


def test_detail_url_none_without_id():
    assert v6._tupelo_detail_url("https://x.com/listings", None) is None
    assert v6._tupelo_detail_url("https://x.com/listings", "  ") is None


def test_detail_url_quotes_id():
    assert v6._tupelo_detail_url("https://x.com/l", "a b&c").endswith("?listingId=a%20b%26c")


# ── Handler: urls, ids, identity change ───────────────────────────────────

def test_handler_gives_each_listing_its_own_url_and_id():
    base = "https://1800bizbroker.com/business-for-sale"
    api = _FakeTupeloAPI([_item(i) for i in range(5)])
    out = v6._handler_tupelosmb(base, PAGE, "1800", api)
    assert len(out) == 5
    assert len({l["url"] for l in out}) == 5
    assert len({l["id"] for l in out}) == 5
    for l, it in zip(out, api.items):
        assert l["url"] == f"{base}?listingId={it['id']}"
        assert l["url_is_listing_specific"] is True
        # new identity is the detail URL ...
        assert l["id"] == _url_id(l["url"])
        # ... and is NOT the old domain|title identity (expected migration)
        assert l["id"] != _old_id("1800bizbroker.com", it["headline"])
        assert l["broker_domain"] == "1800bizbroker.com"


def test_handler_ids_stable_across_runs():
    base = "https://boutiquefitnessbroker.com/businesses-for-sale"
    items = [_item(i) for i in range(4)]
    a = v6._handler_tupelosmb(base, PAGE, "b", _FakeTupeloAPI(items))
    b = v6._handler_tupelosmb(base, PAGE, "b", _FakeTupeloAPI(list(reversed(items))))
    assert {(l["id"], l["url"]) for l in a} == {(l["id"], l["url"]) for l in b}


def test_same_title_two_listings_no_longer_collapse():
    items = [_item(1), _item(2)]
    items[1]["headline"] = items[0]["headline"]
    out = v6._handler_tupelosmb("https://x.com/listings", PAGE, "x", _FakeTupeloAPI(items))
    assert len({l["id"] for l in out}) == 2


def test_item_without_id_keeps_old_identity():
    base = "https://x.com/listings"
    out = v6._handler_tupelosmb(base, PAGE, "x",
                                _FakeTupeloAPI([_item(1, with_id=False)]))
    assert out[0]["url"] == base
    assert out[0]["url_is_listing_specific"] is False
    assert out[0]["id"] == _old_id("x.com", "Listing 1")


# ── Pagination / status ───────────────────────────────────────────────────

def test_paginates_by_page_past_first_page():
    # 150 active + 60 sold: the old skip loop re-read page 1 and lost 50.
    items = [_item(i) for i in range(150)] + [_item(1000 + i, "SOLD") for i in range(60)]
    api = _FakeTupeloAPI(items)
    out = v6._handler_tupelosmb("https://x.com/listings", PAGE, "x", api)
    assert len(out) == 150
    assert len({l["id"] for l in out}) == 150
    assert len(api.calls) == 2
    assert all("statuses=ACTIVE" in c for c in api.calls)


def test_client_side_status_filter_when_api_ignores_statuses():
    items = [_item(1), _item(2, "SALE_PENDING"), _item(3, "SOLD"), _item(4)]
    out = v6._handler_tupelosmb("https://x.com/listings", PAGE, "x",
                                _FakeTupeloAPI(items, honour_status=False))
    assert sorted(l["title"] for l in out) == ["Listing 1", "Listing 4"]


def test_repeated_page_stops_without_duplicates():
    # An API that ignores page (returns page 1 forever) must not double rows.
    items = [_item(i) for i in range(250)]
    api = _FakeTupeloAPI(items, ignore_page=True)
    out = v6._handler_tupelosmb("https://x.com/listings", PAGE, "x", api)
    assert len(out) == 100
    assert len({l["id"] for l in out}) == 100
    assert len(api.calls) == 2


def test_org_id_from_cm_attribute():
    # Stacked on task-A-tupelo_org_id.patch: 'cm…' cuids in organization-id.
    api = _FakeTupeloAPI([_item(1)])
    assert v6._handler_tupelosmb("https://x.com/listings", PAGE, "x", api)
