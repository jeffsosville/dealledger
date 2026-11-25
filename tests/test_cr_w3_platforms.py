"""w3-platforms (2026-11-23): platform readers in scrapers/cr_w3_platforms.py and
the Tupelo Sale Pending / SOLD handling in V6.

Principle 8: every reader/filter test pairs the rejecting case with input that
must still pass (ACTIVE rows, plain cards, a url with a fragment or query).
Fixtures in tests/fixtures/w3 are trimmed copies of the live pages (23 Nov 2026).
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import cr_w3_platforms as c  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "w3"


def fx(name):
    return (FIX / name).read_text(errors="replace")


class _Sess:
    def __init__(self, routes):
        self.routes, self.calls = routes, []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        for key, val in self.routes.items():
            if key in url:
                val = val(url, params) if callable(val) else val
                return _R(val)
        return _R(None, 404)


class _R:
    def __init__(self, data, status=200):
        self.status_code = status
        self._d = data
        self.text = data if isinstance(data, str) else ""

    def json(self):
        return self._d


class _Fetcher:
    def __init__(self, pages=None, routes=None):
        self.pages = pages or {}
        self.session = _Sess(routes or {})

    def fetch(self, url, use_proxy=False):
        for k, v in self.pages.items():
            if k in url:
                return v, "requests"
        raise RuntimeError("no page " + url)


# ── Tupelo: Sale Pending kept, SOLD only for the two named domains ────────────
PAGE = '<tupelo-marketplace organization-id="cmabcdefghijklmnopqrstuv" />'


def _tupelo_items():
    return [
        {"id": "a1", "headline": "HVAC Co", "status": "ACTIVE", "askingPrice": 900000},
        {"id": "p1", "headline": "Marine Contractor", "status": "SALE_PENDING", "askingPrice": 2700000},
        {"id": "s1", "headline": "Old Plumbing", "status": "SOLD", "askingPrice": 500000},
        {"id": "z1", "headline": "Withdrawn Thing", "status": "WITHDRAWN", "askingPrice": 1},
    ]


def _tupelo(domain):
    """Models the live feed: statuses=ACTIVE returns ACTIVE + SALE_PENDING (kcapex:
    20 + 9 = 29 cards); with no status filter every status comes back."""
    items = _tupelo_items()
    f = _Fetcher()

    def get(url, timeout=None):
        pool = items
        if "statuses=ACTIVE" in url:
            pool = [i for i in items if i["status"] in ("ACTIVE", "SALE_PENDING")]
        return _R({"listings": pool, "totalCount": len(pool)})
    f.session.get = get
    return v6._handler_tupelosmb(f"https://www.{domain}/listings/", PAGE, domain, f)


def test_tupelo_sale_pending_is_active_with_label_on_every_domain():
    for dom in ("kcapex.com", "somebroker.com"):
        rows = {r["title"]: r for r in _tupelo(dom)}
        assert rows["Marine Contractor"]["status"] == "active"
        assert rows["Marine Contractor"]["broker_status_text"] == "Sale Pending"


def test_tupelo_active_rows_still_pass_unlabelled():
    rows = {r["title"]: r for r in _tupelo("kcapex.com")}
    assert rows["HVAC Co"]["status"] == "active"
    assert "broker_status_text" not in rows["HVAC Co"]


def test_tupelo_sold_emitted_as_sold_only_for_named_domains():
    named = {r["title"]: r for r in _tupelo("businessmodificationgroup.com")}
    assert named["Old Plumbing"]["status"] == "sold"
    assert named["Old Plumbing"]["broker_status_text"] == "SOLD"
    assert "Withdrawn Thing" not in named
    other = {r["title"] for r in _tupelo("somebroker.com")}
    assert "Old Plumbing" not in other


# ── the url == listings index guard ──────────────────────────────────────────
def test_index_guard_drops_index_rows_only():
    idx = "https://blbrokers.com/businesses-for-sale"
    rows = [
        {"url": idx},
        {"url": idx + "/"},
        {"url": idx + "#dl-some-title"},                      # synthetic per-card url: must pass
        {"url": "https://blbrokers.com/businesses-for-sale/x-2679"},
        {"url": "https://www.kcapex.com/business-listings/?listingId=cm1"},
    ]
    kept, dropped = c.drop_index_url_rows(rows, idx)
    assert dropped == 2
    assert [r["url"] for r in kept] == [r["url"] for r in rows[2:]]
    k2, d2 = c.drop_index_url_rows(
        [{"url": "https://www.kcapex.com/business-listings/?listingId=cm1"}],
        "https://www.kcapex.com/business-listings/")
    assert d2 == 0 and len(k2) == 1


# ── DealRelations widget ─────────────────────────────────────────────────────
def test_dr_sunbelt_cards_strip_query_and_keep_status():
    html = fx("dr_sunbelt_page1.html")
    cards = c.dr_cards(html)
    assert len(cards) == 4
    assert c.dr_total(html) == 30
    for card in cards:
        url, slug = c.dr_identity_url(card["href"], "https://sunbeltofflorida.com/exclusive-listings/", None)
        assert url == f"https://sunbeltofflorida.com/deal-listing/{slug}/"
        assert "?" not in url and "json_url" not in url


def test_dr_same_listing_on_two_pages_has_one_identity():
    a = "https://sunbeltofflorida.com/deal-listing/x/?back=a&dl_page=1&json_url=j"
    b = "https://sunbeltofflorida.com/deal-listing/x/?back=b&dl_page=3&json_url=k"
    assert (c.dr_identity_url(a, "https://sunbeltofflorida.com/", None)
            == c.dr_identity_url(b, "https://sunbeltofflorida.com/", None))


def test_dr_msm_feed_titles_clean_and_hosted_detail_url():
    html = fx("dr_msm_feed_page1.html")
    cards = c.dr_cards(html)
    assert len(cards) == 12
    for card in cards:
        assert not card["title"].startswith("|")
        url, slug = c.dr_identity_url(card["href"], "https://wesellmainstreet.dealrelations.com/listing_feeds/621",
                                      "wesellmainstreet.dealrelations.com")
        assert url == f"https://wesellmainstreet.dealrelations.com/listings/{slug}"


def test_dr_title_leading_pipe_stripped():
    assert c._clean_title(" | Neighborhood Bar ") == "Neighborhood Bar"
    assert c._clean_title("Plain Title") == "Plain Title"


def _install_helpers():
    c.install([], {"make": v6._make_api_listing, "status": v6._ls_lookup,
                   "needs_proxy": lambda d: False, "user_agents": v6.USER_AGENTS,
                   "stable_id": v6.stable_listing_id})


def test_dr_handler_msm_excludes_the_two_real_estate_listings_and_paginates():
    _install_helpers()
    page1 = fx("dr_msm_feed_page1.html")
    iframe_page = ('<div>&lt;iframe src=&quot;https://wesellmainstreet.dealrelations.com/listing_feeds/621&quot;'
                   '&gt;&lt;/iframe&gt;</div>')
    f = _Fetcher(pages={"page=1": page1, "page=2": "<div></div>"})
    rows = c.handler_dealrelations("https://mainstreetmain.com/our-listings", iframe_page, "msm", f)
    titles = {r["title"] for r in rows}
    assert not any("Real Estate for Sale" in t and t.startswith("2nd Generation Restaurant") for t in titles)
    assert all(r["url"].startswith("https://wesellmainstreet.dealrelations.com/listings/") for r in rows)
    assert c.DISPLAYED["mainstreetmain.com"]["count"] == 30


def test_dr_handler_refuses_other_domains():
    _install_helpers()
    html = fx("dr_sunbelt_page1.html")
    f = _Fetcher()
    assert c.handler_dealrelations("https://someotherbroker.com/listings", html, "x", f) is None


# ── BBF office feed ──────────────────────────────────────────────────────────
def test_bbf_rows_dedupe_across_categories_and_keep_distinct():
    rows = c.bbf_rows(fx("bbf_own.html"))
    nums = {r["number"] for r in rows}
    assert {"BBF-170-04312", "BBF-170-04314", "BBF-170-08515", "BBF-170-08516",
            "BBF-170-18724", "BBF-170-03635", "BBF-170-05340", "BBF-170-08530"} == nums
    kept, dups = c.bbf_dedupe(rows)
    assert sorted(dups) == [("BBF-170-04314", "BBF-170-04312"), ("BBF-170-08516", "BBF-170-08515")]
    kept_nums = {r["number"] for r in kept}
    assert {"BBF-170-18724", "BBF-170-03635"} <= kept_nums          # distinct ones pass
    assert len(kept) == 6


def test_bbf_contract_pending_flag_is_read():
    rows = {r["number"]: r for r in c.bbf_rows(fx("bbf_own.html"))}
    assert "contract pending" in rows["BBF-170-18724"]["flags"].lower()
    assert "contract pending" not in rows["BBF-170-03635"]["flags"].lower()
    assert rows["BBF-170-18724"]["price"] == 1595000.0


def test_bbf_other_offices_are_never_read():
    other = fx("bbf_own.html").replace("BBF-170-", "BBF-999-")
    assert c.bbf_rows(other) == []


# ── bossgi ───────────────────────────────────────────────────────────────────
def test_bossgi_cards_and_price_na():
    cards = c.listingbox_cards(fx("bossgi_cards.html"), "https://bossgi.com/featured-businesses-for-sale/")
    assert len(cards) == 3
    assert all(x["href"].startswith("https://bossgi.com/listing/") for x in cards)
    assert all(x["internal_id"].startswith("4110-") for x in cards)
    assert c.money(cards[0]["price_text"]) is not None


def test_bossgi_mls_feed_page_is_never_a_source():
    _install_helpers()
    html = fx("bossgi_cards.html")
    assert c.handler_bossgi("https://bossgi.com/buy-a-business/", html, "b", _Fetcher()) is None
    rows = c.handler_bossgi("https://bossgi.com/featured-businesses-for-sale/", html, "b", _Fetcher())
    assert len(rows) == 3 and all(r["url"] != "https://bossgi.com/featured-businesses-for-sale/" for r in rows)


# ── blbrokers ────────────────────────────────────────────────────────────────
def test_bl_cards_active_list_only_deduped_and_counter():
    html = fx("bl_cards.html")
    cards = c.bl_cards(html, "https://blbrokers.com/businesses-for-sale")
    assert len(cards) == 4
    doubled = c.bl_cards(html + html, "https://blbrokers.com/businesses-for-sale")
    assert len(doubled) == 4                   # desktop + mobile layouts render each card twice
    assert all(x["number"] for x in cards)
    assert all("BottomLine" not in x["title"] for x in cards)
    assert c.bl_counter(html) == (12, 18)
    sold = [x for x in c.bl_cards(html.replace("listing in result.page", "listing in X")
                                  .replace("listing in result.extraPage", "listing in result.page"),
                                  "https://blbrokers.com/")]
    assert sold, "fixture must contain the Sold! carousel cards"


def test_bl_sold_carousel_cards_are_not_in_the_active_list():
    html = fx("bl_cards.html")
    hrefs = {x["href"] for x in c.bl_cards(html, "https://blbrokers.com/businesses-for-sale")}
    swapped = html.replace("listing in result.page", "listing in X") \
                  .replace("listing in result.extraPage", "listing in result.page")
    sold_hrefs = {x["href"] for x in c.bl_cards(swapped, "https://blbrokers.com/businesses-for-sale")}
    assert hrefs and sold_hrefs and not (hrefs & sold_hrefs)


# ── NewLeaf / Legacy ─────────────────────────────────────────────────────────
def test_legacy_status_under_contract_stays_active_with_label():
    _install_helpers()
    items = [{"Status": "Active"}, {"Status": "Under Contract"}, {"Status": "Coming Soon"}, {"Status": "Sold"}]
    got = [(st, lab) for _i, st, lab in c.legacy_rows(items, "https://newleafbrokerage.com/", "newleafbrokerage.com")]
    assert got[0] == ("active", None)
    assert got[1] == ("active", "Under Contract")
    assert got[2][0] == "DROP"
    assert got[3][0] == "sold"


def test_newleaf_never_crawls_cre():
    _install_helpers()
    html = "<legacy-listings company='x'></legacy-listings>"
    assert c.handler_newleaf("https://newleafbrokerage.com/cre/listings/", html, "n", _Fetcher()) is None


# ── 7cint ────────────────────────────────────────────────────────────────────
def _c7(**kw):
    base = {"Id": 1, "ListingType": 1, "ListingHeadline": "Smoke Shop Plano", "AskingPrice": 70000,
            "CashFlow": "12000", "LocationConfidentialityExposure": "Plano, Collin-Texas",
            "Description": "<p>Nice<br />shop</p>", "IsAssociatedRealEstate": "2"}
    base.update(kw)
    return base


def test_c7_scope_drops_only_the_named_kinds():
    assert c.c7_scope(_c7()) is None                                   # plain business passes
    assert c.c7_scope(_c7(ListingType=2)) is None                      # asset sale passes
    assert c.c7_scope(_c7(ListingType=3, ListingHeadline="Dallas Car Wash", IsAssociatedRealEstate="1")) is None
    assert c.c7_scope(_c7(ListingType=4)) is not None                  # lease
    assert c.c7_scope(_c7(ListingHeadline=None)) is not None           # site skips null headline
    assert c.c7_scope(_c7(ListingHeadline="Standalone Building")) is not None
    assert c.c7_scope(_c7(ListingHeadline="Standalone Building Bakery")) is None   # a business with a building word


def test_c7_row_fields():
    _install_helpers()
    row = c.c7_row(_c7(), "https://7cint.com/buying", "7c", "7cint.com")
    assert row["url"] == "https://7cint.com/buying/details?name=Smoke%20Shop%20Plano"
    assert row["state"] == "TX" and row["city"] == "Plano"
    assert row["asking_price"] == 70000.0 and row["cash_flow"] == 12000.0
    assert row["status"] == "active"


def test_c7_handler_paginates_and_logs_exclusions():
    _install_helpers()
    data = [_c7(Id=i, ListingHeadline=f"Biz {i}") for i in range(1, 8)]
    data.append(_c7(Id=90, ListingType=4, ListingHeadline="Space for rent"))

    def route(url, params):
        p = int(params["page"]); n = int(params["pageSize"])
        return {"success": True, "totalRecords": len(data), "data": data[(p - 1) * n:p * n]}
    f = _Fetcher(routes={"/Buying/Filter": route})
    c.SCOPE_LOG.pop("7cint.com", None)
    rows = c.handler_7cint("https://7cint.com/buying", '<script>url: "/Buying/Filter"</script>', "7c", f)
    assert len(rows) == 7
    assert any("Space for rent" in t for t, _w in c.SCOPE_LOG["7cint.com"])


# ── money / state helpers ────────────────────────────────────────────────────
def test_money_and_state_helpers():
    assert c.money("$1,595,000") == 1595000.0
    assert c.money("1,595,000") == 1595000.0
    assert c.money("$2.5m") == 2500000.0
    assert c.money("N/A") is None
    assert c.money("") is None
    assert c.state_code("Florida") == "FL" and c.state_code("fl") == "FL" and c.state_code("Nowhere") is None
