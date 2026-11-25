"""Route brokers, 2026-11-20 (n1): commercialroutesforsale.com, routebrokers.com,
routeworldbrokers.com specialized parsers; routesforsale.net count + fail-loud;
WP-REST multi post type; the runner's scraper-captured displayed count.

Fixtures are the brokers' own pages fetched 2026-11-20 ~12:50 UTC (trimmed to the
listing markup)."""
import os
import sys

import pytest

HERE = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(HERE, "..", "scrapers"))

import commercialroutes_scraper as CR  # noqa: E402
import routebrokers_scraper as RB  # noqa: E402
import routeworld_scraper as RW  # noqa: E402

FIX = os.path.join(HERE, "fixtures")


def _read(name):
    with open(os.path.join(FIX, name), encoding="utf-8") as f:
        return f.read()


# ── commercialroutesforsale.com ─────────────────────────────────────────────
def test_commercialroutes_both_row_classes_and_placeholder():
    g = CR.parse_grid(_read("commercialroutes_routelist.html"))
    assert g["grid_rows"] == 61
    assert g["skipped_placeholder"] == 1                 # 6107 "call us"
    ids = [r["route_id"] for r in g["rows"]]
    assert len(ids) == 60 == len(set(ids))
    # both GridView row styles (even 6250-style and odd 6251-style rows)
    assert "6251" in ids and "6250" in ids
    assert "6107" not in ids


def test_commercialroutes_rows_title_price_not_description():
    rows = CR.to_rows(CR.parse_grid(_read("commercialroutes_routelist.html"))["rows"])
    r = next(x for x in rows if x["listing_url"].endswith("RouteID=6251"))
    assert r["listing_url"] == "https://commercialroutesforsale.com/RouteListDetail.aspx?RouteID=6251"
    assert r["title"] == "Wise Snacks Route - Brooklyn, NY"
    assert r["price"] == 219_000
    assert r.get("revenue") in (None, 0)                 # Weekly Net is not revenue
    assert r.get("cash_flow") in (None, 0)               # nor annualised cash flow
    assert "Weekly net $2,000" in r["description"]
    assert all(len(x["title"]) < 90 for x in rows)       # no 500-char description titles
    assert all(x["price"] for x in rows)


def test_commercialroutes_missing_grid_fails_loud():
    with pytest.raises(ValueError):
        CR.parse_grid("<html><body><p>Service unavailable</p></body></html>")


# ── routebrokers.com ────────────────────────────────────────────────────────
def test_routebrokers_twelve_items_keyed_by_ref():
    rows = RB.to_rows(RB.parse_items(_read("routebrokers_routes_for_sale.html")))
    assert len(rows) == 12
    urls = [r["listing_url"] for r in rows]
    assert len(set(urls)) == 12
    assert "https://routebrokers.com/routes-for-sale#dl-br-1273" in urls
    assert "https://routebrokers.com/routes-for-sale#dl-mr-softee-territories" in urls


def test_routebrokers_prices_down_payments_and_status():
    by = {r["listing_url"].rsplit("#dl-", 1)[1]: r for r in
          RB.to_rows(RB.parse_items(_read("routebrokers_routes_for_sale.html")))}
    assert by["br-1273"]["price"] == 239_000
    assert by["ms-1265"]["price"] == 1_500_000           # "$1.500,000"
    assert by["cn-124"]["price"] == 299_000              # "Asking: $299,000"
    # "$149,000 down" / "Asking Price: $189,000 down" are down payments, not prices
    assert by["br-1271"]["price"] is None and by["br-1271"]["price_text"] == "$149,000 down"
    assert by["ju-403"]["price"] is None
    assert by["ju-403"]["broker_status_text"] == "DEPOSIT TAKEN"
    assert by["ju-403"]["status"] == "active"
    assert by["ms-1265"]["state"] == "ND"
    assert by["br-1273"]["state"] == "AZ"
    # "14 Routes" inside MS-1265 is a fact of that listing, not a count
    assert "14 Routes" in by["ms-1265"]["description"]


# ── routeworldbrokers.com ───────────────────────────────────────────────────
def test_routeworld_priced_blocks_only():
    p = RW.parse_blocks(_read("routeworld_listings_page.html"))
    rows = RW.to_rows(p["items"])
    assert [r["price"] for r in rows] == [165_000, 400_000, 21_000]
    assert all(r["state"] == "NY" for r in rows)
    assert all("#dl-" in r["listing_url"] for r in rows)
    titles = " ".join(r["title"] for r in rows)
    assert "ATTENTION" not in titles and "ROUTE WORLD SERVES" not in titles


# ── routesforsale.net ───────────────────────────────────────────────────────
class _Resp:
    def __init__(self, text, status=200):
        self.text, self.status_code = text, status


class _Sess:
    def __init__(self, resp):
        self.resp = resp

    def get(self, *a, **k):
        return self.resp


def _rfs(resp, monkeypatch):
    import specialized_scrapers as S
    monkeypatch.setattr(S.RoutesForSaleScraper, "_make_session", classmethod(lambda c: _Sess(resp)))
    return S.RoutesForSaleScraper()


def test_routesforsale_count_and_category(monkeypatch):
    import collections
    s = _rfs(_Resp(_read("routesforsale_route_listings.html")), monkeypatch)
    items = s.scrape("13461", verbose=False)
    assert len(items) == 288
    dc = type(s).last_displayed
    assert dc["count"] == 288 and dc["source"] == "counted"
    cats = collections.Counter(i["business_type"] for i in items)
    assert cats["Flowers Bread"] == 85 and cats["Mission's Tortilla"] == 65
    assert cats["Bimbo Bakery"] == 24 and cats["Pepperidge Farm"] == 10


@pytest.mark.parametrize("resp", [_Resp("<html><title>Just a moment...</title></html>", 403),
                                  _Resp("<html><body>maintenance</body></html>", 200)])
def test_routesforsale_bad_page_fails_loud(monkeypatch, resp):
    s = _rfs(resp, monkeypatch)
    with pytest.raises(RuntimeError) as e:
        s.scrape("13461", verbose=False)
    assert f"HTTP {resp.status_code}" in str(e.value)
    assert type(s).last_displayed is None


# ── WP-REST: several post types, POST-grid count ────────────────────────────
def test_wprest_multiple_rest_bases(monkeypatch):
    import specialized_scrapers as S
    w = S.WPRestScraper("www.example-routes.com", "pnd|linehaul")
    assert w.rest_bases == ["pnd", "linehaul"]
    calls = []

    class R:
        def __init__(self, items):
            self.status_code, self.headers = 200, {"content-type": "application/json",
                                                     "x-wp-totalpages": "1", "x-wp-total": str(len(items))}
            self._items = items
            import json as _json
            self.content = _json.dumps(items).encode()  # scraper now parses r.content

        def json(self):
            return self._items

    def fake_get(url):
        calls.append(url)
        base = url.split("/wp/v2/")[1].split("?")[0]
        n = {"pnd": 1, "linehaul": 2}[base]
        return R([{"title": {"rendered": f"{base} route {i}"},
                   "link": f"https://www.example-routes.com/{base}/r{i}/", "slug": f"r{i}"}
                  for i in range(n)])

    monkeypatch.setattr(w, "_get", fake_get)
    out = w.scrape("x", verbose=False)
    assert len(out) == 3
    assert any("/wp/v2/pnd?" in c for c in calls) and any("/wp/v2/linehaul?" in c for c in calls)


def test_single_rest_base_unchanged():
    import specialized_scrapers as S
    w = S.WPRestScraper("eatz-associates.com", "listing")
    assert w.rest_bases == ["listing"] and w._api_url(2).startswith(
        "https://eatz-associates.com/wp-json/wp/v2/listing?per_page=100&page=2")


# ── runner: scraper-captured displayed count ────────────────────────────────
def test_runner_scraper_displayed_hook():
    import run_specialized as RS
    for key in ("commercialroutes", "routebrokers", "routeworld", "routesforsale"):
        assert callable(RS.BROKERS[key].get("displayed")), key
    meta = {"displayed": lambda: {"count": 60, "source": "counted", "note": "grid"}}
    listings = [{"status": "active"}] * 60
    kw = RS._displayed_count_for("x", meta, listings)
    assert kw["displayed_count"] == 60 and kw["displayed_source"] == "counted"
    assert kw["compared_count"] == 60
    assert RS._scraper_displayed("x", {"displayed": lambda: None}, listings) is None
    assert RS._scraper_displayed("x", {}, listings) is None


def test_runner_post_index_gets_displayed_hook():
    import run_specialized as RS
    import specialized_scrapers as S
    meta = RS.BROKERS["wp_listings_routeconsultant_com"]
    assert callable(meta.get("displayed"))
    S.WPRestScraper.last_index_cards["listings.routeconsultant.com"] = 236
    assert meta["displayed"]()["count"] == 236
    S.WPRestScraper.last_index_cards.pop("listings.routeconsultant.com", None)
    assert meta["displayed"]() is None
    assert RS.BROKERS["wp_usroutesales_com"]["domain"] == "usroutesales.com"


def test_v6_skips_new_specialized_domains():
    import dealledger_scraper_v6 as V6
    for d in ("commercialroutesforsale.com", "www.routebrokers.com", "routeworldbrokers.com"):
        assert V6.is_blocked(d), d
    # a different company whose name ends the same way is not caught
    assert not V6.is_blocked("poolroutebrokers.com")
