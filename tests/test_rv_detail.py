"""rv-detail (2026-11-24): progressivepracticesales.com and laskacompany.com detail readers.

Fixtures in tests/fixtures/rv are trimmed copies of the live pages (24 Nov 2026).
Principle 8: each narrowing read is paired with input that must still pass.
"""
import hashlib
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import cr_w3_platforms as w3  # noqa: E402
import rv_detail as rd  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

FIX = Path(__file__).parent / "fixtures" / "rv"


def fx(n):
    return (FIX / n).read_text(errors="replace")


def _helpers():
    w3._H.update({"make": v6._make_api_listing, "status": v6._ls_lookup,
                  "needs_proxy": v6._needs_proxy, "user_agents": v6.USER_AGENTS,
                  "stable_id": v6.stable_listing_id})


# ---- progressivepracticesales.com ----------------------------------------

def test_cards_one_per_post_id_and_duplicate_titles_stay_separate():
    cards = rd.pps_cards(fx("pps_index.html"))
    assert len(cards) == 7
    man = [c for c in cards if c["location"] == "Manhattan, NY"]
    assert len(man) == 2 and man[0]["post_id"] != man[1]["post_id"] and man[0]["href"] != man[1]["href"]
    assert any(t for c in cards for t in c["tags"])


def test_detail_reads_collections_profit_type_and_coming_soon_is_none():
    sd = rd.pps_detail(fx("pps_detail_san_diego.html"))
    assert sd == {"practice_type": "Chiropractic Practice", "collections": 992000.0,
                  "profit": None, "years": 31}                       # Clinic Profit: Coming Soon
    co = rd.pps_detail(fx("pps_detail_central_or.html"))
    assert (co["collections"], co["profit"], co["years"]) == (339000.0, 139000.0, 47)


def test_title_state_and_no_price():
    _helpers()
    cards = rd.pps_cards(fx("pps_index.html"))
    pages = {c["href"]: fx("pps_detail_central_or.html") for c in cards}
    rows = rd.handler_pps("https://progressivepracticesales.com/practices-for-sale/", fx("pps_index.html"),
                          "Progressive Practice Sales", None, sleep=0, fetch=lambda u: pages[u])
    assert len(rows) == 7
    co = next(r for r in rows if "central-oregon" in r["url"])
    assert co["title"] == "Chiropractic Practice – Central OR" and co["state"] == "OR"
    assert co["asking_price"] is None                                # no asking price on the site
    assert co["revenue"] == 339000.0 and co["cash_flow"] == 139000.0
    assert all(r["status"] == "active" for r in rows)


def test_row_ids_are_the_unchanged_url_hash_and_duplicate_titles_get_distinct_ids():
    _helpers()
    cards = rd.pps_cards(fx("pps_index.html"))
    rows = rd.handler_pps("https://progressivepracticesales.com/practices-for-sale/", fx("pps_index.html"),
                          "Progressive Practice Sales", None, sleep=0, fetch=lambda u: "")
    by_url = {r["url"]: r["id"] for r in rows}
    dan = "https://progressivepracticesales.com/danville-illinois/"
    assert by_url[dan] == hashlib.sha256(dan.encode()).hexdigest()[:16] == "007c6c847ba60eb6"
    assert len(set(by_url.values())) == len(rows)
    man = [r for r in rows if r["title"].endswith("Manhattan, NY")]
    assert len(man) == 2 and man[0]["id"] != man[1]["id"]


def test_failed_detail_fetch_keeps_the_row_without_figures():
    _helpers()

    def boom(u):
        raise RuntimeError("x")
    rows = rd.handler_pps("https://progressivepracticesales.com/practices-for-sale/", fx("pps_index.html"),
                          "b", None, sleep=0, fetch=boom)
    assert len(rows) == 7 and all(r["revenue"] is None and r["asking_price"] is None for r in rows)
    assert all(r["title"].startswith("Chiropractic Practice – ") for r in rows)


def test_pps_handler_ignores_other_domains_and_pages_without_cards():
    assert rd.handler_pps("https://example.com/x", fx("pps_index.html"), "b", None) is None
    assert rd.handler_pps("https://progressivepracticesales.com/x/", "<html></html>", "b", None) is None
    assert ("e-loop-item-", rd.handler_pps) in v6.API_HANDLERS


# ---- laskacompany.com -----------------------------------------------------

def test_laska_labelled_fields():
    r = rd.laska_detail(fx("laska_retail.html"))
    assert (r["price"], r["revenue"], r["cash_flow"], r["asset_value"]) == (1850000.0, 2789570.0, 356792.0, 850000.0)
    assert r["state"] == "ID" and r["state_source"] == "page" and r["year"] == 2009


def test_laska_contact_agent_and_not_disclosed_are_no_value():
    b = rd.laska_detail(fx("laska_bond.html"))                       # Cash Flow: Contact Agent
    assert (b["price"], b["revenue"], b["cash_flow"]) == (110000.0, 221000.0, None)
    p = rd.laska_detail(fx("laska_pickle.html"))                     # Not Disclosed x3
    assert (p["price"], p["revenue"], p["cash_flow"], p["asset_value"]) == (60000.0, None, None, None)


def test_laska_state_from_page_else_default_id():
    assert rd.laska_detail(fx("laska_pickle.html"))["state_source"] == "page"      # "Bonneville County, ID"
    b = rd.laska_detail(fx("laska_bond.html"))                                      # "Canyon County"
    assert b["state"] == "ID" and b["state_source"] == "default"
    assert rd.laska_detail(fx("laska_bond.html"), default_state=None)["state"] is None
    # a page that names another state is not overridden by the default
    html = fx("laska_retail.html").replace("Ada County, ID", "Spokane, WA")
    assert rd.laska_detail(html)["state"] == "WA"


def test_laska_asset_only_listing_keeps_price_and_no_revenue_or_cashflow():
    d = rd.laska_detail(fx("laska_dog.html"))
    assert d["price"] == 39900.0 and d["revenue"] is None and d["cash_flow"] is None


def test_laska_enrich_fills_missing_and_never_overwrites():
    class R:
        status_code = 200

        def __init__(self, t):
            self.text = t

    class S:
        def get(self, url, timeout=0):
            return R(fx("laska_retail.html"))

    class Scraper:
        domain = "www.laskacompany.com"
        session = S()

    a = {"status": "active", "listing_url": "u1", "price": None, "revenue": None, "cash_flow": None, "state": None}
    b = {"status": "active", "listing_url": "u2", "price": 1.0, "revenue": 2.0, "cash_flow": None, "state": "OR"}
    c = {"status": "inactive", "listing_url": "u3", "price": None}
    rd.enrich_wp(Scraper(), [a, b, c], verbose=False, sleep=0)
    assert (a["price"], a["revenue"], a["cash_flow"], a["state"]) == (1850000.0, 2789570.0, 356792.0, "ID")
    assert (b["price"], b["revenue"], b["cash_flow"], b["state"]) == (1.0, 2.0, 356792.0, "OR")
    assert c["price"] is None                                         # only ACTIVE listings are fetched
    assert rd.handles("www.laskacompany.com") and not rd.handles("synergybb.com")
