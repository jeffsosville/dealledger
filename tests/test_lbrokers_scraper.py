"""lbrokers.com parser (scrapers/lbrokers_scraper.py).

Fixture: the 18 detail pages linked from https://www.lbrokers.com/stores-for-sale
on 2026-11-19 (h1, Location line, stat columns, and whether the page says it is
a new build-out / equipment package), rebuilt in the site's markup.
"""
import html
import json
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from lbrokers_scraper import detail_urls, money, parse_detail, split_h1  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "lbrokers_pages.json")
NB_TEXT = "This is not a business sale. No existing business, revenue, or customer base is being transferred."


def _page(h1, loc, stats, nb):
    price = h1.split("$", 1)[1] if "$" in h1 else ""
    head = h1.split("$", 1)[0]
    cols = "".join(
        f'<div class="col js-stat-col"><div class="title-wrapper">{html.escape(v)}'
        f'<div class="stat-title section-subtitle">{html.escape(lab)}</div></div></div>' for v, lab in stats)
    return (f'<html><body><header><nav>Vacant Laundry Invest In a Laundromat</nav></header><main>'
            f'<h1 class="section-title">{html.escape(head)}<br><strong>${html.escape(price)}</strong></h1>'
            + (f'<p>Type: For Sale Listing ID: 1 Location: {html.escape(loc)} Previous</p>' if loc else '')
            + (f'<p>{NB_TEXT}</p>' if nb else '<p>Established store with loyal customers.</p>')
            + f'<div class="stats-row inview">{cols}</div><p>Interested? Request more details today</p>'
            + '<p>Disclaimer: All figures are projected.</p></main><footer>f</footer></body></html>')


def _all():
    out = {}
    for slug, h1, loc, stats, nb in json.load(open(FIX)):
        row, why = parse_detail(f"https://www.lbrokers.com/coin-laundry-stores-for-sale/{slug}",
                                _page(h1, loc, stats, nb))
        out[slug] = row or why
    return out


def test_new_builds_are_not_emitted():
    r = _all()
    nb = sorted(k for k, v in r.items() if v == "new_build")
    assert nb == ["burlington-nc-215", "charleston-south-carolina-221", "louisburg-nc-laundry-for-sale",
                  "norfolk-va", "richmond-va-laundry-for-sale"]


def test_counts():
    rows = [v for v in _all().values() if isinstance(v, dict)]
    assert len(rows) == 13
    assert Counter(r["status"] for r in rows) == {"active": 11, "sold": 2}
    assert all(r["state"] == "FL" for r in rows)
    assert all(r["business_type"] == "Laundromat" for r in rows)


def test_figures():
    r = _all()
    m = r["miami-gardens-laundry-for-sale"]
    assert (m["status"], m["broker_status_text"], m["price"], m["cash_flow"], m["city"]) == \
        ("sold", "SOLD", 850000.0, 170000.0, "Miami Gardens")
    t = r["temple-terrace-fl2"]
    assert (t["status"], t["broker_status_text"], t["price"], t["revenue"], t["cash_flow"]) == \
        ("active", "Sale Pending", 425000.0, 400000.0, 130000.0)
    h = r["homestead-fl"]
    assert (h["status"], h["price"], h["cash_flow"], h["city"], h["state"]) == ("sold", 95000.0, 45000.0, "Homestead", "FL")
    assert r["south-florida-206"]["price"] == 1500000.0
    assert (r["south-florida-206"]["city"], r["south-florida-206"]["state"]) == ("Palm Beach County", "FL")
    assert r["haines-city-fl-laundry-for-sale"]["revenue"] == 350000.0
    s = r["southwest-florida-211"]
    assert (s["status"], s["broker_status_text"], s["price"]) == ("active", "PENDING", 1200000.0)


def test_helpers():
    assert split_h1("PENDING- Richmond, VA Laundry For Sale $799,000.00") == \
        ("Richmond, VA Laundry For Sale", "PENDING", 799000.0)
    assert split_h1("Burlington, NC Laundry For Sale $489,000- SOLD")[1] == "SOLD"
    assert money("$45,000-50,000") == 45000.0
    assert money("$200,000+") == 200000.0


def test_index_links():
    idx = ('<a href="/coin-laundry-stores-for-sale/norfolk-va">x</a>'
           '<a href="/coin-laundry-store-for-sale/south-florida-173">y</a>'
           '<a href="/list-your-coin-laundry-store-for-sale/laundry-valuation">z</a>'
           '<a href="/coin-laundry-stores-for-sale/norfolk-va">dup</a>')
    assert detail_urls(idx) == ["https://www.lbrokers.com/coin-laundry-stores-for-sale/norfolk-va",
                                "https://www.lbrokers.com/coin-laundry-store-for-sale/south-florida-173"]
