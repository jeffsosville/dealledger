"""Unit tests for scrapers/displayed_count.py (W6, 2026-11-11).

Run (after `git apply w6-displayed_count.patch`, or with SCRAPERS pointing at a
patched copy):
    python3 -m pytest claude/staging/2026-11-11/w6-test_displayed_count.py -q
Includes the principle-8 side: pages that must NOT yield a count.
"""
import os
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.environ.get("SCRAPERS", os.path.join(REPO, "scrapers")))

from displayed_count import (  # noqa: E402
    DisplayedCountTracker, count_cards, count_verdict, parse_stated_count, verdict_label)


def page(body):
    return f"<html><head><title>x</title><script>var n='Showing 999 items';</script></head><body>{body}</body></html>"


# ── each phrase form ────────────────────────────────────────────────────────
@pytest.mark.parametrize("body,expected", [
    ("<div class='count'>Showing 62 items</div>", 62),          # vikingmergers.com
    ("<p>Showing 33</p>", 33),
    ("<p>Showing <span>1</span>-<span>12</span> of <b>85</b> results</p>", 85),
    ("<p>Showing 1 to 20 of 1,204 listings</p>", 1204),
    ("<p>1–12 of 85</p>", 85),
    ("<h2>Properties (14)</h2>", 14),
    ("<a>For Sale (23)</a>", 23),
    ("<h3>Listings (9)</h3>", 9),
    ("<p>42 results</p>", 42),
    ("<p>17 listings found</p>", 17),
    ("<span>8 businesses for sale</span>", 8),
])
def test_phrase_forms(body, expected):
    r = parse_stated_count(page(body))
    assert r is not None and r["count"] == expected and r["source"] == "stated"
    assert r["text"]


def test_largest_showing_wins_and_rest_noted():
    # vikingmergers: real grid "Showing 62 items" + a featured widget "Showing 7 items"
    r = parse_stated_count(page("<p>Showing 62 items</p><aside>Showing 7 items</aside>"))
    assert r["count"] == 62 and "7" in r["note"]


def test_range_beats_bare_noun():
    r = parse_stated_count(page("<p>500 businesses sold since 1999</p><p>Showing 1-10 of 48</p>"))
    assert r["count"] == 48


# ── sectioned counts: For Sale + Under Contract (decision 6), others in note ─
def test_sectioned_for_sale_plus_under_contract():
    body = ("<ul><li><span>For Sale</span><span>(56)</span></li>"
            "<li><span>Under Contract</span><span>(16)</span></li>"
            "<li><span>Recently Sold</span><span>(964)</span></li></ul>")
    r = parse_stated_count(page(body))
    assert r["count"] == 72                     # 56 for sale + 16 under contract
    assert "under_contract=16" in r["note"] and "sold=964" in r["note"]
    assert "decision 6" in r["note"]


def test_sectioned_facetwp_in_script_json():
    # eatz-associates.com: FacetWP preloads facet HTML inside a JSON string
    blob = ('<script>window.FWP_JSON={"facets":{"status":"<div class=\\"facetwp-checkbox\\" '
            'data-value=\\"for-sale\\"><span class=\\"facetwp-display-value\\">For Sale<\\/span>'
            '<span class=\\"facetwp-counter\\">(56)<\\/span><\\/div><div class=\\"facetwp-checkbox\\" '
            'data-value=\\"under-contract\\"><span class=\\"facetwp-display-value\\">Under Contract'
            '<\\/span><span class=\\"facetwp-counter\\">(16)<\\/span><\\/div>",'
            '"type":"<div class=\\"facetwp-checkbox\\" data-value=\\"business-for-sale\\">'
            '<span class=\\"facetwp-display-value\\">Business For Sale<\\/span>'
            '<span class=\\"facetwp-counter\\">(774)<\\/span><\\/div>"}};</script>')
    r = parse_stated_count(page(blob))
    assert r["count"] == 72, r          # 56 + 16; not 774 ("Business For Sale" sale-type facet)
    assert "under_contract=16" in r["note"]


def test_business_for_sale_label_is_not_for_sale_section():
    body = "<li>Business For Sale (774)</li><li>Under Contract (16)</li>"
    assert parse_stated_count(page(body)) is None or parse_stated_count(page(body))["count"] != 774


# ── no-count pages -> None (false-accept guards) ────────────────────────────
@pytest.mark.parametrize("body", [
    "<h1>Businesses for Sale</h1><div>Asking Price: $250,000</div>",
    "<p>We have sold over 500 businesses since 1985.</p>",
    "<p>Over 1,700 listings sold</p>",
    "<p>Call (866) 432-2410</p><p>Established 2004, 8 employees</p>",
    "<p>Showing 0 items</p>",
    "",
])
def test_no_count(body):
    assert parse_stated_count(page(body)) is None


def test_script_text_ignored():
    assert parse_stated_count(page("<p>nothing here</p>")) is None   # 'Showing 999 items' is in <script>


# ── counted fallback ────────────────────────────────────────────────────────
def test_count_cards_price_labels():
    cards = "".join(f"<div class='card'><h3>Biz {i}</h3><p>Asking Price: $1{i}0,000</p></div>" for i in range(33))
    assert count_cards(page(cards)) == {"count": 33, "family": "price_label"}


def test_count_cards_price_without_colon():
    # calhouncompanies.com cards: "Price $450,000 Gross Sales $926,280"
    cards = "".join(f"<div><h3>Biz {i}</h3><span>Price</span> <span>${i+1}50,000</span>"
                    f"<span>Gross Sales</span> <span>$900,000</span></div>" for i in range(10))
    assert count_cards(page(cards)) == {"count": 10, "family": "price_label"}


def test_count_cards_price_sort_option_not_counted():
    sort = "<select><option>Price (Low to High)</option><option>Price (High to Low)</option></select>"
    assert count_cards(page(sort + "<p>Price range</p>")) is None


def test_count_cards_table_rows():
    rows = "".join(f"<tr class='{'even' if i % 2 else 'odd'}'><td>{6200+i}</td><td>Route</td><td>${100+i},000</td></tr>"
                   for i in range(60))
    html = page(f"<table><tr><th>No</th><th>Type</th><th>Price</th></tr>{rows}"
                "<tr><td>Coming soon</td><td></td><td>Call</td></tr></table>")
    assert count_cards(html) == {"count": 60, "family": "table_rows"}


def test_count_cards_too_few():
    assert count_cards(page("<p>Price: $10</p><p>More Info</p>")) is None


def test_tracker_stated_wins_and_pages_dedupe():
    t = DisplayedCountTracker()
    p1 = page("<p>Showing 1-2 of 30</p>" + "<p>Price: $1</p>" * 5)
    t.add_page(p1, count=False)
    t.add_page(p1)
    t.add_page(p1)                       # repeated page counts once
    assert t.result()["count"] == 30 and t.result()["source"] == "stated"
    assert t.counted == 5


def test_tracker_counted_sums_pages():
    t = DisplayedCountTracker()
    t.add_page(page("<p>Price: $1</p>" * 12 + "<i>p1</i>"))
    t.add_page(page("<p>Price: $1</p>" * 5 + "<i>p2</i>"))
    r = t.result()
    assert r["count"] == 17 and r["source"] == "counted"


def test_tracker_nothing():
    t = DisplayedCountTracker()
    t.add_page(page("<p>hello</p>"))
    assert t.result() is None


def test_tracker_never_raises():
    t = DisplayedCountTracker()
    t.add_page(None)
    t.add_page(12345)                    # garbage in, no exception
    assert t.result() is None


# ── verdict ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("seen,displayed,verdict", [
    (62, 62, True), (56, 62, True), (68, 62, True),
    (55, 62, False), (69, 62, False),
    (411, 62, False), (1, 33, False), (30, 61, False),
    (10, None, None), (10, 0, None),
])
def test_verdict(seen, displayed, verdict):
    assert count_verdict(seen, displayed) is verdict


def test_verdict_label():
    assert verdict_label(411, 62) == "INCOMPLETE_HIGH"
    assert verdict_label(1, 33) == "INCOMPLETE_LOW"
    assert verdict_label(33, 33) == "COMPLETE"
    assert verdict_label(5, None).startswith("UNKNOWN")


# ── crawl_run_log._count_check: what gets written ──────────────────────────
def test_count_check_stated_closes_partial():
    import crawl_run_log as c
    st, err, ok = c._count_check("ok", None, 411, 62, "stated")
    assert st == "partial" and ok is False and err.startswith("COUNT_MISMATCH")
    assert st in c._VALID_STATUS          # principle 17: must be in the CHECK


def test_count_check_counted_is_advisory():
    import crawl_run_log as c
    st, err, ok = c._count_check("ok", None, 18, 33, "counted")
    assert st == "ok" and ok is False and err is None


def test_count_check_keeps_failed_and_truncated():
    import crawl_run_log as c
    assert c._count_check("failed", "EMPTY", 0, 33, "stated")[0] == "failed"
    assert c._count_check("partial", "TRUNCATED", 2, 333, "stated")[:2] == ("partial", "TRUNCATED")
    assert c._count_check("ok", None, 62, 62, "stated") == ("ok", None, True)
    assert c._count_check("ok", None, 62, None, None) == ("ok", None, None)


def test_displayed_fields_empty_without_count():
    import crawl_run_log as c
    assert c._displayed_fields(None, None, None, None) == {}
    f = c._displayed_fields(62, "stated", "matched: Showing 62 items", False)
    assert f == {"broker_displayed_count": 62, "broker_displayed_count_source": "stated",
                 "broker_displayed_note": "matched: Showing 62 items", "count_complete": False}


def test_finish_run_retries_without_new_columns(monkeypatch):
    """Columns not applied yet -> PostgREST 400 -> close the row anyway."""
    import crawl_run_log as c
    calls = []

    class R:
        def __init__(self, ok): self.ok, self.status_code, self.text = ok, (204 if ok else 400), "PGRST204"

    def fake_patch(url, headers=None, params=None, json=None, timeout=None):
        calls.append(json)
        return R("broker_displayed_count" not in json)

    monkeypatch.setenv("SUPABASE_SERVICE_KEY", "test")
    monkeypatch.setattr(c, "_truncation_check", lambda *a: ("ok", None))
    monkeypatch.setattr(c.requests, "patch", fake_patch)
    c.finish_run(1, "ok", 411, 411, displayed_count=62, displayed_source="stated")
    assert len(calls) == 2
    assert calls[0]["status"] == "partial" and calls[0]["broker_displayed_count"] == 62
    assert calls[1]["status"] == "partial" and "broker_displayed_count" not in calls[1]


def test_sectioned_without_under_contract_is_for_sale_only():
    body = "<ul><li>For Sale (56)</li><li>Recently Sold (964)</li><li>Coming Soon (4)</li></ul>"
    r = parse_stated_count(page(body))
    assert r["count"] == 56                     # coming soon is not emitted (decision 5)
    assert "coming_soon=4" in r["note"]


def test_sectioned_pending_and_loi_variants_add():
    body = "<ul><li>Available (30)</li><li>Sale Pending (5)</li></ul>"
    assert parse_stated_count(page(body))["count"] == 35
    body = "<ul><li>Active (30)</li><li>Under LOI (2)</li></ul>"
    assert parse_stated_count(page(body))["count"] == 32


def test_all_listings_total_not_double_counted():
    body = "<ul><li>All Listings (88)</li><li>Under Contract (16)</li></ul>"
    assert parse_stated_count(page(body))["count"] == 88
