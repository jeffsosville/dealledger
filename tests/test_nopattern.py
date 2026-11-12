"""
Job 3 NOPATTERN class (2026-11-12): brokers whose index page renders listings
but whose nightly V6 crawl failed every night.

  bucket 1  a cached pattern that extracts 0 cards is dropped for the run and
            detection (+ Playwright re-detect) runs; a working entry stored
            only under the www/bare twin is a last resort before NO_PATTERN.
  bucket 3  ML prediction must out-EXTRACT the detected pattern on this page;
            Wix-Studio per-card class tokens are detected (fallback only -
            never when strategies 1-2 succeed); table rows are j3-table_rows'.
  bucket 7  a priced card whose only link is a contact/home button is kept
            as a no-link card instead of dropped by the site-page gate.
Principle 8: every new path is also tested on input that must NOT change.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scrapers"))

import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import (ListingExtractor, PatternCache,  # noqa: E402
                                   PatternDetector, pattern_card_count)

BASE = "https://example-broker.com/listings"


def _cards_html(n=4, cls="biz-card", priced=True, link=True):
    out = []
    for i in range(n):
        price = f"<p>Asking Price: ${(i + 2) * 100},000</p>" if priced else ""
        title = f"Profitable Business Number {i} in Ohio"
        t = (f'<h3><a href="/listing/business-{i}">{title}</a></h3>' if link
             else f"<h3>{title}</h3>")
        out.append(f'<div class="{cls}">{t}{price}<p>Cash Flow: $90,000</p></div>')
    return "<html><body><main>" + "".join(out) + "</main></body></html>"


# ── pattern_card_count ───────────────────────────────────────────────────────

def test_pattern_card_count_counts_extracted_cards_not_matches():
    html = _cards_html(4)
    assert pattern_card_count(html, BASE, "div.biz-card", "x") == 4
    assert pattern_card_count(html, BASE, "div.biz-card", "x", stop_at=1) == 1
    assert pattern_card_count(html, BASE, "div.gone-selector", "x") == 0
    assert pattern_card_count(html, BASE, "main", "x") <= 1     # wrapper, not cards
    assert pattern_card_count(html, BASE, "div[[[bad", "x") == 0  # never raises


# ── PatternCache ─────────────────────────────────────────────────────────────

def test_cache_replace_overrides_keep_the_better_rule(tmp_path):
    pc = PatternCache(str(tmp_path / "pc.json"))
    pc.store("a.com", {"container_selector": "div.old", "total_listings": 50})
    pc.store("a.com", {"container_selector": "div.new", "score": 6})
    assert pc.get("a.com")["container_selector"] == "div.old"   # unchanged rule
    pc.replace("a.com", {"container_selector": "div.new", "score": 6})
    assert pc.get("a.com")["container_selector"] == "div.new"
    assert json.loads((tmp_path / "pc.json").read_text())["a.com"]["container_selector"] == "div.new"


def test_other_spelling(tmp_path):
    pc = PatternCache(str(tmp_path / "pc.json"))
    pc.patterns = {"www.a.com": {"container_selector": "td.x"},
                   "b.com": {"container_selector": "div.y"},
                   "c.com": {"pattern": "div|children:div", "total_listings": 3}}
    assert pc.other_spelling("a.com")["container_selector"] == "td.x"
    assert pc.other_spelling("www.b.com")["container_selector"] == "div.y"
    assert pc.other_spelling("www.a.com") is None
    assert pc.other_spelling("www.c.com") is None      # old-schema entry unusable


# ── the scrape path: stale cache / twin spelling ─────────────────────────────

class _FakeFetcher:
    def __init__(self, html, pw_html=None):
        self.html, self.pw_html = html, pw_html
        self._proxy_domains_runtime = set()
        self.pw_calls = 0
        self.last_scroll_growth = 0

    def fetch(self, url, use_proxy=False):
        if url.rstrip("/") == BASE:
            return self.html, "requests"
        return "<html><body><p>detail</p></body></html>", "requests"

    def fetch_playwright(self, url, use_proxy=False, timeout=30000):
        self.pw_calls += 1
        if self.pw_html is None:
            raise RuntimeError("no browser in tests")
        return self.pw_html

    def close(self):
        pass


def _scraper(tmp_path, monkeypatch, html, cache, pw_html=None):
    monkeypatch.setattr(v6, "HAS_PLAYWRIGHT", True)
    monkeypatch.setattr(v6, "MAX_DETAIL_PAGES", 0, raising=False)
    s = v6.DealLedgerScraper(output_dir=str(tmp_path / "out"), use_supabase=False)
    s.pattern_cache = PatternCache(str(tmp_path / "pc.json"))
    s.pattern_cache.patterns = dict(cache)
    s.fetcher = _FakeFetcher(html, pw_html)
    return s


def _run(s):
    return s._scrape_broker_inner({"name": "example-broker.com", "url": BASE,
                                   "domain": "example-broker.com"})


def test_dead_cached_pattern_is_redetected_and_replaced(tmp_path, monkeypatch, capsys):
    s = _scraper(tmp_path, monkeypatch, _cards_html(4),
                 {"example-broker.com": {"container_selector": "div.row.fl-post-image-beside-wrap",
                                         "count": 10, "score": 16, "total_listings": 40}})
    rows = _run(s)
    out = capsys.readouterr().out
    assert len(rows) == 4
    assert "STALE_CACHE" in out and "healed" in out
    assert s.pattern_cache.get("example-broker.com")["container_selector"] == "div.biz-card"
    ev = s.stats["pattern_cache_events"]
    assert ev == [{"broker": "example-broker.com", "domain": "example-broker.com",
                   "event": "stale_cache",
                   "selector": "div.row.fl-post-image-beside-wrap",
                   "healed_to": "div.biz-card"}]


def test_dead_cache_and_nothing_detectable_fails_loudly(tmp_path, monkeypatch, capsys):
    html = "<html><body><p>We are rebuilding our site.</p></body></html>"
    s = _scraper(tmp_path, monkeypatch, html,
                 {"example-broker.com": {"container_selector": "div.old-card"}})
    assert _run(s) == []
    out = capsys.readouterr().out
    assert "STALE_CACHE" in out and "re-detect found nothing" in out
    assert "Playwright re-detect failed" in out      # the escalation ran and said why
    assert s.failures[-1]["type"] == "NO_PATTERN"
    assert s.failures[-1]["stale_cache"] == "div.old-card"
    # the dead entry is kept (nothing better was found) - and reported
    assert s.pattern_cache.get("example-broker.com")["container_selector"] == "div.old-card"


def test_working_cached_pattern_is_untouched(tmp_path, monkeypatch, capsys):
    s = _scraper(tmp_path, monkeypatch, _cards_html(4),
                 {"example-broker.com": {"container_selector": "div.biz-card", "score": 8}})
    assert len(_run(s)) == 4
    out = capsys.readouterr().out
    assert "STALE_CACHE" not in out and "Cached pattern: div.biz-card" in out
    assert s.stats["pattern_cache_events"] == []
    assert s.fetcher.pw_calls == 0


def test_twin_spelling_entry_is_last_resort_only(tmp_path, monkeypatch, capsys):
    # cells with no class: detection cannot see them; the www. entry can
    rows = "".join(f'<tr><td style="border:1px inset #CC0000">DRY CLEANER {i} NEW JERSEY '
                   f'Asking ${(i + 1) * 90},000 Cash flow $60,000</td></tr>' for i in range(4))
    html = f"<html><body><table>{rows}</table></body></html>"
    s = _scraper(tmp_path, monkeypatch, html,
                 {"www.example-broker.com": {"container_selector": 'td[style*="1px inset #CC0000"]'}})
    got = _run(s)
    out = capsys.readouterr().out
    assert len(got) == 4 and "www/bare twin" in out
    assert s.stats["pattern_cache_events"][0]["event"] == "twin_spelling"
    # ...and never pre-empts detection when detection works
    s2 = _scraper(tmp_path, monkeypatch, _cards_html(4),
                  {"www.example-broker.com": {"container_selector": "div.biz-card:first-child"}})
    assert len(_run(s2)) == 4
    assert "twin" not in capsys.readouterr().out


# ── detection fallbacks ──────────────────────────────────────────────────────

def _table(cls='class="csortable"', n=5, link=True):
    head = "<tr><th>Type</th><th>Title</th><th>Asking</th><th>Cash Flow</th></tr>"
    body = ""
    for i in range(n):
        t = (f'<a href="https://example-broker.com/listing-{i}/">Business Opportunity {i}</a>'
             if link else f"Business Opportunity {i} - Richmond")
        body += f"<tr><td>Services</td><td>{t}</td><td>$4{i}0.000</td><td>$1{i}0.000</td></tr>"
    return f"<html><body><div class='entry'><table {cls}>{head}{body}</table></div></body></html>"


def test_classless_table_rows_come_from_table_rows_strategy():
    # alpinebusinessbrokers / lightandraphael shape: j3-table_rows' Strategy 1c
    # finds them (this patch adds no table fallback of its own)
    p = PatternDetector.detect(_table(), BASE)
    assert "table.csortable" in p["container_selector"]
    assert pattern_card_count(_table(), BASE, p["container_selector"], "x") == 5
    p2 = PatternDetector.detect(_table('class="wp-table"', link=False), BASE)
    assert "table.wp-table" in p2["container_selector"]


def test_fallback_never_replaces_a_strategy_1_pattern():
    # a page strategy 1 already reads keeps its pattern even when per-card
    # token groups (the fallback's shape) are also present
    wix = "".join(f'<div class="_root_x container comp-q{i}z"><h3>Other Business {i} For Sale</h3>'
                  f'<p>Asking Price: $9{i}0,000</p><a href="/biz-{i}">View</a></div>'
                  for i in range(6))
    html = _cards_html(4).replace("</main>", wix + "</main>")
    p = PatternDetector.detect(html, BASE)
    assert p["container_selector"] == "div.biz-card"


def test_per_card_unique_class_token_grouped_on_shared_classes():
    # Wix Studio: every card carries its own comp-<id> beside shared classes;
    # rows of three cards also repeat - the card, not the row, must win.
    def card(i):
        return (f'<div class="_root_1w52m_1 container builder-root comp-x{i}q">'
                f'<h3>Liquor Store Number {i} for Sale</h3><p>Asking Price: $5{i}5,000</p>'
                f'<a href="https://example-broker.com/liquor-store-{i}">View Listing</a></div>')
    rows = "".join(f'<div class="row-wrap theme-vars comp-r{r}z">{card(3 * r)}{card(3 * r + 1)}'
                   f'{card(3 * r + 2)}</div>' for r in range(3))
    html = f"<html><body><main>{rows}</main></body></html>"
    p = PatternDetector.detect(html, BASE)
    assert p["container_selector"] == "div._root_1w52m_1.container.builder-root"
    assert pattern_card_count(html, BASE, p["container_selector"], "x") == 9


# ── ML prediction must out-extract ───────────────────────────────────────────

def test_ml_prediction_that_extracts_nothing_is_rejected(tmp_path, monkeypatch, capsys):
    s = _scraper(tmp_path, monkeypatch, _cards_html(4), {})
    # another broker's generic selector with a big score from ITS page
    s.pattern_cache.patterns = {"other.com": {"container_selector": "main", "score": 99,
                                              "count": 3},
                                "other2.com": {"container_selector": "div.biz-card h3",
                                               "score": 98, "count": 3}}
    monkeypatch.setattr(PatternCache, "predict",
                        lambda self, html, url: {"container_selector": "body main", "score": 99,
                                                 "count": 1})
    rows = _run(s)
    out = capsys.readouterr().out
    assert len(rows) == 4
    assert "ML prediction body main rejected" in out


def test_ml_prediction_that_extracts_more_still_wins(tmp_path, monkeypatch, capsys):
    html = _cards_html(4) + _cards_html(2, cls="biz-card extra").replace("Number", "Extra").replace("business-", "extra-")
    s = _scraper(tmp_path, monkeypatch, html, {})
    monkeypatch.setattr(PatternDetector, "detect",
                        classmethod(lambda cls, h, u: {"container_selector": "div.extra",
                                                       "count": 2, "score": 4}))
    monkeypatch.setattr(PatternCache, "predict",
                        lambda self, h, u: {"container_selector": "div.biz-card", "score": 9,
                                            "count": 6})
    assert len(_run(s)) == 6
    assert "ML predicted: div.biz-card" in capsys.readouterr().out


# ── CTA-only site link ───────────────────────────────────────────────────────

def _cta_card(title, href, btn, price=True, cls="routes-card"):
    p = "<p>Listing Price: $1,250,000</p>" if price else "<p>Call for details</p>"
    return (f'<div class="{cls}"><h3>{title}</h3><p>London, KY 10 Routes</p>{p}'
            f'<a href="{href}">{btn}</a></div>')


def _one(html, base="https://nextstep-capital.com"):
    from bs4 import BeautifulSoup
    el = BeautifulSoup(html, "html.parser").div
    return ListingExtractor.from_card(el, base, "Next Step")


def test_priced_card_with_contact_button_kept_as_no_link_card():
    c = _one(_cta_card("FedEx P/D Routes", "/contact", "Learn More"))
    assert c and c["url"] == "https://nextstep-capital.com" and not c["url_is_listing_specific"]
    c2 = _one(_cta_card("Family-Owned Printing Company", "https://www.jennessey.com",
                        "Contact Us for More Info"), base="https://jennessey.com/businesses-for-sale")
    assert c2 and c2["url"] == "https://jennessey.com/businesses-for-sale"


def test_site_page_drop_unchanged_where_it_must_hold():
    # no price -> still dropped
    assert _one(_cta_card("FedEx P/D Routes", "/contact", "Learn More", price=False)) is None
    # the link IS the card title (a site page linked by name) -> dropped
    assert _one(_cta_card("Sell Your Business", "/sell-your-business",
                          "Sell Your Business")) is None
    # site-page TITLE -> dropped regardless of the link
    assert _one(_cta_card("Contact us today", "/contact", "Learn More")) is None
    # off-site link (social profile) -> dropped
    assert _one(_cta_card("FedEx P/D Routes", "https://www.linkedin.com/company/x",
                          "Learn More")) is None
    # a real detail link is untouched
    c = _one(_cta_card("FedEx P/D Routes", "/listing/fedex-london-ky", "Learn More"))
    assert c["url"] == "https://nextstep-capital.com/listing/fedex-london-ky"


def test_ml_prediction_tie_keeps_todays_choice(tmp_path, monkeypatch, capsys):
    # equal yield: unchanged behaviour (the prediction wins), so brokers that
    # produce today through a prediction keep their selector
    s = _scraper(tmp_path, monkeypatch, _cards_html(4), {})
    monkeypatch.setattr(PatternCache, "predict",
                        lambda self, h, u: {"container_selector": "main div.biz-card",
                                            "score": 99, "count": 4})
    assert len(_run(s)) == 4
    out = capsys.readouterr().out
    assert "ML predicted: main div.biz-card" in out and "rejected" not in out


def test_jennessey_reads_the_labelled_asking_price():
    html = ('<div class="_FiCX"><h2>Family-Owned Printing Company</h2><p>Asking Price: $700,000</p>'
            '<p>Sale Pending</p><p>Location: Linn County, Iowa</p>'
            '<a href="https://www.jennessey.com">Contact Us for More Info</a>'
            '<p>Gross sales of $1,300,000 last year.</p></div>')
    c = _one(html, base="https://jennessey.com/businesses-for-sale")
    assert c["asking_price"] == 700000


def test_redetected_pattern_after_dead_cache_must_look_like_listings(tmp_path, monkeypatch, capsys):
    # every card shares one NDA link (ccbusinessbrokers.com shape): 1 distinct listing
    cards = "".join(f'<div class="nda-card"><h3>Antique Store Number {i} Central Coast</h3>'
                    f'<p>Asking Price: $18{i},000</p><a href="/non-disclosure-nda">Sign NDA</a></div>'
                    for i in range(5))
    html = f"<html><body><main>{cards}</main></body></html>"
    s = _scraper(tmp_path, monkeypatch, html,
                 {"example-broker.com": {"container_selector": "div.old-card"}})
    monkeypatch.setattr(PatternDetector, "detect",
                        classmethod(lambda cls, h, u: {"container_selector": "div.nda-card",
                                                       "count": 5, "score": 10}))
    assert _run(s) == []
    out = capsys.readouterr().out
    assert "re-detected div.nda-card rejected" in out
    assert s.failures[-1]["stale_cache"] == "div.old-card"
    assert "rejected" in s.stats["pattern_cache_events"][-1]
    assert s.pattern_cache.get("example-broker.com")["container_selector"] == "div.old-card"


def test_no_ml_prediction_while_healing_a_dead_cache(tmp_path, monkeypatch, capsys):
    s = _scraper(tmp_path, monkeypatch, _cards_html(4),
                 {"example-broker.com": {"container_selector": "div.old-card"}})
    called = []
    monkeypatch.setattr(PatternCache, "predict",
                        lambda self, h, u: called.append(1) or {"container_selector": "main div",
                                                                "score": 99, "count": 9})
    assert len(_run(s)) == 4
    assert called == [] and "healed" in capsys.readouterr().out


def test_pattern_exposed_by_yield_guard_is_gated(tmp_path, monkeypatch, capsys):
    # the rejected prediction hid a weak detection (2 of 3 cards are nav junk)
    html = ('<html><body><main>'
            '<div class="d-flex"><h3>Listings menu entry</h3><a href="/listings">Listings page</a></div>'
            '<div class="d-flex"><h3>Listings menu again</h3><a href="/listings/">Listings page</a></div>'
            '<div class="d-flex"><h3>Baltimore Laundromat For Sale</h3><p>Price: $600,000</p>'
            '<a href="/property/baltimore-laundromat-for-sale/">Baltimore</a></div>'
            '</main></body></html>')
    s = _scraper(tmp_path, monkeypatch, html, {})
    monkeypatch.setattr(PatternDetector, "detect",
                        classmethod(lambda cls, h, u: {"container_selector": "div.d-flex",
                                                       "count": 3, "score": 4}))
    monkeypatch.setattr(PatternCache, "predict",
                        lambda self, h, u: {"container_selector": "div.row", "score": 30,
                                            "count": 9})
    assert _run(s) == []
    out = capsys.readouterr().out
    assert "ML prediction div.row rejected" in out
    assert "ML prediction rejected; re-detected div.d-flex rejected" in out
    assert "_exposed_by_yield_guard" not in json.dumps(s.pattern_cache.patterns)


def test_pattern_exposed_by_yield_guard_that_looks_real_is_used(tmp_path, monkeypatch, capsys):
    s = _scraper(tmp_path, monkeypatch, _cards_html(4), {})
    monkeypatch.setattr(PatternCache, "predict",
                        lambda self, h, u: {"container_selector": "div.row", "score": 99,
                                            "count": 9})
    assert len(_run(s)) == 4
    assert "_exposed_by_yield_guard" not in json.dumps(s.pattern_cache.patterns)
