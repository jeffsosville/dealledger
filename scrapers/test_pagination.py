"""Offline tests for PaginationHandler and the per-broker pagination override.

    python3 scrapers/test_pagination.py
"""
import os
import sys
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(__file__))
from bs4 import BeautifulSoup  # noqa: E402
import dealledger_scraper_v6 as v6  # noqa: E402

PH = v6.PaginationHandler


def _soup(html):
    return BeautifulSoup(html, "html.parser")


def test_next_link_variants():
    base = "https://ex.com/find-a-business"
    cases = {
        # Drupal pager: visually-hidden text + ›› glyph, rel=next
        '<li class="pager__item--next"><a href="?page=1" title="Go to next page">'
        '<span class="visually-hidden">Next page</span><span>››</span></a></li>':
            "https://ex.com/find-a-business?page=1",
        # WordPress with &nbsp; between Next and »
        '<a class="page-numbers" href="/x/page/2/">Next\xa0»</a>': "https://ex.com/x/page/2/",
        # aria-label only
        '<a aria-label="Next page" href="/p/2">›</a>': "https://ex.com/p/2",
        # bare glyph
        '<a href="?pg=2">»</a>': "https://ex.com/find-a-business?pg=2",
    }
    for html, want in cases.items():
        got = PH.find_next_page(_soup(html), base)
        assert got == want, (html, got, want)
    # javascript / anchor links are not next pages
    assert PH.find_next_page(_soup('<a class="next" href="#">Next</a>'), base) is None
    assert PH.find_next_page(_soup('<a href="javascript:void(0)">Next</a>'), base) is None


def test_override_parse_and_build():
    assert PH.parse_override(None) is None
    assert PH.parse_override("") is None
    assert PH.parse_override("nan") is None
    assert PH.parse_override("none") == "none"
    assert PH.parse_override("param0") == ("param0", "page")
    assert PH.parse_override("param", "paged") == ("param", "paged")
    assert PH.parse_override("path") == ("path", None)
    assert PH.parse_override("bogus") is None
    u = "https://www.calhouncompanies.com/find-a-business"
    assert PH.build_from_template(u, ("param0", "page"), 2, 10).endswith("?page=1")
    assert PH.build_from_template(u, ("param0", "page"), 12, 10).endswith("?page=11")
    p = "https://poegroupadvisors.com/buying/practice-search"
    assert PH.build_from_template(p, ("path", None), 3, 14) == p + "/page/3/"


def test_candidates_try_zero_based_first():
    c = PH.candidate_page_urls("https://ex.com/l", 2, 10)
    assert c[0] == ("https://ex.com/l?page=1", ("param0", "page"))
    assert ("https://ex.com/l?page=2", ("param", "page")) in c


# ── end-to-end: the page loop against a fake 0-based (Drupal) site ──────────
def _drupal_page(n, per=10, last=3, sidebar=True):
    cards = "".join(
        f'<div class="listing-card"><h3><a href="/business/{n}-{i}">Business {n}-{i} '
        f'for sale</a></h3><p>Asking Price: ${(n*per+i+1)*10000:,}</p>'
        f'<p>Cash Flow: $50,000</p></div>' for i in range(per))
    pager = ("" if n >= last else
             f'<li class="pager__item--next"><a href="?page={n+1}" title="Go to next page">'
             f'<span>Next page</span><span>››</span></a></li>')
    return f"<html><body><main>{cards}</main><ul class='pager'>{pager}</ul></body></html>"


class _FakeFetcher:
    def __init__(self, pages):
        self.pages, self.calls = pages, []
        self._proxy_domains_runtime = set()

    def fetch(self, url, use_proxy=False):
        self.calls.append(url)
        q = parse_qs(urlparse(url).query)
        n = int(q.get("page", ["0"])[0])
        return self.pages.get(n, self.pages[0]), "requests"


def _run(broker, pages):
    s = v6.DealLedgerScraper.__new__(v6.DealLedgerScraper)
    # Minimal state the inner method touches
    s.stats = {"brokers_attempted": 0, "brokers_success": 0, "brokers_failed": 0,
               "patterns_learned": 0, "detail_pages_fetched": 0,
               "failure_types": __import__("collections").Counter()}
    s.failures, s.embed_brokers = [], []
    s.fetcher = _FakeFetcher(pages)

    class _Cache:
        def get(self, d): return {"container_selector": "div.listing-card", "score": 1}
        def predict(self, h, u): return None
        def store(self, d, p): pass
    s.pattern_cache = _Cache()
    s._stored_listings = lambda ids: {}
    v6.extract_via_api = lambda *a, **k: None
    cards = []
    orig = v6.ListingExtractor.from_card

    # Capture card count from phase 1 by stopping before detail fetches.
    def stop_after_cards(self_, ids):
        raise _Stop(len(ids))
    s._stored_listings = lambda ids: (_ for _ in ()).throw(_Stop(len(ids)))
    try:
        s._scrape_broker_inner(broker)
    except _Stop as e:
        return e.n, s.fetcher.calls
    return None, s.fetcher.calls


class _Stop(BaseException):
    def __init__(self, n):
        self.n = n


def test_loop_follows_drupal_pager_and_override():
    pages = {n: _drupal_page(n) for n in range(4)}
    b = {"name": "t", "url": "https://www.calhouncompanies.com/find-a-business",
         "domain": "www.calhouncompanies.com"}
    n, calls = _run(dict(b), pages)
    assert n == 40, (n, calls)          # next-link path reads all 4 pages
    n, calls = _run(dict(b, pagination_mode="param0", pagination_param="page"), pages)
    assert n == 40, (n, calls)          # forced template reads all 4 pages too
    n, calls = _run(dict(b, pagination_mode="none"), pages)
    assert n == 10 and len(calls) == 1, (n, calls)


if __name__ == "__main__":
    v6.time.sleep = lambda s: None
    v6.random.uniform = lambda a, b: 0
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok ", name)
