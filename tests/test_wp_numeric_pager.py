import os, sys
sys.path.insert(0, os.environ.get("SCRAPERS") or os.path.join(os.path.dirname(__file__), "..", "scrapers"))
from specialized_scrapers import WPRestScraper as W

BASE = "https://dellingco.com/businesses-for-sale"
PAGER = '''<nav><a class="page-numbers" href="https://dellingco.com/businesses-for-sale/">1</a>
<a class="page-numbers" href="https://dellingco.com/businesses-for-sale/2/">2</a>
<a href="https://dellingco.com/businesses-for-sale/3/" class="next page-numbers">3</a>
<a class="page-numbers" href="/businesses-for-sale/10/">10</a></nav>'''


def test_follows_numeric_pager_links_skips_page_one():
    got = W._numeric_pager_links(PAGER, BASE + "/", BASE)
    assert got == [BASE + "/page/2/", BASE + "/page/3/", BASE + "/page/10/"]


def test_ignores_numeric_slugs_and_unclassed_links():
    body = '''<a class="card" href="https://dellingco.com/businesses-for-sale/694/">x</a>
<a href="https://dellingco.com/businesses-for-sale/5/">5</a>
<a class="page-numbers" href="https://other.com/businesses-for-sale/2/">2</a>
<a class="page-numbers" href="https://dellingco.com/businesses-for-sale/foo/2/">2</a>
<a class="page-numbers" href="https://dellingco.com/businesses-for-sale/1/">1</a>'''
    assert W._numeric_pager_links(body, BASE + "/", BASE) == []
