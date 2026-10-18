"""Cards whose detail link sits outside the card (restaurantrealty.com)."""
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
from dealledger_scraper_v6 import _wrapper_detail_link  # noqa: E402

BASE = "https://restaurantrealty.com/search-listing-by-list/"
CARD = ('<div class="mob">Active Los Angeles County Playa Vista Restaurant &amp; Bar, '
        'Asset Sale. Los Angeles County, CA $175,000</div>')


def _card(html):
    return BeautifulSoup(html, "html.parser").select_one("div.mob")


def test_sibling_link():
    html = (f'<div class="item"><div class="desk"><a href="/listing/?listing=5255-Playa">x</a></div>'
            f'{CARD}</div>')
    assert _wrapper_detail_link(_card(html), BASE) == "/listing/?listing=5255-Playa"


def test_wrapper_link():
    html = f'<a class="wrap" href="/listing/?listing=5187-Lodi"><span>{CARD}</span></a>'
    assert _wrapper_detail_link(_card(html), BASE) == "/listing/?listing=5187-Lodi"


def test_attribute_link():
    html = CARD.replace('class="mob"', 'class="mob" data-href="/listing/?listing=5314-BBQ"')
    assert _wrapper_detail_link(_card(html), BASE) == "/listing/?listing=5314-BBQ"


def test_wrapper_with_many_listings_is_refused():
    html = ('<section><a href="/listing/?listing=1-A">a</a><a href="/listing/?listing=2-B">b</a>'
            f'<div>{CARD}</div></section>')
    assert _wrapper_detail_link(_card(html), BASE) is None


def test_other_domains_unchanged():
    html = f'<a href="/listing/?listing=9-X"><span>{CARD}</span></a>'
    assert _wrapper_detail_link(_card(html), "https://example-broker.com/listings/") is None
