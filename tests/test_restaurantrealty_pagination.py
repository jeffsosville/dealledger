"""restaurantrealty.com: infinite-scroll truncation and title-in-URL identity.

1. The listing index is a BBCRM widget that appends 12 cards per XHR as the
   page is scrolled. fetch_playwright scrolled a fixed four times, so the
   crawl kept 55-114 of 124 listings with nothing in the log. It now scrolls
   until the page height stops changing (never fewer than four rounds) and
   says so when the cap is reached while the page is still growing.
2. Detail URLs are /listing/?listing=<number>-<title incl. price>; a price
   edit changed the URL and the id. The id now keys on the number only.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import (PageFetcher, listing_id_key,  # noqa: E402
                                   stable_listing_id)


class _Mouse:
    def __init__(self, page):
        self.page = page

    def wheel(self, dx, dy):
        self.page.scrolls += 1


class _InfinitePage:
    """Grows by one 'batch' per scroll until `batches` are exhausted."""

    def __init__(self, batches, per_batch=12):
        self.batches, self.per_batch = batches, per_batch
        self.loaded, self.scrolls, self.closed = 1, 0, False
        self.mouse = _Mouse(self)

    def goto(self, *a, **k):
        pass

    def wait_for_load_state(self, *a, **k):
        pass

    def wait_for_timeout(self, ms):
        pass

    def evaluate(self, js):
        if "scrollTo" in js:
            if self.loaded < self.batches:
                self.loaded += 1
            return None
        return 1000 * self.loaded           # document.body.scrollHeight

    def content(self):
        return "<div class='mob'>x</div>" * min(self.loaded * self.per_batch, 124)

    def close(self):
        self.closed = True


class _Browser:
    def __init__(self, page):
        self.page = page

    def new_page(self, **k):
        return self.page


def _fetcher(page, monkeypatch):
    monkeypatch.setattr(v6, "HAS_PLAYWRIGHT", True)
    f = PageFetcher.__new__(PageFetcher)
    f._get_browser = lambda use_proxy=False: _Browser(page)
    return f


def test_scrolls_until_all_cards_loaded(monkeypatch):
    page = _InfinitePage(batches=11)        # 124 listings, 12 per XHR
    f = _fetcher(page, monkeypatch)
    html = f.fetch_playwright("https://restaurantrealty.com/x/")
    assert html.count("class='mob'") == 124
    assert page.scrolls > 4                 # the old fixed loop stopped at 4
    # grew past the old four scrolls: _scrape_broker_inner then skips
    # guessing ?page= URLs (each would be rendered and scrolled again)
    assert f.last_scroll_growth > 4


def test_static_page_keeps_the_old_four_rounds(monkeypatch):
    page = _InfinitePage(batches=1)
    f = _fetcher(page, monkeypatch)
    f.fetch_playwright("https://example.com/")
    assert page.scrolls == 4
    assert f.last_scroll_growth == 0        # ?page= discovery still runs


def test_short_growth_keeps_page_discovery(monkeypatch):
    # a page that finished growing within the old four scrolls (lazy images,
    # one late XHR) keeps the old ?page= candidate discovery
    page = _InfinitePage(batches=4)
    f = _fetcher(page, monkeypatch)
    f.fetch_playwright("https://example.com/")
    assert f.last_scroll_growth <= 4


def test_cap_reached_while_growing_is_logged(monkeypatch, capsys):
    monkeypatch.setattr(v6, "SCROLL_MAX_ROUNDS", 6)
    page = _InfinitePage(batches=50)
    _fetcher(page, monkeypatch).fetch_playwright("https://example.com/")
    assert page.scrolls == 6
    assert "still growing" in capsys.readouterr().out


BASE = "https://restaurantrealty.com/search-listing-by-list/"
A = "https://restaurantrealty.com/listing/?listing=5357-Established Breakfast/Lunch Restaurant with $182,000 Profit, SBA Pre-Qualified for 90% Loan"
B = "https://restaurantrealty.com/listing/?listing=5357-Established Breakfast/Lunch Restaurant with $98,784 Profit"


def test_title_edit_keeps_the_id():
    assert stable_listing_id("a", A, BASE, "restaurantrealty.com") == \
        stable_listing_id("b", B, BASE, "restaurantrealty.com")


def test_different_listings_differ():
    other = "https://restaurantrealty.com/listing/?listing=5356-Established Breakfast"
    assert stable_listing_id("a", A, BASE, "restaurantrealty.com") != \
        stable_listing_id("a", other, BASE, "restaurantrealty.com")


def test_key_form_and_www():
    want = "https://restaurantrealty.com/listing/?listing=5357"
    assert listing_id_key(A) == want
    assert listing_id_key(A.replace("://", "://www.")) == want


def test_other_domains_unchanged():
    for u in ("https://oregonrestaurantrealty.com/listing/?listing=5357-Foo",
              "https://www.therestaurantrealty.com/listing/?listing=12-x",
              "https://example.com/biz/123-cafe"):
        assert listing_id_key(u) == u


def test_url_without_number_unchanged():
    u = "https://restaurantrealty.com/listing/"
    assert listing_id_key(u) == u


# ── 3. cards with no parseable price ─────────────────────────────────────
# Mirrors the BBCRM listviewnew.html card: the link wraps the whole card, the
# outer div.mob holds title + price + agent, and nested div.mob blocks repeat
# the title alone and the agent's email. Before 2026-11-11 a card whose price
# was "Make Offer" / "79,000" / "$120,00O" had neither a price nor a link of
# its own and was dropped: 8 of 124 listings on every crawl.
from bs4 import BeautifulSoup  # noqa: E402

from dealledger_scraper_v6 import ListingExtractor  # noqa: E402


def _bbcrm_card(num, slug, title, price):
    return (
        f"<a href='/listing/?listing={num}-{slug}'><div class='hb'>"
        "<div class='mob h'><div>Active</div></div>"
        "<div class='mob'>"
        f"<div class='mob'>{title}</div>"
        f"<div class='mob'>{price}</div>"
        "<div>Los Angeles County</div>"
        f"<div>SQ Ft: 2,000 RRC # {num}</div>"
        "<div class='mob'>Agent Todd Pliss</div>"
        "<div class='mob'>Phone : 310-667-6058 Email : todd@restaurantrealty.com</div>"
        "</div></div></a>")


def _extract(html, base=BASE):
    soup = BeautifulSoup(f"<html><body>{html}</body></html>", "html.parser")
    cards = [ListingExtractor.from_card(el, base, "restaurantrealty.com")
             for el in soup.select("div.mob")]
    return [c for c in cards if c]


PRICELESS = [
    ("5246", "MAKE AN OFFER - License", "MAKE AN OFFER - License &amp; Assets for Sale in a Stunning Restaurant", "Make Offer"),
    ("5337", "Downtown LA Turnkey Tapas Lounge with Full Liquor License",
     "Downtown LA Turnkey Tapas Lounge with Full Liquor License", "79,000"),
    ("5421", "Phil", "Phil's Fish Market &amp; Eatery – Iconic Seafood Restaurant", "Make Offer"),
]


def test_priceless_cards_are_kept_once_each():
    html = "".join(_bbcrm_card(*p) for p in PRICELESS)
    cards = _extract(html)
    ids = {c["id"] for c in cards}
    assert len(ids) == 3
    titles = {c["_detail_url"]: {x["title"] for x in cards if x["_detail_url"] == c["_detail_url"]}
              for c in cards}
    assert all(len(t) == 1 for t in titles.values())      # no email/agent titles
    assert not any("Email" in c["title"] or "Agent" in c["title"] for c in cards)


def test_priced_card_unchanged():
    html = _bbcrm_card("5410", "Taco Franchise Busy Location",
                       "Taco Franchise Busy Location", "$199,990")
    cards = _extract(html)
    assert {c["id"] for c in cards} and len({c["id"] for c in cards}) == 1
    assert len({c["title"] for c in cards}) == 1


def test_other_domains_still_need_price_or_own_link():
    html = _bbcrm_card("5246", "MAKE AN OFFER - License",
                       "MAKE AN OFFER - License &amp; Assets for Sale in a Stunning Restaurant",
                       "Make Offer")
    assert _extract(html, "https://example-broker.com/listings/") == []


def test_title_must_match_the_link():
    assert v6._title_matches_link("Phil's Fish Market", "/listing/?listing=5421-Phil")
    assert v6._title_matches_link("Restaurant & Cocktail Bar in SF",
                                  "/listing/?listing=5420-Restaurant & Cocktail Bar in SF")
    assert not v6._title_matches_link("Email : todd@restaurantrealty.com",
                                      "/listing/?listing=5410-Taco Franchise Busy Location")
    assert not v6._title_matches_link("Agent Todd Pliss", "/listing/?listing=5410-Taco")
