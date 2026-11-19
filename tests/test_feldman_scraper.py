import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

from feldman_scraper import parse_lines, to_rows, parse_page  # noqa: E402

FIX = os.path.join(os.path.dirname(__file__), "fixtures", "feldman_page_text.txt")


def _rows():
    with open(FIX, encoding="utf-8") as f:
        return to_rows(parse_lines(f.read().split("\n")))


def by_title(rows, prefix):
    return next(r for r in rows if r["title"].lower().startswith(prefix.lower()))


def test_count_and_keys_unique():
    rows = _rows()
    assert len(rows) == 64
    urls = [r["listing_url"] for r in rows]
    assert len(set(urls)) == len(urls)
    assert all("#fb-" in u for u in urls)


def test_first_block_skips_nav():
    r = _rows()[0]
    assert r["title"] == "Hi producing coin laundromat (NY)"
    assert r["price"] == 1_039_000
    assert r["revenue"] == 610_600
    assert r["cash_flow"] == 231_048
    assert r["city"] == "Brooklyn" and r["state"] == "NY"


def test_every_row_has_price_and_state():
    rows = _rows()
    assert all(r["price"] for r in rows)
    assert all(r["state"] for r in rows)


def test_price_without_dollar_and_blank_net():
    rows = _rows()
    va = by_title(rows, "COIN LAUNDROMAT (VA)")
    assert va["price"] == 425_000 and va["location"] == "Fairfax County, VA"
    bk = by_title(rows, "BANKRUPTCY OPPORTUNITY")
    assert bk["price"] == 358_000 and bk["cash_flow"] is None and bk["revenue"] is None


def test_dc_from_title():
    dc = by_title(_rows(), "capital city")
    assert dc["state"] == "DC" and dc["city"] == "Washington"


def test_same_title_different_states_distinct():
    rows = _rows()
    titles = [r["title"].lower() for r in rows if r["title"].lower().startswith("coin laundromat (")]
    assert len(titles) >= 3


def test_html_wrapper():
    html = ("<div id='wsite-content'><div class='paragraph'>Test card laundromat (NJ)<br>"
            "Revenue. 100,000<br>Net 50,000<br>Town. EDISON<br>County. MIDDLESEX<br>"
            "State. NJ<br>Selling price. $199,000</div></div>")
    rows = parse_page(html)
    assert len(rows) == 1
    assert rows[0]["city"] == "Edison" and rows[0]["price"] == 199_000
