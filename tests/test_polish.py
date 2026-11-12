"""j3-polish (2026-11-12) -> tests/test_polish.py.

Applies after the full apply_job3.sh stack + j3-polish.patch. Four fixes:
  1. listing_status: "in contract" (wesellrestaurants feed) is under contract;
     "withdraw"/"withdrawn" (lbaweb card value) are inactive.
  2. displayed_count: the count-sweep harness's guards ported into the parser —
     phone-number tail, year, track record ("sold 500 businesses"), implausible
     weak counts (>10x every card/link/seen signal), and the forms
     "Found N ...", "N Business Listings", "N Businesses", "N opportunities",
     "N Listings Selected".
  3. junk_filter.is_listing_junk: a title opening with "$X Revenue" passes when
     business words follow (lbaweb "$1M+ Revenue – $347K Net Income -9+ Years
     Established"); bare figures and metric fragments still fail.
  4. V6: the '🧺 wrapper skipped' line prints once per broker run.
"""
import inspect
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))

import displayed_count as dc  # noqa: E402
import junk_filter as jf  # noqa: E402
import listing_status as ls  # noqa: E402
from displayed_count import DisplayedCountTracker, parse_stated_count  # noqa: E402


def page(body):
    return f"<html><head><title>x</title></head><body>{body}</body></html>"


def cards(n, path="/listing/biz-"):
    """n listing cards: a price label each and a distinct detail link."""
    return "".join(f"<div class='card'><a href='{path}{i}/'><h3>Biz {i}</h3></a>"
                   f"<p>Asking Price: $1{i}0,000</p></div>" for i in range(n))


# ── 1. listing_status vocabulary ────────────────────────────────────────────
@pytest.mark.parametrize("text,expected", [
    ("In Contract", ("active", "In Contract")),
    ("IN CONTRACT", ("active", "IN CONTRACT")),
    ("in-contract", ("active", "in-contract")),
    ("Withdraw", ("inactive", "Withdraw")),
    ("Withdrawn", ("inactive", "Withdrawn")),
    ("WITHDRAWN!", ("inactive", "WITHDRAWN!")),
])
def test_new_phrases(text, expected):
    assert ls.lookup_status(text) == expected


def test_new_phrases_are_in_their_families():
    assert "in contract" in ls.UNDER_CONTRACT_PHRASES
    assert {"withdraw", "withdrawn"} <= ls.INACTIVE_PHRASES
    # each phrase in exactly one family
    fams = [ls.SOLD_PHRASES, ls.UNDER_CONTRACT_PHRASES, ls.COMING_SOON_PHRASES,
            ls.OUT_OF_SCOPE_PHRASES, ls.INACTIVE_PHRASES, ls.ACTIVE_PHRASES]
    for p in ("in contract", "withdraw", "withdrawn"):
        assert sum(p in f for f in fams) == 1


def test_free_text_still_not_a_status():
    # exact phrase only: a description mentioning a contract is not a status
    assert ls.lookup_status("Owner will sign a non-compete in contract") is None
    assert ls.lookup_status("Withdraw cash from the ATM route daily") is None


def test_wsr_alias_now_redundant_but_harmless():
    ss = pytest.importorskip("specialized_scrapers")
    # the shared module now answers "In Contract" itself; the local alias
    # (WeSellRestaurantsScraper.STATUS_ALIASES) is never consulted
    assert ss.lookup_status_text("In Contract", "wesellrestaurants.com") == ("active", "In Contract")
    rec = {"id": 1, "bstatuslist": "In Contract", "bname": "Taqueria", "bheadlinead": "Taqueria"}
    assert ss.WeSellRestaurantsScraper.record_status(rec) == ("active", "In Contract")


def test_displayed_count_uc_section_picks_up_in_contract():
    # the UC section regex is built from UNDER_CONTRACT_PHRASES
    body = "<ul><li>For Sale (40)</li><li>In Contract (6)</li></ul>"
    assert parse_stated_count(page(body))["count"] == 46


# ── 2. displayed_count guards ───────────────────────────────────────────────
LBAWEB_HEADER = ("<header><div class='top'>Liberty Business Advisors "
                 "<a href='tel:9259322450'>(925) 932-2450</a></div>"
                 "<nav><a href='/listings'>Listings</a><a href='/sellers'>Sellers</a></nav></header>")


def test_lbaweb_phone_tail_is_not_a_count():
    # live lbaweb.com, 12 Nov: W6 read '2450 Listings' and logged INCOMPLETE_LOW
    assert parse_stated_count(page(LBAWEB_HEADER + cards(20))) is None


@pytest.mark.parametrize("phone", ["(925) 932-2450", "(925)932-2450", "925-932-2450",
                                   "925.932.2450", "925 932 2450"])
def test_phone_shapes(phone):
    assert parse_stated_count(page(f"<p>Call {phone}</p><p>Listings</p>")) is None


def test_phone_guard_keeps_a_real_count_on_the_same_page():
    r = parse_stated_count(page(LBAWEB_HEADER + "<p>24 listings</p>" + cards(20)))
    assert r["count"] == 24


@pytest.mark.parametrize("body", [
    "<footer>© 2026 Routes For Sale. All rights reserved.</footer>",   # routesforsale.net
    "<p>Since 1985 Listings</p>",
    "<p>2025 Listing price: $550,000</p>",
])
def test_year_is_not_a_count(body):
    assert parse_stated_count(page(body)) is None


@pytest.mark.parametrize("body", [
    "<p>We have sold 500 businesses</p>",
    "<p>Our brokers helped over 300 businesses</p>",
    "<p>We sell 45 businesses</p>",
    "<p>Brokered more than 1,200 businesses</p>",
    "<p>Represented 85 businesses</p>",
])
def test_track_record_is_not_a_count(body):
    assert parse_stated_count(page(body)) is None


def test_status_filter_nav_is_not_track_record():
    # unbroker.com: the status filter sits right before the real count
    body = "<p>All Statuses Active Sold Under Contract Wanted 108 listings Sort: Newest</p>"
    assert parse_stated_count(page(body))["count"] == 108


@pytest.mark.parametrize("body,expected,level", [
    ("<p>Found 1085 Business Listings</p>", 1085, "found"),           # execbb.com
    ("<p>Found 12 results</p>", 12, "found"),
    ("<p>Found 3 Businesses</p>", 3, "found"),
    ("<p>45 Business Listings</p>", 45, "noun"),
    ("<p>18 Businesses</p>", 18, "noun"),
    ("<p>30 opportunities</p>", 30, "noun"),
    ("<p>12 Listings Selected</p>", 12, "noun"),
])
def test_stated_forms(body, expected, level):
    r = parse_stated_count(page(body))
    assert r["count"] == expected and f"level={level}" in r["note"]


def test_execbb_found_with_its_links():
    links = "".join(f"<a href='/Buyer/sub/listingdetail.asp?listingid={i}'>L{i}</a>"
                    for i in range(1085))
    r = parse_stated_count(page("<h2>Found 1085 Business Listings</h2>" + links))
    assert r["count"] == 1085


def test_found_ranks_above_bare_noun():
    r = parse_stated_count(page("<p>Found 40 listings</p><p>250 businesses for sale nationwide</p>"))
    assert r["count"] == 40


def test_pagination_forms_still_win():
    assert parse_stated_count(page("<p>Found 9 results</p><p>Showing 1-10 of 48</p>"))["count"] == 48


# implausible weak counts
def test_zip_code_label_dropped_against_cards():
    # lmmre.com: '06525 Listing' beside 6 cards
    r = parse_stated_count(page("<p>Woodbridge, CT 06525 Listing</p>" + cards(6)))
    assert r is None


def test_implausible_drop_does_not_promote_a_lower_level():
    # reputedbrokerage page 1: 'For Sale (379)' label vs 16 cards (paginated) ->
    # page-only check drops it; the weaker '103 properties' must not replace it
    body = "<li>For Sale (379)</li><p>103 properties</p>" + cards(16)
    assert parse_stated_count(page(body)) is None


def test_seen_rescues_a_paginated_label():
    body = "<li>For Sale (379)</li>" + cards(16)
    assert parse_stated_count(page(body), seen=375)["count"] == 379


def test_no_signal_keeps_the_count():
    # JS-rendered page, nothing seen: no evidence either way -> keep
    assert parse_stated_count(page("<p>245 listings</p>"))["count"] == 245
    assert parse_stated_count(page("<p>245 listings</p>"), seen=0)["count"] == 245


def test_ratio_boundary():
    body = "<p>120 listings</p>" + cards(12)
    assert parse_stated_count(page(body))["count"] == 120           # 10x: kept
    body = "<p>121 listings</p>" + cards(12)
    assert parse_stated_count(page(body)) is None                   # >10x: dropped


def test_strong_levels_not_plausibility_checked():
    # 'Showing 1-12 of 850' is a pagination statement: never dropped
    assert parse_stated_count(page("<p>Showing 1-12 of 850</p>" + cards(12)))["count"] == 850
    assert parse_stated_count(page("<p>Showing 900 items</p>" + cards(12)))["count"] == 900


def test_link_signal():
    assert dc.link_signal(page(cards(7))) == 7
    assert dc.link_signal(page("<a href='/a'>a</a><a href='/b'>b</a><a href='/c'>c</a>")) == 0  # under '/'
    assert dc.link_signal(page("<a href='#x'>x</a><a href='mailto:a@b'>m</a>")) == 0
    q = "".join(f"<a href='detail.asp?listingid={i}&s=1'>x</a>" for i in range(5))
    assert dc.link_signal(page(q)) == 5


# tracker: plausibility judged with every page and the seen count
def test_tracker_paginated_label_survives_the_walk():
    t = DisplayedCountTracker()
    for p in range(20):
        t.add_page(page((f"<li>Listings (245)</li>" if p == 0 else "") + cards(12, f"/listing/p{p}-")))
    assert t.result(seen=240)["count"] == 245


def test_tracker_crawl_stopped_on_page_one_drops_weak_count():
    # the known cost of the 10x rule: a crawl that stopped at 12 of 245 loses
    # the weak stated count (strong 'x of N' forms are unaffected)
    t = DisplayedCountTracker()
    t.add_page(page("<p>245 listings</p>" + cards(12)))
    assert t.result(seen=12)["source"] == "counted"
    t = DisplayedCountTracker()
    t.add_page(page("<p>Showing 1-12 of 245</p>" + cards(12)))
    assert t.result(seen=12)["count"] == 245


def test_tracker_zip_dropped_falls_back_to_counted():
    t = DisplayedCountTracker()
    t.add_page(page("<p>Woodbridge, CT 06525 Listing</p>" + cards(6)))
    r = t.result(seen=6)
    assert r["count"] == 6 and r["source"] == "counted"
    assert r["note"].startswith("stated '06525 Listing' (noun) dropped: >10x signal 6")
    assert r["note"].endswith("price_label over 1 page(s)")


def test_largest_implausible_hit_does_not_promote_a_smaller_one():
    # matthews.com: '2,467 Results' (dropped page-only) must not become '5 Properties'
    body = "<p>2,467 Results</p><p>5 Properties</p>" + cards(21)
    assert parse_stated_count(page(body)) is None
    assert parse_stated_count(page(body), seen=2400)["count"] == 2467


def test_tracker_result_without_seen_still_works():
    t = DisplayedCountTracker()
    t.add_page(page("<p>Showing 62 items</p>"))
    assert t.result()["count"] == 62


def test_tracker_lbaweb_phone():
    t = DisplayedCountTracker()
    t.add_page(page(LBAWEB_HEADER + cards(30)))
    assert t.result(seen=590)["source"] == "counted"


# ── 3. title gate: dollar-led titles ────────────────────────────────────────
LBA_TITLE = "$1M+ Revenue – $347K Net Income -9+ Years Established"


@pytest.mark.parametrize("title", [
    LBA_TITLE,
    "$310,000 Revenue - Marin County Tax and Accounting Practice",      # acctsales.com
    "$500K (REVENUE) CENTRAL WISCONSIN – CPA PRACTICE FOR SALE",
    "$2M+ Revenue Dallas, Texas Restaurant and Bar for sale",           # eatsbroker.com
    "$650K Revenue | $120K Net | 50% Custom Cake Business | Semi-Absentee | Est. 1993",
    "$3.1M Revenue High-Volume Pizzeria – Long Island, NY",
])
def test_dollar_led_titles_with_business_words_pass(title):
    assert jf.is_listing_junk(title) is False


@pytest.mark.parametrize("title", [
    "$450,000",
    "$1,200,000.00",
    "Price: $450,000",
    "Asking Price: $1,250,000",
    "$450,000 Gross Sales",
    "$1,250,000.00 Gross Revenue",                                      # greenlifebusiness.com
    "$1,200,000 Revenue",
    "$1M+ Revenue – $347K Net Income",
    "$350,000 Gross Revenue, $90K SDE",
    "$500K for sale",
    "$925,000For Sale5 Island Ave #8HMiami Beach, FL 331391,302sqft2 beds2 bathsA12048077",
])
def test_dollar_fragments_still_junk(title):
    assert jf.is_listing_junk(title) is True


def test_lbaweb_card_is_emitted_by_v6():
    v6 = pytest.importorskip("dealledger_scraper_v6")
    from bs4 import BeautifulSoup
    html = f"""<div class="post"><div class="image">
      <a href="https://www.lbaweb.com/listing/390000-net-income-cryotherapy/"><img src="x.png"/></a>
      <span class="price">$390,000</span></div>
      <div class="textBox"><a href="https://www.lbaweb.com/listing/390000-net-income-cryotherapy/">
      <h3>$1M+ Revenue &#8211; $347K Net Income -9+ Years Established</h3></a>
      <span class="location">Contra Costa</span>
      <div class="post-info"><span class="tag">Listing No: <em>4801</em></span>
      <span class="tag">Status: <em class="text-secondary">Available</em></span></div>
      <a class="btn-view" href="https://www.lbaweb.com/listing/390000-net-income-cryotherapy/">View Business</a>
      </div></div>"""
    el = BeautifulSoup(html, "html.parser").select_one("div.post")
    row = v6.ListingExtractor.from_card(el, "https://www.lbaweb.com/listings", "lbaweb.com")
    assert row is not None and row["status"] == "active"
    assert row["title"].startswith("$1M+ Revenue")


# ── 4. wrapper-skipped log: once per broker ─────────────────────────────────
def test_wrapper_skip_log_summarises_once():
    v6 = pytest.importorskip("dealledger_scraper_v6")
    log = v6.WrapperSkipLog()
    assert log.summary() is None
    for n in (1, 0, 1, 1):                # peak: page 1, two probes, a re-read
        log.add(n)
    s = log.summary()
    assert "🧺 1 wrapper match(es)" in s and "3 page parse(s)" in s


def test_wrapper_line_not_printed_inside_page_parse():
    v6 = pytest.importorskip("dealledger_scraper_v6")
    src = inspect.getsource(v6)
    assert src.count("wrapper match(es) holding other cards skipped") == 1   # in WrapperSkipLog only
    assert "wrapper_log.add(n_wrappers)" in src
    assert "print(wrapper_log.summary())" in src
