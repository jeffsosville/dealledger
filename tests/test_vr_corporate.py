"""VR corporate index (vrbusinessbrokers.com): card status badge, skipped
offices, dedupe by office URL. No network: pages come from a saved fixture
(tests/fixtures/vr_corporate_cards.html, real cards from the 2026-11-23 index,
images removed)."""
import os
import sys

from bs4 import BeautifulSoup

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.environ.get("SCRAPERS", os.path.join(HERE, "..", "scrapers")))

import specialized_scrapers as ss  # noqa: E402
import run_specialized as rs  # noqa: E402

FIXTURE = os.path.join(HERE, "fixtures", "vr_corporate_cards.html")
DUP = "563832-fabrication-countertop"


def _html():
    with open(FIXTURE, encoding="utf-8") as f:
        return f.read()


def _run(pages=None, monkeypatch=None):
    s = ss.VRScraper()
    served = list(pages or [_html()])
    s._fetch_page = lambda n: served[n - 1] if n <= len(served) else "<html></html>"
    return s, s.scrape("1004", verbose=False)


def _dom(l):
    return ss.VRScraper.office_domain(l["listing_url"])


def test_sold_card_is_sold_with_verbatim_label(monkeypatch):
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    _, ls = _run()
    san = [l for l in ls if _dom(l) == "vrsanantonio.com"]
    assert len(san) == 1 and san[0]["status"] == "sold"
    assert san[0]["broker_status_text"] == "SOLD"


def test_under_contract_is_active_with_label(monkeypatch):
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    _, ls = _run()
    uc = [l for l in ls if _dom(l) == "vrtwincities.com"]
    assert len(uc) == 1 and uc[0]["status"] == "active"
    assert uc[0]["broker_status_text"] == "UNDER CONTRACT"


def test_cards_that_must_stay_active(monkeypatch):
    """NEW, REDUCED PRICE and no badge are active and carry no status label
    (principle 8: the reader must not reject what must pass)."""
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    _, ls = _run()
    by = {_dom(l): l for l in ls if l["status"] == "active"}
    assert by["vrbocaraton.com"].get("broker_status_text") is None      # NEW
    assert by["vrmiamicenter.com"].get("broker_status_text") is None    # no badge
    box = BeautifulSoup(
        '<div class="vrbb-listing-box"><div class="image-holder"><div class="image-top-left">'
        '<div class="reduced-price-button">REDUCED PRICE</div></div></div></div>', "html.parser")
    assert ss.VRScraper.card_status(box, "bizbizbiz.com") == ("active", None, ["REDUCED PRICE"])
    # nothing active carries a status word it did not earn
    assert all(l.get("broker_status_text") in (None, "UNDER CONTRACT")
               for l in ls if l["status"] == "active")


def test_skipped_offices_not_emitted(monkeypatch):
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    s, ls = _run()
    doms = {_dom(l) for l in ls}
    assert "vrdallas.com" not in doms and "vrcalgarybowvalley.com" not in doms
    assert s.last_stats["skipped"] == {"vrdallas.com": 1, "vrcalgarybowvalley.com": 1}
    assert s.is_skipped("https://www.vrdallas.com/listing/x/")
    assert s.is_skipped("https://sub.vrdallas.com/x")
    assert not s.is_skipped("https://vrdallas.com.evil.net/x")
    assert not s.is_skipped("https://vrmiamicenter.com/listing/x/")


def test_dup_url_one_row_most_final_status_either_order(monkeypatch):
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    _, ls = _run()
    rows = [l for l in ls if DUP in l["listing_url"]]
    assert len(rows) == 1 and rows[0]["status"] == "sold"
    a_cards = [c for c in BeautifulSoup(_html(), "html.parser").find_all("a") if DUP in c["href"]]
    assert len(a_cards) == 2
    # the SOLD card first, the un-badged duplicate later: still sold
    rev = "<html><body>" + str(a_cards[1]) + str(a_cards[0]) + "</body></html>"
    _, ls = _run([rev])
    assert [l["status"] for l in ls] == ["sold"]
    # a duplicate on a later page
    _, ls = _run([_html(), _html()])
    assert len([l for l in ls if DUP in l["listing_url"]]) == 1


def test_identity_is_the_url_and_has_no_price(monkeypatch):
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    _, ls = _run()
    for l in ls:
        assert l["listing_id"] == ss.hashlib.md5(l["listing_url"].encode()).hexdigest()
        d = rs.derive_broker_domain(l["listing_url"])
        assert rs.listing_key(l["listing_url"], d) == l["listing_url"]   # default key, as before
    _, ls2 = _run([_html().replace("$20,000,000", "$1,000")])
    assert {l["listing_url"] for l in ls} == {l["listing_url"] for l in ls2}


def test_broker_domain_breakdown_is_per_office(monkeypatch):
    monkeypatch.setattr(ss.time, "sleep", lambda *_: None)
    _, ls = _run()
    br = rs.domain_breakdown(ls)
    assert "vrdallas.com" not in br and br.get("vrmiamicenter.com") == 1
    assert sum(br.values()) == len(ls)


def test_missing_badge_is_active():
    box = BeautifulSoup('<div class="vrbb-listing-box"><div class="image-holder"></div></div>',
                        "html.parser")
    assert ss.VRScraper.card_status(box) == ("active", None, [])
