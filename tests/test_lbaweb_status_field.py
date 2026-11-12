"""Labelled card status field (job 3, 2026-11-12): V6 reads a labelled
"Status: <value>" / "Listing Status:" / "Availability:" field on a card and
classifies the value through scrapers/listing_status.py. lbaweb.com prints
<span class="tag">Status: <em class="text-secondary">Sold</em></span> on 382 of
its 593 cards, which looks_sold() / is_sold_or_pending() never saw.

Sold -> 'sold' + verbatim label; Pending / Under Contract -> 'active' + label
(decision 6); Coming Soon / For Lease -> not emitted (decision 5); unknown
values, bare "Status" headings and prose -> no change (principle 8); a plain
active value never un-sells a card the generic rules call sold; ids never
depend on the status (principle 4)."""
import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402
from dealledger_scraper_v6 import ListingExtractor, card_status_field  # noqa: E402

BASE = "https://www.lbaweb.com/listings"
NAME = "lbaweb.com"


def _lba_card(status="Sold", slug="senior-care-facility", title="Senior Care facility",
              no="4739", price="$200,000"):
    """The live lbaweb.com card markup (2026-11-12), status value swappable;
    status=None removes the Status field entirely."""
    status_span = (f"""<span class="tag">
     Status:
     <em class="text-secondary">{status}</em>
    </span>""" if status is not None else "")
    return f"""
<div class="col-lg-4 col-md-6 col-12"><div class="post">
  <div class="image">
   <a href="https://www.lbaweb.com/listing/{slug}/"><img alt="" class="img-fluid" src="x.png"/></a>
   <span class="price"><span class="price">{price}</span></span>
  </div>
  <div class="textBox">
   <a href="https://www.lbaweb.com/listing/{slug}/"><h3>{title}</h3></a>
   <span class="location"><i class="icon-location"></i>Placer County</span>
   <div class="d-flex justify-content-between post-info">
    <span class="tag">Listing No: <em>{no}</em></span>
    {status_span}
   </div>
   <div class="bottom d-flex justify-content-between align-items-center">
    <a class="btn-view" href="https://www.lbaweb.com/listing/{slug}/">View Business</a>
   </div>
  </div>
</div></div>"""


def _el(html, sel="div.post"):
    return BeautifulSoup(html, "html.parser").select_one(sel)


def _row(html, sel="div.post", base=BASE, name=NAME):
    return ListingExtractor.from_card(_el(html, sel), base, name)


# ── card_status_field ──────────────────────────────────────────────────────

@pytest.mark.parametrize("value,expected", [
    ("Sold", ("sold", "Sold")),
    ("SOLD", ("sold", "SOLD")),
    ("Pending", ("active", "Pending")),
    ("Under Contract", ("active", "Under Contract")),
    ("Available", ("active", None)),
    ("Withdraw", ("inactive", "Withdraw")),   # INACTIVE_PHRASES since j3-polish
    ("Withdrawn", ("inactive", "Withdrawn")),
    ("Owner retiring", None),
])
def test_field_values(value, expected):
    assert card_status_field(_el(_lba_card(value)), "www.lbaweb.com") == expected


def test_no_field_is_none():
    assert card_status_field(_el(_lba_card(None)), "www.lbaweb.com") is None


@pytest.mark.parametrize("html", [
    '<div class="c"><p>Listing Status: Under Contract</p><a href="/l/1">x</a></div>',
    '<div class="c"><span>Availability:</span> <b>Sold</b></div>',
    '<div class="c"><span>Business Status:</span><span>Pending</span></div>',
])
def test_label_variants(html):
    assert card_status_field(_el(html, "div.c")) is not None


@pytest.mark.parametrize("html", [
    # bare heading, no colon: a filter widget ("Status / Available / Sold")
    '<div class="c"><h4>Status</h4><label>Sold</label><label>Available</label></div>',
    # the word inside prose is never read
    '<div class="c"><p>Our current status: sold out of inventory every summer.</p></div>',
    '<div class="c"><p>Owner explains the status: Sold the second store in 2019.</p></div>',
    # label mid-line (not at the start of its own text node)
    '<div class="c"><p>Marital status: sold</p></div>',
])
def test_not_a_field(html):
    assert card_status_field(_el(html, "div.c")) is None


def test_most_final_of_several_fields_wins():
    html = '<div class="c"><span>Status:</span><em>Pending</em><span>Status:</span><em>Sold</em></div>'
    assert card_status_field(_el(html, "div.c")) == ("sold", "Sold")


def test_module_absent_is_inert(monkeypatch):
    monkeypatch.setattr(v6, "_sf_lookup", None)
    assert card_status_field(_el(_lba_card("Sold")), "www.lbaweb.com") is None


# ── from_card ──────────────────────────────────────────────────────────────

def test_sold_card_is_sold_with_label():
    r = _row(_lba_card("Sold"))
    assert r["status"] == "sold" and r["broker_status_text"] == "Sold"
    assert r["url"] == "https://www.lbaweb.com/listing/senior-care-facility/"


def test_pending_card_is_active_with_label():
    r = _row(_lba_card("Pending"))
    assert r["status"] == "active" and r["broker_status_text"] == "Pending"


def test_available_card_unchanged():
    r = _row(_lba_card("Available"))
    assert r["status"] == "active" and "broker_status_text" not in r


def test_unknown_value_unchanged():
    r = _row(_lba_card("Owner retiring"))
    assert r["status"] == "active" and "broker_status_text" not in r


def test_withdraw_card_is_inactive_with_label():
    # lbaweb's 2 "Withdraw" cards (12 Nov): inactive, label verbatim (j3-polish)
    r = _row(_lba_card("Withdraw"))
    assert r["status"] == "inactive" and r["broker_status_text"] == "Withdraw"


@pytest.mark.parametrize("value", ["Coming Soon", "Leased"])
def test_drop_values_not_emitted_and_counted(value):
    v6._STATUS_FIELD_DROPS.clear()
    assert _row(_lba_card(value, slug="x-" + value.lower().replace(" ", "-"))) is None
    assert sum(len(s) for s in v6._STATUS_FIELD_DROPS["lbaweb.com"].values()) == 1
    v6._STATUS_FIELD_DROPS.clear()


def test_for_lease_not_emitted():
    # caught earlier by the CRE/lease text gate; either way never emitted
    assert _row(_lba_card("For Lease", slug="x-for-lease")) is None


def test_id_and_url_do_not_depend_on_status():
    ids = {(_row(_lba_card(s))["id"], _row(_lba_card(s))["url"])
           for s in (None, "Sold", "Pending", "Available", "Withdraw")}
    assert len(ids) == 1


def test_plain_active_field_never_unsells_generic_sold():
    # a SOLD ribbon (looks_sold) and a contradictory "Status: Available"
    html = _lba_card("Available").replace(
        '<div class="image">', '<div class="image"><span class="ribbon">SOLD</span>')
    assert _row(html)["status"] == "sold"


def test_under_contract_field_beats_generic_class():
    # decision 6: the broker's explicit Under Contract label means active
    html = _lba_card("Under Contract").replace('class="post"', 'class="post under-contract"')
    r = _row(html)
    assert r["status"] == "active" and r["broker_status_text"] == "Under Contract"


def test_no_field_card_identical_to_generic():
    html = _lba_card(None).replace('<div class="image">',
                                   '<div class="image"><span class="badge">Sold</span>')
    r = _row(html)
    # j3-v6_generic_status keeps the badge's own word as the label (decision 6)
    assert r["status"] == "sold" and r.get("broker_status_text") in (None, "Sold")
