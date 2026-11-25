"""Offline tests for EREPropertyScraper (reputedbrokerage.com, G5 Beyot +
Essential Real Estate). No network: archive pages are faked."""
import os, sys
# append (not insert) so a PYTHONPATH pointing at a patched copy wins
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import pytest
import ere_property_scraper as ere
from ere_property_scraper import (EREPropertyScraper, parse_archive, parse_ere_price,
                                  asking_in_text, price_conflict, is_cre_type, label_status,
                                  cre_override)

try:                                    # job-3 shared vocabulary (position 16 of the stack)
    import listing_status as _ls        # noqa: F401
    VOCAB = ["shared", "local"]
except Exception:
    VOCAB = ["local"]


@pytest.fixture(params=VOCAB)
def vocab(request, monkeypatch):
    """Run the label tests against listing_status.py (when applied) AND the
    local fallback: both must give the same answers (principle 15)."""
    if request.param == "local":
        monkeypatch.setattr(ere, "_shared_lookup", None)
    return request.param


def card(pid, slug, title, price="$1,000,000", postfix=None, status="for-sale",
         label="Active", types=(), excerpt=None):
    pf = f'<span class="property-price-postfix">/ {postfix}</span>' if postfix else ""
    pr = f'<span class="property-price ere__loop-property-price">{price}{pf}</span>' if price is not None else ""
    lb = (f'<span class="ere__loop-property-badge-item ere__term-label {label.lower().replace(" ", "-")}">'
          f'<span class="ere__lpbi-inner">{label}</span></span>') if label else ""
    ty = ('<div class="property-type-list"><i class="fa fa-tag"></i>'
          + ", ".join(f'<a href="https://www.reputedbrokerage.com/property-type/x/" rel="tag">{t}</a>' for t in types)
          + '</div>') if types else ""
    ex = f'<div class="property-excerpt"><p>{excerpt}</p></div>' if excerpt else ""
    return f'''<div class="mg-bottom-30 ere-item-wrap"><div class="property-inner">
      <a class="property-favorite" data-property-id="{pid}"></a>
      <span class="ere__loop-property-badge-item ere__term-status {status}"><span>{status.replace("-", " ").title()}</span></span>
      {lb}
      <a class="property-link" href="https://www.reputedbrokerage.com/property/{slug}/"></a>
      <div class="property-item-content"><div class="property-heading">
        <h2 class="property-title"><a href="https://www.reputedbrokerage.com/property/{slug}/">{title}</a></h2>
        {pr}</div><div class="property-element-inline">{ty}</div>{ex}</div></div></div>'''


SEARCH_FORM = '''<div class="ere-property-advanced-search"><input type="hidden" name="status" value="for-sale"/>
  <select name="min-price" title="Min Price"><option value="">Min Price</option><option value="1000">$1,000</option></select>
  <select name="max-price" title="Max Price"><option value="">Max Price</option><option value="5000000">$5,000,000</option></select>
  <a href="https://www.reputedbrokerage.com/advanced-search/?status=for-sale">Search</a></div>'''

SIDEBAR = '''<aside class="widget ere_widget_recent_properties"><div class="list-recent-properties">''' + \
    card(999, "car-wash-for-lease", "Car Wash For Lease", status="for-lease") + "</div></aside>"


def page(heading, n, cards):
    return (f'<html><body>{SEARCH_FORM}<div class="ere-archive-property"><div class="ere-heading">'
            f'<h2>{heading}\t\t\t<sub>({n})</sub></h2></div><div class="ere-property">'
            + "".join(cards) + f'</div></div>{SIDEBAR}</body></html>')


class FakeResp:
    def __init__(self, status, text):
        self.status_code, self.text = status, text


class FakeSession:
    def __init__(self, pages):
        self.pages, self.calls = pages, []

    def get(self, url, timeout=30):
        self.calls.append(url)
        return FakeResp(200, self.pages[url]) if url in self.pages else FakeResp(404, "")


B = "https://www.reputedbrokerage.com"


def site():
    sale = [card(1, "branded-motel-for-sale-in-tx", "Branded Motel For Sale in TX", "$3,300,000"),
            card(2, "truck-stop", "Truck Stop &amp; Casino", "$6,500,000", "Net Income of Casino: $35 k"),
            card(3, "gas-station-no-price", "Gas Station Near Orange", price=None),
            card(4, "both-sale-and-lease", "Liquor Store Sale or Lease", "$450,000")]
    return {
        f"{B}/wp-sitemap-taxonomies-property-status-1.xml":
            "".join(f"<loc>{B}/property-status/{t}/</loc>" for t in ("for-lease", "for-sale", "not-available")),
        f"{B}/wp-sitemap-taxonomies-property-state-1.xml": f"<loc>{B}/property-state/texas/</loc><loc>{B}/property-state/others/</loc>",
        f"{B}/wp-sitemap-taxonomies-property-city-1.xml": f"<loc>{B}/property-city/none/</loc><loc>{B}/property-city/orange/</loc>",
        f"{B}/property-city/none/": page("None", 1, [sale[0]]),
        f"{B}/wp-sitemap-taxonomies-property-type-1.xml": f"<loc>{B}/property-type/motels-hotels/</loc>",
        f"{B}/property-type/motels-hotels/": page("Motels/Hotels", 1, [sale[0]]),
        f"{B}/property-status/for-sale/": page("For Sale", 4, sale[:2]),
        f"{B}/property-status/for-sale/page/2/": page("For Sale", 4, sale[2:]),
        f"{B}/property-status/for-lease/": page("For Lease", 2, [
            card(4, "both-sale-and-lease", "Liquor Store Sale or Lease", "$450,000", status="for-lease"),
            card(5, "retail-space-for-lease", "Retail Space", "$25 / per sq ft", status="for-lease")]),
        f"{B}/property-status/not-available/": page("Not Available", 1, [
            card(6, "sold-hotel", "Hotel North Dallas", "$4,500,000", status="not-available")]),
        f"{B}/property-state/texas/": page("Texas", 2, [sale[0], sale[2]]),
        f"{B}/property-state/others/": page("Others", 1, [sale[1]]),
        f"{B}/property-city/orange/": page("Orange", 1, [sale[2]]),
    }


def run():
    s = EREPropertyScraper("www.reputedbrokerage.com", page_delay=0, session=FakeSession(site()))
    return s, {r["broker_listing_id"]: r for r in s.scrape("acct", verbose=False)}


def test_price_plain():
    assert parse_ere_price("$2,990,000") == (2990000.0, None)


def test_price_slash_note_is_not_price():
    assert parse_ere_price("$6,500,000 / Net Income of Casino: $35 k") == (6500000.0, "Net Income of Casino: $35 k")
    p, note = parse_ere_price("$25 / per sq ft")
    assert p == 25.0 and note == "per sq ft"


def test_price_absent():
    assert parse_ere_price(None) == (None, None)
    assert parse_ere_price("Call for price") == (None, None)


def test_archive_ignores_search_form_and_sidebar():
    n, name, cards = parse_archive(site()[f"{B}/property-status/for-sale/"])
    assert (n, name) == (4, "For Sale")
    assert [c["post_id"] for c in cards] == ["1", "2"]
    assert all("/property/" in c["url"] for c in cards)
    assert not any("price" in c["title"].lower() for c in cards)


def test_status_filter():
    s, rows = run()
    assert rows["1"]["status"] == "active" and rows["2"]["status"] == "active"
    assert rows["3"]["status"] == "active"
    assert rows["4"]["status"] == "active"          # for-sale AND for-lease -> offered for sale
    assert "5" not in rows                           # lease-only never emitted
    assert rows["6"]["status"] == "inactive"         # not-available retires
    assert s.stats["emitted_active"] == s.stats["site_counts"]["for-sale"] == 4
    assert s.stats["dropped_lease_only"] == 1


def test_price_and_no_price_rows():
    _, rows = run()
    assert rows["2"]["price"] == 6500000.0 and rows["2"]["price_note"] == "Net Income of Casino: $35 k"
    assert rows["2"]["price_not_disclosed"] is False
    assert rows["3"]["price"] is None and rows["3"]["price_not_disclosed"] is True


def test_location_and_identity():
    _, rows = run()
    assert rows["1"]["state"] == "TX" and rows["3"]["city"] == "Orange"
    assert rows["2"]["state"] is None                # 'others' is not a state
    assert rows["1"]["listing_url"] == f"{B}/property/branded-motel-for-sale-in-tx/"
    _, again = run()
    assert {k: r["listing_id"] for k, r in rows.items()} == {k: r["listing_id"] for k, r in again.items()}


def test_never_emits_non_property_urls():
    _, rows = run()
    assert all("/property/" in r["listing_url"] and "advanced-search" not in r["listing_url"]
               for r in rows.values())


def test_city_none_term_and_type():
    _, rows = run()
    assert rows["1"]["city"] is None                 # the theme's literal 'none' city term
    assert rows["1"]["business_type"] == "Motels/Hotels"


# ---- revision 2026-11-12 (owner decision 7) --------------------------------

def site_v2():
    """For Sale 10: 2 plain active, 1 Sold, 1 Expired, 1 Under Contract, 1 On
    Hold, 1 Coming Soon, 1 Lands, 1 Apartment (multifamily), 1 Gas Station that
    is ALSO typed Lands (kept), and one price-conflict card."""
    sale = [
        card(11, "plain-a", "Motel A", "$3,300,000", types=["Motels/Hotels"]),
        card(12, "hotel-conflict", "Hotel For Sale", "$4,500,000", postfix="High Revenue",
             types=["Motels/Hotels"], excerpt="16 Rooms | Annual Revenue: ~$150K Asking Price: $550K (Negotiable)"),
        card(13, "sold-one", "Gas Station Near Tyler", "$900,000", label="Sold", types=["Gas Stations"]),
        card(14, "expired-one", "Liquor Store", "$300,000", label="Expired", types=["Liquor Stores"]),
        card(15, "uc-one", "C-Store", "$700,000", label="Under Contract", types=["C-Stores & Restaurants"]),
        card(20, "hold-one", "Truck Stop", "$2,000,000", label="On Hold", types=["Gas Stations"]),
        card(16, "soon-one", "Coming Soon Store", "$500,000", label="Coming Soon", types=["Gas Stations"]),
        card(17, "land-one", "10 Acres Near Tyler", "$200,000", types=["Lands"]),
        card(18, "prime-multifamily-investment", "Prime Multifamily Investment", "$1,049,000", types=["Apartment"]),
        card(31, "gas-station-with-real-estate-in-tx", "Land Approved for Gas Station in TX", "$2,000,000",
             types=["Lands"], excerpt="Land Size: 5.12 Acres Fuel Sales: ~160,000 Gallons/Month Inside Sales: "
                                      "~$130,000/Month Restaurant Sales: ~$38,000/Month Asking Price: $2,000,000"),
        card(32, "land-for-sale-approved-for-truck-stop", "Land For Sale Approved for Truck Stop", "$900,000",
             types=["Lands"], excerpt="Approved for Truck Stop Land: 3.25 Acres IMST Inside Sales: $165,000"),
        card(19, "gas-with-land", "Gas Station on 5 Acres", "$1,200,000", types=["Gas Stations", "Lands"],
             excerpt="Asking Price: $1.2 million"),
    ]
    return {
        f"{B}/wp-sitemap-taxonomies-property-status-1.xml":
            "".join(f"<loc>{B}/property-status/{t}/</loc>" for t in ("for-sale", "not-available")),
        f"{B}/property-status/for-sale/": page("For Sale", 12, sale),
        f"{B}/property-status/not-available/": page("Not Available", 2, [
            card(21, "na-sold", "Old Hotel", "$1,000,000", status="not-available", label="Sold",
                 types=["Motels/Hotels"]),
            card(22, "na-land", "Old Land", "$100,000", status="not-available", types=["Lands"])]),
    }


def run_v2(vocab=None):
    s = EREPropertyScraper("www.reputedbrokerage.com", page_delay=0, session=FakeSession(site_v2()))
    return s, {r["broker_listing_id"]: r for r in s.scrape("acct", verbose=False)}


def test_label_sold_and_expired_become_sold_verbatim(vocab):
    s, rows = run_v2()
    assert rows["13"]["status"] == "sold" and rows["13"]["broker_status_text"] == "Sold"
    assert rows["14"]["status"] == "sold" and rows["14"]["broker_status_text"] == "Expired"
    assert s.stats["sold_by_label"] == {"Sold": 1, "Expired": 1}


def test_under_contract_stays_active_with_label_and_plain_active_carries_nothing(vocab):
    _, rows = run_v2()
    assert rows["15"]["status"] == "active" and rows["15"]["broker_status_text"] == "Under Contract"
    assert rows["11"]["status"] == "active" and "broker_status_text" not in rows["11"]


def test_coming_soon_not_emitted(vocab):
    s, rows = run_v2()
    assert "16" not in rows and s.stats["dropped_label"] == {"Coming Soon": 1}


def test_cre_lands_and_apartment_not_emitted_mixed_type_kept(vocab):
    s, rows = run_v2()
    assert "17" not in rows and "18" not in rows and "22" not in rows
    assert rows["19"]["status"] == "active"           # Gas Stations + Lands: an operating business
    # 18 (Prime Multifamily) and 31/32 now go out through the title scope filter before the type rule
    assert s.stats["dropped_cre"] == 2 and s.stats["dropped_cre_for_sale"] == 1
    assert s.stats["dropped_cre_by_type"] == {"Lands": 2}
    assert s.stats["dropped_scope_n"] == 3
    assert "32" not in rows                           # approved land, IMST projections
    # 31 is an operating station typed Lands (the CRE override would keep it), but its title
    # "Land Approved for Gas Station" is on Jeff's 2026-11-23 scope list -> not emitted
    assert "31" not in rows and s.stats["cre_overrides"] == []


def test_for_sale_subtraction_adds_up(vocab):
    s, rows = run_v2()
    st = s.stats
    assert st["site_counts"]["for-sale"] == 12
    moved = sum(st["for_sale_moved_by_label"].values())
    assert st["for_sale_moved_by_label"] == {"Sold -> sold": 1, "Expired -> sold": 1, "On Hold -> inactive": 1}
    assert (st["emitted_active"] + moved + st["dropped_cre_for_sale"] + st["dropped_scope_n"]
            + st["dropped_label_for_sale"]) == 12
    assert st["emitted_active"] == 4                  # 11, 12, 15, 19


def test_on_hold_is_inactive_with_label(vocab):
    _, rows = run_v2()
    assert rows["20"]["status"] == "inactive" and rows["20"]["broker_status_text"] == "On Hold"


def test_not_available_card_labelled_sold_is_sold(vocab):
    # status term not-available -> inactive, but the broker's Sold label is more final
    _, rows = run_v2()
    assert rows["21"]["status"] == "sold" and rows["21"]["broker_status_text"] == "Sold"


def test_label_vocabulary(vocab):
    d = "www.reputedbrokerage.com"
    assert label_status("Sold", d) == ("sold", "Sold")
    assert label_status("Expired", d) == ("sold", "Expired")
    assert label_status("Expired", "other.com")[0] == "inactive"   # override is per domain
    assert label_status("Under Contract", d) == ("active", "Under Contract")
    assert label_status("On Hold", d) == ("inactive", "On Hold")
    assert label_status("Active", d) == ("active", None)
    assert label_status("Coming Soon", d)[0] == ere.DROP
    assert label_status("", d) is None


def test_price_conflict_flagged_header_kept(vocab):
    s, rows = run_v2()
    r = rows["12"]
    assert r["price"] == 4500000.0                    # header price stays asking_price
    assert r["price_conflict"] and "$550,000" in r["price_conflict"]
    assert rows["19"]["price_conflict"] is None       # $1.2M vs $1,200,000 agree
    assert rows["11"]["price_conflict"] is None       # no excerpt
    assert [c["url"] for c in s.stats["price_conflicts"]] == [f"{B}/property/hotel-conflict/"]


def test_asking_parser_and_threshold():
    assert asking_in_text("Asking Price:$3.9M")[0] == 3_900_000
    assert asking_in_text("Asking Price: $4.5 million")[0] == 4_500_000
    assert asking_in_text("Asking $2,300,000 firm")[0] == 2_300_000
    assert asking_in_text("Revenue $1.1M")[0] is None
    assert asking_in_text("hall and parking. Asking Price: $9,200, […]")[0] is None   # excerpt cut
    assert asking_in_text("Asking Price: $9,200,000 […]")[0] == 9_200_000
    assert asking_in_text("Asking Price: $2 Million")[0] == 2_000_000
    assert asking_in_text("Asking- $4.7 Mil")[0] == 4_700_000
    # live excerpt 2026-11-12: lines glued, no space after the number
    assert asking_in_text("Asking Price: $9,200,000Gross Annual Revenue: $1,200,000")[0] == 9_200_000
    assert asking_in_text("Asking: $350k+Inv")[0] == 350_000
    assert price_conflict(1_000_000, "Asking Price: $1.5M") is None     # exactly 50%: not a conflict
    assert price_conflict(1_000_000, "Asking Price: $1.6M") is not None
    assert price_conflict(1_600_000, "Asking Price: $1M") is not None   # symmetric
    assert price_conflict(None, "Asking Price: $1M") is None


def test_is_cre_type():
    assert is_cre_type(["Lands"]) and is_cre_type(["Apartment"]) and is_cre_type(["Multifamily"])
    assert not is_cre_type(["Gas Stations", "Lands"]) and not is_cre_type([]) and not is_cre_type(["Investments"])


def test_ids_unchanged_by_revision(vocab):
    _, rows = run_v2()
    import hashlib
    for r in rows.values():
        assert r["listing_id"] == hashlib.md5(r["listing_url"].encode()).hexdigest()


def test_header_without_suffix_is_flagged():
    # live 2026-11-12: header "$899" with excerpt "Asking Price: $899K" (broker typed thousands)
    assert price_conflict(899.0, "Asking Price: $899K") is not None


def test_cre_override_on_live_titles():
    # 2026-11-12 live cards (property-type Lands)
    keep = [("Land for GS 45min from Houston, TX",
             "Inside Sales: ~$175,000/Month Fuel Sales: ~255,000 Gallons/Month Land Size: 6.5 Acres"),
            ("Land Approved for Gas Station in TX",
             "Land Size: 5.12 Acres Fuel Sales: ~160,000 Gallons/Month Inside Sales: ~$130,000/Month"),
            ("Branded Motel Near Waco", None)]
    drop = [("Land Approved for Gas Station Close to DFW",
             "Land Size: 2.73 Acres Feasibility Projections Sales Inside Sales: $130,000/month Fuel"),
            ("Land In Dallas Approved for Gas Station", "Approved for Gas Station Land: 0.63 Acre IMST Inside Sales: $1"),
            ("Land For Sale Approved for Truck Stop", "Land: 3.25 Acres IMST Inside Sales: $165,00"),
            ("Gas Station Pre-Approved Land Near Houston, Tx", "Land Area: 2.2 Acres Property Type: Land suitable"),
            ("Unimproved Land 1hr from Waco, TX", "There is power on the road"),
            ("Prime Multifamily Investment Opportunity", "Fourplex 4 x 2bd/1ba")]
    for t, ex in keep:
        assert cre_override(t, ex), t
    for t, ex in drop:
        assert cre_override(t, ex) is None, t


# ---- 2026-11-24 rv-specialized (Jeff, 23 Nov): scope filter, K/M prices, state ----

SCOPE_DROP = [
    "Land for GS 45min from Houston", "Land Approved for Gas Station in TX", "Gas Station Land In Palestine",
    "Pre-Approved Land For Gas Station", "Land For Sale Approved for Truck Stop",
    "Prime Multifamily Investment", "Multi-Family Building Tulsa", "Strip Center For Sale Near Dallas",
    "Strip Centre in Tulsa", "Free Standing Building for Liquor/C-Store",
]
SCOPE_KEEP = [
    "Gas Station With Retail Plaza", "Gas Station + Strip Center in Texas", "Gas Station with House Near Tyler",
    "Gas Station and Retail Plaza Near Atoka", "Motel + House Near Dallas",
    "Branded Motel Out Of Texas", "Liquor C Store With Property For Sale", "Gas Station Near Orange,TX",
    "Truck Stop & Casino", "Car Wash With Land",
]


@pytest.mark.parametrize("title", SCOPE_DROP)
def test_scope_filter_drops_land_multifamily_strip_center_building(title):
    assert ere.scope_reason(title, "www.reputedbrokerage.com")


@pytest.mark.parametrize("title", SCOPE_KEEP)
def test_scope_filter_keeps_operating_businesses_with_property(title):
    assert ere.scope_reason(title, "www.reputedbrokerage.com") is None


def test_scope_filter_is_reputedbrokerage_only():
    assert ere.scope_reason("Land for GS 45min from Houston", "someotherbroker.com") is None
    assert ere.scope_reason("Land for GS 45min from Houston", "reputedbrokerage.com")


def site_v3():
    sale = [
        card(41, "gs-plaza", "Gas Station With Retail Plaza", "$2,500,000", types=["Gas Stations"]),
        card(42, "land-gs", "Land for GS 45min from Houston", "$900,000", types=["Gas Stations"]),
        card(43, "strip", "Strip Center Near Dallas", "$1,200,000", types=["Retail"]),
        card(44, "tyler", "Gas Station Near Tyler Texas With Property", "$599", postfix="Inside Sales: $50K",
             types=["Gas Stations"]),
        card(45, "liquor", "Liquor C Store With Property For Sale", "$725", types=["Liquor Stores"]),
        card(46, "dup-a", "Gas Station Near Orange,TX", "$400,000", types=["Gas Stations"]),
        card(47, "dup-b", "Gas Station Near Orange,TX", "$420,000", types=["Gas Stations"]),
        card(48, "tulsa", "Gas Station near Tulsa", "$500,000", types=["Gas Stations"]),
        card(49, "okla", "C-Store Oklahoma", "$450,000", types=["Gas Stations"]),
        card(50, "nowhere", "Motel For Sale", "$800,000", types=["Motels/Hotels"]),
    ]
    detail = lambda body: f'<html><body><div class="ere__single-property-description">{body}</div></body></html>'
    return {
        f"{B}/wp-sitemap-taxonomies-property-status-1.xml": f"<loc>{B}/property-status/for-sale/</loc>",
        f"{B}/property-status/for-sale/": page("For Sale", 10, sale),
        f"{B}/property/tyler/": detail("Gas Station Asking Price: $599K + inventory Owner Financing"),
        f"{B}/property/liquor/": detail("Liquor store. Asking Price: $725,000 Disclaimer"),
    }


def run_v3(pages=None):
    s = EREPropertyScraper("www.reputedbrokerage.com", page_delay=0, session=FakeSession(pages or site_v3()))
    return s, {r["broker_listing_id"]: r for r in s.scrape("acct", verbose=False)}


def test_scope_drops_only_the_out_of_scope_for_sale_cards():
    s, rows = run_v3()
    assert "42" not in rows and "43" not in rows
    assert "41" in rows                                   # gas station + retail plaza stays
    assert len(s.stats["dropped_scope"]) == 2
    assert len(rows) == 8


def test_truncated_header_price_recovered_from_the_pages_own_asking_price():
    s, rows = run_v3()
    assert rows["44"]["price"] == 599_000 and rows["45"]["price"] == 725_000
    assert len(s.stats["price_fixed_from_detail"]) == 2 and not s.stats["price_flagged"]


def test_header_price_page_does_not_confirm_is_null_and_flagged():
    pages = site_v3()
    pages[f"{B}/property/tyler/"] = '<div class="ere__single-property-description">Asking Price: $350K</div>'
    del pages[f"{B}/property/liquor/"]                      # fetch fails
    s, rows = run_v3(pages)
    for pid in ("44", "45"):
        assert rows[pid]["price"] is None and rows[pid]["price_not_disclosed"] is True
        assert "not a plausible asking price" in rows[pid]["price_note"]
    assert len(s.stats["price_flagged"]) == 2


def test_normal_prices_are_untouched_and_never_trigger_a_detail_fetch():
    s, rows = run_v3()
    assert rows["41"]["price"] == 2_500_000
    assert not any("/property/gs-plaza/" in u for u in s.session.calls)


def test_recover_only_exact_k_or_m_multiple():
    assert ere.recover_suffixed_price(599, "Asking Price: $599K + inventory") == 599_000
    assert ere.recover_suffixed_price(2, "Asking Price: $2 M") == 2_000_000
    assert ere.recover_suffixed_price(725, "Asking Price: $725,000") == 725_000
    assert ere.recover_suffixed_price(599, "Asking Price: $350K") is None      # unrelated figure
    assert ere.recover_suffixed_price(599, "no price here") is None


def test_state_filled_from_title_when_archive_gave_none():
    s, rows = run_v3()
    assert rows["46"]["state"] == "TX" and rows["48"]["state"] == "OK" and rows["49"]["state"] == "OK"
    assert rows["50"]["state"] is None                       # says nothing: left empty
    assert rows["44"]["state"] == "TX"                       # 'Texas' in the title
    assert ere.infer_state("Gas Station Near Orange,TX") == ("TX", "title code 'TX'")
    assert ere.infer_state("OK Corral Motel")[0] is None     # the word OK is not a state


def test_same_title_on_different_urls_are_two_rows():
    _, rows = run_v3()
    assert rows["46"]["listing_url"] != rows["47"]["listing_url"]
    assert rows["46"]["title"] == rows["47"]["title"]
    assert rows["46"]["listing_id"] != rows["47"]["listing_id"]


def test_state_from_tx_case_variant_and_url_slug():
    assert ere.infer_state("Store For Sale Near Benbrook, Tx") == ("TX", "title code 'Tx'")
    st = ere.infer_state("Branded Motel For Sale", None, f"{B}/property/branded-motel-for-sale-in-louisiana/")
    assert st[0] == "LA"
    assert ere.infer_state("Flagged Hotel for Sale", None, f"{B}/property/flagged-hotel-for-sale/") == (None, None)
