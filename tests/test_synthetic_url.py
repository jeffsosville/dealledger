"""Synthetic URLs for listing cards with no detail link (2026-11-11, rev 2026-11-12).

Owner decisions 1-4 (2026-11-12):
  - gated: only SYNTHETIC_URL_DOMAINS (bucket (b)) get synthetic URLs; the
    broker-ref rule also covers BROKER_REF_ONLY_DOMAINS (naz-business.com);
    every other broker keeps url = index URL, id = sha256(domain|title.lower()).
  - every synthetic fragment is '#dl-<slug>'.
  - no-link id = sha256(domain|title.lower()|slug(city-state)); no parsed
    location -> sha256(domain|title.lower()) (today's id).
  - a printed broker ref -> url = index#dl-<slug(ref)>, id = sha256(url).
  - no price tiebreak: a same-slug collision between different ids is left.
"""
import hashlib
import sys
from pathlib import Path

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
from dealledger_scraper_v6 import (  # noqa: E402
    SYNTHETIC_URL_DOMAINS, ListingExtractor, _make_api_listing,
    assign_synthetic_urls, broker_ref_enabled, extract_broker_ref,
    stable_listing_id, synthetic_listing_url, synthetic_slug,
    synthetic_urls_enabled,
)

DOMAIN = "peakbusinessbrokersllc.com"          # allowlisted (bucket b)
INDEX = f"https://{DOMAIN}/business-listings"
OFF_DOMAIN = "thedecaingroup.com"              # not allowlisted
OFF_INDEX = f"https://{OFF_DOMAIN}/listings"
NAZ_INDEX = "https://naz-business.com/listings"  # bucket (c): ref rule only


def _sha(key):
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _title_id(title, domain=DOMAIN):
    return _sha(f"{domain}|{title.strip().lower()}")


def _loc_id(title, loc, domain=DOMAIN):
    return _sha(f"{domain}|{title.strip().lower()}|{loc}")


def _card(title, loc="Austin, TX", price="$250,000", link=None, index=INDEX,
          extra=""):
    inner = f'<h3>{title}</h3>'
    if loc:
        inner += f'<p>Location: {loc}</p>'
    inner += f'<p>Asking Price: {price}</p><p>Cash Flow: $90,000</p>{extra}'
    if link:
        inner += f'<a href="{link}">View details</a>'
    html = f'<div class="listing">{inner}</div>'
    el = BeautifulSoup(html, "html.parser").select_one("div.listing")
    return ListingExtractor.from_card(el, index, "Example Broker")


# ── allowlist ───────────────────────────────────────────────────────────────
def test_allowlist_is_the_33_bucket_b_domains_with_and_without_www():
    assert len(SYNTHETIC_URL_DOMAINS) == 33
    assert synthetic_urls_enabled("peakbusinessbrokersllc.com")
    assert synthetic_urls_enabled("www.dentalbroker.com")
    assert synthetic_urls_enabled("WWW.Patriotbusinessadvisors.com")
    assert not synthetic_urls_enabled("thedecaingroup.com")
    # naz: ref rule only, no title/location synthetic URLs
    assert not synthetic_urls_enabled("naz-business.com")
    assert broker_ref_enabled("naz-business.com")
    assert broker_ref_enabled("www.naz-business.com")
    assert not broker_ref_enabled("thedecaingroup.com")


# ── slug / url ──────────────────────────────────────────────────────────────
def test_slug_is_deterministic_and_normalised():
    a = synthetic_slug("Café & Bakery — Downtown!!", "Austin", "TX")
    assert a == synthetic_slug("Café & Bakery — Downtown!!", "Austin", "TX")
    assert a == "cafe-bakery-downtown-austin-tx"


def test_slug_skips_missing_parts_and_caps_length():
    assert synthetic_slug("Pizza Shop", None, "TX") == "pizza-shop-tx"
    assert synthetic_slug("Pizza Shop", "", None) == "pizza-shop"
    long = synthetic_slug("x " * 200)
    assert len(long) <= 80 and not long.endswith("-")


def test_synthetic_url_has_dl_prefix_and_replaces_fragment():
    u = synthetic_listing_url(INDEX + "#top", "Pizza Shop", "Austin", "TX")
    assert u == INDEX + "#dl-pizza-shop-austin-tx"


def test_cap_never_cuts_location():
    long_title = "This interior, exterior, commercial, and specialty painting services " * 3
    a = synthetic_listing_url(INDEX, long_title, "Stamford", "CT")
    b = synthetic_listing_url(INDEX, long_title, "Philadelphia", "PA")
    assert a.endswith("-stamford-ct") and b.endswith("-philadelphia-pa")
    assert a != b


def test_no_price_ever_in_the_url():
    import inspect
    assert "price" not in inspect.signature(synthetic_listing_url).parameters


# ── broker refs ─────────────────────────────────────────────────────────────
def test_labelled_refs_are_found():
    d = DOMAIN
    assert extract_broker_ref("Listing #1088 Under Contract Bakery", d) == "1088"
    assert extract_broker_ref("Listing# 6242 Seller pursuing", d) == "6242"
    assert extract_broker_ref("LISTING ID DS8887 Dispensary", d) == "DS8887"
    assert extract_broker_ref("Gym in Frisco ID:875354 Turn-key", d) == "875354"
    assert extract_broker_ref("Salon #3070 in Tampa", d) == "3070"
    assert extract_broker_ref("Ref # 441 Plumbing", d) == "441"


def test_prices_phones_years_licences_are_not_refs():
    d = DOMAIN
    assert extract_broker_ref("Asking $250,000 Cash Flow $90,000", d) is None
    assert extract_broker_ref("Call 602-555-1234 or #555-1234", d) is None
    assert extract_broker_ref("Listing #2019 financials", d) is None   # year
    assert extract_broker_ref("Established in #2015", d) is None
    assert extract_broker_ref("CA DRE #02340613 Available", d) is None  # licence
    assert extract_broker_ref("Suite #200, Phoenix AZ", d) is None
    assert extract_broker_ref("#1 car wash in town", d) is None
    assert extract_broker_ref("Reference 2019 tax returns", d) is None


def test_per_broker_unlabelled_refs():
    assert extract_broker_ref(
        "CA2633 Los Angeles County $1,600,000 $1,800,000 Tax firm",
        "accountingfirmsold.com") == "CA2633"
    # printed with a space / lower-case suffix: whitespace dropped, stable
    assert extract_broker_ref("OH 2083 Central Ohio $1M", "accountingfirmsold.com") == "OH2083"
    assert extract_broker_ref("WI1084a Appleton Area $190,000",
                              "accountingfirmsold.com") == "WI1084a"
    assert extract_broker_ref("2246 Asking Price: $ 200,000",
                              "naz-business.com") == "2246"
    assert extract_broker_ref("FOR SALE CDT 123 Phoenix $900,000",
                              "www.dentalbroker.com") == "CDT123"
    assert extract_broker_ref("ES-01 Confidential HVAC", "selahdirective.com") == "ES-01"
    # same text on an off-list broker: never a ref
    assert extract_broker_ref("Listing #1088 Bakery", OFF_DOMAIN) is None
    assert extract_broker_ref("2246 Asking Price: $ 200,000", OFF_DOMAIN) is None


# ── from_card: allowlisted no-link card ─────────────────────────────────────
def test_no_link_card_location_in_id_and_dl_url():
    c = _card("Profitable Pizza Restaurant")
    assert c["id"] == _loc_id("Profitable Pizza Restaurant", "austin-tx")
    assert c["url"] == INDEX            # gates still see the index URL ...
    assign_synthetic_urls([c], INDEX)   # ... until the end-of-broker pass
    assert c["url"] == INDEX + "#dl-profitable-pizza-restaurant-austin-tx"
    assert c["url_is_listing_specific"] is False
    assert c["id"] == _loc_id("Profitable Pizza Restaurant", "austin-tx")
    assert "_no_detail_link" not in c   # internal marker never reaches the writer


def test_same_title_different_city_splits():
    # peak: franchise boilerplate reused as the title in several cities
    t = "This interior, exterior, commercial, and specialty painting services"
    a, b = _card(t, loc="Stamford, CT"), _card(t, loc="Philadelphia, PA")
    assert a["id"] != b["id"]
    stats = assign_synthetic_urls([a, b], INDEX)
    assert a["url"] != b["url"] and stats["unresolved_collisions"] == 0


def test_no_location_falls_back_to_todays_id():
    c = _card("Profitable Pizza Restaurant", loc=None)
    assert c["city"] is None and c["state"] is None
    assert c["id"] == _title_id("Profitable Pizza Restaurant")
    assign_synthetic_urls([c], INDEX)
    assert c["url"] == INDEX + "#dl-profitable-pizza-restaurant"


def test_ref_card_url_and_id_use_the_ref():
    c = _card("Profitable Pizza Restaurant", extra="<p>Listing #1088</p>")
    assign_synthetic_urls([c], INDEX)
    assert c["url"] == INDEX + "#dl-1088"
    assert c["id"] == _sha(INDEX + "#dl-1088")
    assert c["id"] == stable_listing_id(c["title"], c["url"], INDEX, DOMAIN)
    assert c["url_is_listing_specific"] is False


def test_ref_id_rekeyed_on_index_url_for_page_2_cards():
    page2 = INDEX + "?page=2"
    c = _card("Car Wash", index=page2, extra="<p>Listing #1201</p>")
    assign_synthetic_urls([c], INDEX)
    assert c["url"] == INDEX + "#dl-1201" and c["id"] == _sha(INDEX + "#dl-1201")


def test_card_with_detail_link_is_untouched():
    c = _card("Profitable Pizza Restaurant", link="/listing/pizza-123")
    assign_synthetic_urls([c], INDEX)
    assert c["url"] == f"https://{DOMAIN}/listing/pizza-123"
    assert c["url_is_listing_specific"] is True
    assert c["id"] == _sha(c["url"])


def test_same_card_same_url_across_runs_and_order():
    run1 = [_card("Car Wash"), _card("Dry Cleaner", loc="Dallas, TX")]
    run2 = [_card("Dry Cleaner", loc="Dallas, TX"), _card("Car Wash")]
    assign_synthetic_urls(run1, INDEX)
    assign_synthetic_urls(run2, INDEX)
    assert {(c["id"], c["url"]) for c in run1} == {(c["id"], c["url"]) for c in run2}


# ── collisions: logged and left, never price-broken ─────────────────────────
def test_collision_is_left_and_counted(capsys):
    a = _card("Pizza Shop", price="$100,000")
    b = _card("Pizza Shop!", price="$200,000")
    assert a["id"] != b["id"]
    stats = assign_synthetic_urls([a, b], INDEX)
    assert stats["unresolved_collisions"] == 2 and stats["price_tiebreaks"] == 0
    assert a["url"] == b["url"] == INDEX + "#dl-pizza-shop-austin-tx"
    assert "collision" in capsys.readouterr().out


def test_price_change_does_not_change_id_or_url():
    a, b = _card("Pizza Shop", price="$100,000"), _card("Pizza Shop", price="$90,000")
    assign_synthetic_urls([a], INDEX)
    assign_synthetic_urls([b], INDEX)
    assert (a["id"], a["url"]) == (b["id"], b["url"])


# ── off-list broker: today's behaviour ──────────────────────────────────────
def test_off_list_broker_unchanged():
    c = _card("Profitable Pizza Restaurant", index=OFF_INDEX,
              extra="<p>Listing #1088</p>")
    assert c["id"] == _title_id("Profitable Pizza Restaurant", OFF_DOMAIN)
    stats = assign_synthetic_urls([c], OFF_INDEX)
    assert stats["synthetic"] == 0
    assert c["url"] == OFF_INDEX and "#" not in c["url"]
    assert c["id"] == _title_id("Profitable Pizza Restaurant", OFF_DOMAIN)


# ── naz-business.com: ref rule only ─────────────────────────────────────────
def _naz_card(text_html, link=None):
    html = f'<div class="c">{text_html}' + (f'<a href="{link}">Detail</a>' if link else "") + "</div>"
    el = BeautifulSoup(html, "html.parser").select_one("div.c")
    return ListingExtractor.from_card(el, NAZ_INDEX, "NAZ")


def test_naz_ref_card_keyed_on_ref():
    c = _naz_card("<h4>Liquor Store For Sale With Real Estate Waukesha County, WI</h4>"
                  "<p>2286 Asking Price: $ 90,000</p>")
    assign_synthetic_urls([c], NAZ_INDEX)
    assert c["url"] == NAZ_INDEX + "#dl-2286"
    assert c["id"] == _sha(NAZ_INDEX + "#dl-2286")


def test_naz_marketplace_link_never_stored_ref_kept():
    c = _naz_card("<h4>Turnkey Specialty Coffee Shop Business for Sale</h4>"
                  "<p>2273 Asking Price: $ 1,100,000</p>",
                  link="https://www.bizbuysell.com/business-opportunity/x/2412345/")
    assert c is not None
    assign_synthetic_urls([c], NAZ_INDEX)
    assert c["url"] == NAZ_INDEX + "#dl-2273"
    assert "bizbuysell" not in repr(c)


def test_naz_marketplace_link_without_ref_still_dropped():
    c = _naz_card("<h4>Turnkey Specialty Coffee Shop Business for Sale</h4>"
                  "<p>Asking Price: $ 1,100,000</p>",
                  link="https://www.bizbuysell.com/business-opportunity/x/2412345/")
    assert c is None


def test_naz_card_without_ref_keeps_todays_behaviour():
    c = _naz_card("<h4>Turnkey Specialty Coffee Shop Milwaukee County, WI</h4>"
                  "<p>Price $ 75,000</p>")
    assert c["id"] == _title_id(c["title"], "naz-business.com")
    assign_synthetic_urls([c], NAZ_INDEX)
    assert c["url"] == NAZ_INDEX


# ── API writer agrees with the card writer (principle 15) ───────────────────
def test_api_listing_same_rule():
    l = _make_api_listing(INDEX, "Example Broker", "Car Wash", city="Austin", state="TX")
    assert l["id"] == _loc_id("Car Wash", "austin-tx")
    assign_synthetic_urls([l], INDEX)
    assert l["url"] == INDEX + "#dl-car-wash-austin-tx"
    assert l["url_is_listing_specific"] is False

    r = _make_api_listing(INDEX, "Example Broker", "Car Wash",
                          description="Listing #1088 turnkey")
    assign_synthetic_urls([r], INDEX)
    assert r["url"] == INDEX + "#dl-1088" and r["id"] == _sha(INDEX + "#dl-1088")

    d = _make_api_listing(INDEX, "Example Broker", "Car Wash",
                          detail_url=f"https://{DOMAIN}/l/9")
    assign_synthetic_urls([d], INDEX)
    assert d["url"] == f"https://{DOMAIN}/l/9" and d["url_is_listing_specific"] is True

    o = _make_api_listing(OFF_INDEX, "Off", "Car Wash", city="Austin", state="TX")
    assign_synthetic_urls([o], OFF_INDEX)
    assert o["url"] == OFF_INDEX and o["id"] == _title_id("Car Wash", OFF_DOMAIN)
