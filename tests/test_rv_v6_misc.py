"""rv-v6-misc (2026-11-24): cache-buster identity (pacificrb) and the residential gate (tbxllc).

Principle 8: every narrowing is paired with input that must still behave as before.
"""
import hashlib
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))

import dealledger_scraper_v6 as v6  # noqa: E402

SITE = "9a18c007-ed10-432c-bb65-741014c2327a"
FILE = "b9738069-0bab-46d9-b92e-8d3d55cae71a"


def wsimg(ver=None, name="SAN%20DIEGO%20GENERATIVE1.pdf"):
    q = f"?ver={ver}" if ver else ""
    return f"https://img1.wsimg.com/blobby/go/{SITE}/downloads/{FILE}/{name}{q}"


def sha(s):
    return hashlib.sha256(s.encode()).hexdigest()[:16]


# ---- identity -------------------------------------------------------------

def test_wsimg_ver_changes_do_not_change_the_id():
    a = v6.stable_listing_id("t", wsimg(1790810534917), "https://pacificrb.com/listings", "pacificrb.com")
    b = v6.stable_listing_id("t", wsimg(1794448938781), "https://pacificrb.com/listings", "pacificrb.com")
    assert a == b
    assert v6.listing_id_key(wsimg(1794448938781)) == f"wsimg-file:{FILE}"


def test_wsimg_without_file_id_segment_drops_ver_only():
    u = "https://img1.wsimg.com/blobby/go/896e005f-631a-4284-adea-3fdf9fc8ef8d/downloads/Ontario%20real%20estate%202.1.pdf"
    k1 = v6.listing_id_key(u + "?ver=1782358625570")
    k2 = v6.listing_id_key(u + "?ver=1793145629363")
    assert k1 == k2 == u


def test_buster_params_stripped_only_when_long_numbers():
    base = "https://example.com/listing/abc"
    for p in ("ver", "v", "t", "ts", "cb", "_"):
        assert v6.listing_id_key(f"{base}?{p}=1794448938781") == base
    # must still pass: short values, non-numeric values, other params are identity
    assert v6.listing_id_key(f"{base}?ver=12345") == f"{base}?ver=12345"        # 5 digits
    assert v6.listing_id_key(f"{base}?v=abc123456") == f"{base}?v=abc123456"
    assert v6.listing_id_key(f"{base}?id=1794448938781") == f"{base}?id=1794448938781"
    assert v6.listing_id_key(f"{base}?id=77&ts=1794448938781") == f"{base}?id=77"


def test_urls_without_buster_params_keep_their_ids():
    """The ids of every normal url must not move (twins otherwise)."""
    urls = [
        "https://tbxllc.com/listing/south-shore-country-store-w-real-estate-and-apartment/",
        "https://ctbusinessforsale.com/our-listings?listingId=cmo27naxr000sic04joqcaqy4",
        "https://briobrokers.dealrelations.com/listings/x?item_id=5092&from=listing_feeds#top_registration_form",
        "https://img1.wsimg.com/blobby/go/s/downloads/f/n.pdf",          # wsimg, no buster -> unchanged
        "https://example.com/p?page=2",
    ]
    for u in urls:
        assert v6.listing_id_key(u) == u
        assert v6.stable_listing_id("t", u, "https://x/index", "x") == sha(u)


def test_existing_id_rules_still_win():
    u = "https://restaurantrealty.com/listing/?listing=1234-some-title&ver=1794448938781"
    assert v6.listing_id_key(u) == "https://restaurantrealty.com/listing/?listing=1234"


def test_snapshot_pacificrb_hashes_one_identity_per_file():
    # two stored rows of the same file (snapshot ids 66c136075e9d0c0f / 3e6f37c702df1c8c)
    old = wsimg(1790810534917)
    new = wsimg(1794448938781)
    assert sha(old) != sha(new)                                    # the old behaviour: two ids
    assert v6.stable_listing_id("t", old, "i", "d") == v6.stable_listing_id("t", new, "i", "d")


# ---- residential gate -----------------------------------------------------

def test_restaurant_with_house_passes():
    t = "Established Free Standing Restaurant w/ 3 Bedroom home"
    assert not v6.looks_residential(t, t + " $1,950,000 Southeastern MA")


def test_store_hotel_motel_market_with_home_pass():
    for t in ("South Shore Country Store w/ Real Estate and 2 Bedroom Apartment",
              "Boutique Hotel with 4 bedrooms owner residence",
              "Roadside Motel plus 3 Bedroom house",
              "Neighborhood Market and 2 bedroom apartment above"):
        assert not v6.looks_residential(t, t), t


def test_residential_only_still_rejected():
    for t, body in (("Charming 3 Bedroom home", "Charming 3 Bedroom home 2 full baths"),
                    ("Lovely cottage near the Inn", "Lovely cottage near the Inn, 3 bedrooms, 2 full baths"),
                    ("Colonial on quiet street", "Colonial on quiet street 4 bedrooms")):
        assert v6.looks_residential(t, body), t
    # MLS, bd/ba titles and street-address titles reject even with a business word in the title
    assert v6.looks_residential("Restaurant building 3 bd 2 ba", "x")
    assert v6.looks_residential("Restaurant for sale", "Restaurant MLS # 1234567 with 3 bedrooms")
    assert v6.looks_residential("Boston, MA - 123 Main Street", "restaurant 3 bedrooms")


def test_plain_business_cards_untouched():
    assert not v6.looks_residential("Fairfield County Pizza Restaurant", "Fairfield County Pizza Restaurant $275,000")
    assert not v6.looks_residential("HVAC Contractor", "")
