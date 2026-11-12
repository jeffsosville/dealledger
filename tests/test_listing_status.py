"""Shared broker-status policy (scrapers/listing_status.py, job 3, decisions 5–7)."""
import pytest

from listing_status import (
    ACTIVE_PHRASES, COMING_SOON_PHRASES, DROP, INACTIVE_PHRASES, OUT_OF_SCOPE_PHRASES,
    SOLD_PHRASES, STATUS_RANK, UNDER_CONTRACT_PHRASES, apply_status, classify_class_term,
    classify_slug, classify_status, drop_summary, is_drop, lookup_status, more_final,
    normalize_status_text)


# ── every phrase in the constant set ────────────────────────────────────────
@pytest.mark.parametrize("phrase", sorted(SOLD_PHRASES))
def test_every_sold_phrase(phrase):
    assert classify_status(phrase) == ("sold", phrase)


@pytest.mark.parametrize("phrase", sorted(UNDER_CONTRACT_PHRASES))
def test_every_under_contract_phrase_is_active_with_label(phrase):
    assert classify_status(phrase) == ("active", phrase)


@pytest.mark.parametrize("phrase", sorted(COMING_SOON_PHRASES | OUT_OF_SCOPE_PHRASES))
def test_every_drop_phrase(phrase):
    assert classify_status(phrase) == (DROP, phrase)


@pytest.mark.parametrize("phrase", sorted(INACTIVE_PHRASES))
def test_every_inactive_phrase(phrase):
    assert classify_status(phrase) == ("inactive", phrase)


@pytest.mark.parametrize("phrase", sorted(ACTIVE_PHRASES))
def test_every_plain_active_phrase_has_no_label(phrase):
    assert classify_status(phrase) == ("active", None)


def test_classes_do_not_overlap():
    sets = [SOLD_PHRASES, UNDER_CONTRACT_PHRASES, COMING_SOON_PHRASES,
            OUT_OF_SCOPE_PHRASES, INACTIVE_PHRASES, ACTIVE_PHRASES]
    for i, a in enumerate(sets):
        for b in sets[i + 1:]:
            assert not (a & b), a & b


# ── case / punctuation variants; label is verbatim ──────────────────────────
@pytest.mark.parametrize("text,status", [
    ("SOLD", "sold"), ("Sold", "sold"), ("sold", "sold"), ("Sold!", "sold"), ("SOLD!", "sold"),
    ("(Sold)", "sold"), ("Sold:", "sold"), ("  SOLD  ", "sold"), ("SOLD FOR", "sold"),
    ("Sold For $1,250,000", "sold"), ("SOLD FOR $1.2M", "sold"), ("Recently-Sold", "sold"),
    ("Under Contract", "active"), ("UNDER CONTRACT", "active"), ("under-contract", "active"),
    ("Under_Contract", "active"), ("Under Contract!", "active"), ("(Under Contract)", "active"),
    ("Pending", "active"), ("PENDING", "active"), ("Sale Pending", "active"),
    ("sale-pending", "active"), ("Under LOI", "active"), ("Has Accepted Offer", "active"),
    ("Accepted Offer", "active"), ("Off Market", "active"), ("Off-Market", "active"),
    ("off_market", "active"), ("OFF-MARKET", "active"),
    ("Coming Soon", DROP), ("Coming Soon!", DROP), ("COMING SOON", DROP), ("coming-soon", DROP),
    ("For Lease", DROP), ("FOR LEASE", DROP), ("for-lease", DROP),
    ("Not Available", "inactive"), ("NOT AVAILABLE", "inactive"), ("Withdrawn", "inactive"),
    ("Expired", "inactive"), ("expired", "inactive"),
])
def test_variants(text, status):
    st, label = classify_status(text)
    assert st == status
    assert label == text.strip()


def test_label_whitespace_collapsed_but_otherwise_verbatim():
    assert classify_status("  Under \n Contract!\xa0") == ("active", "Under Contract!")


# ── unknown -> active, no label ─────────────────────────────────────────────
@pytest.mark.parametrize("text", [
    None, "", "   ", "Price Reduced!!! Call Now", "Patent pending technology",
    "Sold separately", "Great opportunity", "Contact us", True, 0,
    "Under contract with a national franchisor since 2004 and growing every year",
])
def test_unknown_is_active_without_label(text):
    assert classify_status(text) == ("active", None)
    assert lookup_status(text) is None


def test_free_text_is_not_searched():
    # a description mentioning a status is not a status label
    assert lookup_status("This business has never been sold before") is None
    assert lookup_status("The building is for lease next door") is None


# ── per-domain override (decision 7) ───────────────────────────────────────
@pytest.mark.parametrize("domain", ["reputedbrokerage.com", "www.reputedbrokerage.com",
                                    "https://www.reputedbrokerage.com/listings/"])
def test_reputed_expired_is_sold(domain):
    assert classify_status("Expired", domain) == ("sold", "Expired")
    assert classify_status("Sold", domain) == ("sold", "Sold")
    assert classify_status("For Sale", domain) == ("active", None)


def test_override_is_per_domain():
    assert classify_status("Expired", "otherbroker.com") == ("inactive", "Expired")


# ── WordPress taxonomy terms and slugs ──────────────────────────────────────
@pytest.mark.parametrize("term,status", [
    ("status-sold", "sold"), ("availability-sold", "sold"), ("listing_status-past", "sold"),
    ("property_status-for-sale", "active"), ("listing-status-under-contract", "active"),
    ("status-coming-soon", DROP), ("property-status-for-lease", DROP),
    ("listing_status-withdrawn", "inactive"), ("sold", "sold"), ("Sold", "sold"),
])
def test_class_terms(term, status):
    assert classify_class_term(term)[0] == status


@pytest.mark.parametrize("term", ["status-publish", "type-listing", "post-123", "hentry", "", None,
                                  "category-restaurants"])
def test_class_terms_without_status(term):
    assert classify_class_term(term) is None


def test_class_term_active_plain_has_no_label_others_keep_term():
    assert classify_class_term("property_status-for-sale") == ("active", None)
    assert classify_class_term("status-under-contract") == ("active", "status-under-contract")


def test_past_only_counts_as_taxonomy():
    assert classify_class_term("listing_status-past") == ("sold", "listing_status-past")
    assert lookup_status("past") is None


@pytest.mark.parametrize("slug,hit", [
    ("sold-hvac-company-1234", True), ("hvac-company-sold", True), ("hvac-company-sold-2", True),
    ("SOLD_bakery", True), ("soldier-field-cafe", False), ("resold-goods-store", False),
    ("hvac-company", False), ("", False), (None, False),
])
def test_slug(slug, hit):
    assert (classify_slug(slug) is not None) is hit
    if hit:
        assert classify_slug(slug)[0] == "sold"


# ── apply_status ────────────────────────────────────────────────────────────
def test_apply_status_under_contract_keeps_active_and_label():
    row = {"title": "Pizza", "status": "active"}
    out = apply_status(row, "Under Contract")
    assert out == {"title": "Pizza", "status": "active", "broker_status_text": "Under Contract"}
    assert row == {"title": "Pizza", "status": "active"}          # pure: input untouched


def test_apply_status_sold():
    assert apply_status({}, "SOLD!")["status"] == "sold"
    assert apply_status({}, "SOLD!")["broker_status_text"] == "SOLD!"


@pytest.mark.parametrize("text", ["Coming Soon!", "For Lease"])
def test_apply_status_drop(text):
    assert apply_status({"status": "active"}, text) is DROP


def test_apply_status_unknown_unchanged():
    row = {"status": "sold", "x": 1}
    assert apply_status(row, "Great opportunity") == row
    assert apply_status({}, None) == {"status": "active"}


def test_apply_status_does_not_reopen_more_final():
    out = apply_status({"status": "sold", "broker_status_text": "SOLD"}, "Pending")
    assert out["status"] == "sold" and out["broker_status_text"] == "SOLD"
    out = apply_status({"status": "inactive"}, "Under Contract")
    assert out["status"] == "inactive" and "broker_status_text" not in out


def test_apply_status_uses_row_domain_for_override():
    assert apply_status({"broker_domain": "reputedbrokerage.com"}, "Expired")["status"] == "sold"
    assert apply_status({"broker_domain": "x.com"}, "Expired")["status"] == "inactive"


def test_rank_and_helpers():
    assert STATUS_RANK["sold"] > STATUS_RANK[DROP] > STATUS_RANK["inactive"] > STATUS_RANK["active"]
    assert more_final("active", "sold") == "sold"
    assert more_final(DROP, "inactive") == DROP
    assert is_drop(DROP) and not is_drop("sold")
    assert normalize_status_text("(Off-Market)!") == "off market"
    assert drop_summary(["Coming Soon!", "Coming Soon!", "For Lease"]) == \
        "3 dropped (Coming Soon!: 2, For Lease: 1)"
    assert drop_summary([]) == "0 dropped"


def test_constants_are_frozen():
    with pytest.raises(AttributeError):
        SOLD_PHRASES.add("x")
    with pytest.raises(TypeError):
        STATUS_RANK["x"] = 9


# ── FacetWP cards (eatz / synergy) route through the shared set ───────────
def _card(html):
    from bs4 import BeautifulSoup
    return BeautifulSoup(html, "html.parser").div


def _facet():
    try:
        from specialized_scrapers import facet_card_status
    except Exception:                       # selenium etc. missing in a bare env
        pytest.skip("specialized_scrapers not importable")
    return facet_card_status


@pytest.mark.parametrize("html,title,want", [
    ('<div><a>Pizza Place</a><span class="b">Under Contract</span><p>Asking Price: $1</p></div>',
     "Pizza Place", ("active", "Under Contract")),
    ('<div><a>Pizza Place</a><span>Coming Soon!</span></div>', "Pizza Place", (DROP, "Coming Soon!")),
    ('<div><a>Pizza Place</a><span>For Lease</span></div>', "Pizza Place", (DROP, "For Lease")),
    ('<div><a>Pizza Place</a><span>Sold!</span></div>', "Pizza Place", ("sold", "Sold!")),
    ('<div><a>Diner: Under Contract</a></div>', "Diner: Under Contract", ("active", "Under Contract")),
    ('<div><a>Diner - SOLD</a></div>', "Diner - SOLD", ("sold", None)),
    ('<div><a>Pizza Place</a><p>Great location. Asking Price: $250,000</p></div>',
     "Pizza Place", ("active", None)),
    ('<div><a>Pizza Place</a><span>Under Contract</span><p>SOLD to the first buyer</p></div>',
     "Pizza Place", ("sold", "SOLD")),
    ('<div><a>Pizza Place</a><p>Patent pending recipes</p></div>', "Pizza Place", ("active", None)),
])
def test_facet_card_status(html, title, want):
    assert _facet()(title, _card(html)) == want
