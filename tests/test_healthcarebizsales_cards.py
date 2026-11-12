"""healthcarebizsales.com cards: CTA button text is never a title, one row per
listing (the <article>, not its two div.col-full halves), dangling " |" from
the page-title template, and the "SOLD!" title marker (2026-11-11). The title
marker applies to healthcarebizsales.com only (owner decision 11), and UNDER
CONTRACT / PENDING mean active with the label kept (decision 6), 2026-11-12."""
import sys
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scrapers"))
from dealledger_scraper_v6 import (  # noqa: E402
    ListingExtractor, best_card_title, card_layout, card_selector_override,
    TITLE_STATUS_MARKER_DOMAINS, domain_title_status_marker, stable_listing_id,
    strip_dangling_sep, title_marker_status, title_status_marker)
from junk_filter import is_cta_text, is_junk_title  # noqa: E402

BASE = "https://healthcarebizsales.com/business/"
NAME = "healthcarebizsales.com"


def _article(heading, slug, price="$650,000", post=3400, cat="medical-practices",
             gross="$609,752"):
    price_span = (f'<span class="price block meta-item"><strong>Price</strong>: {price}</span>'
                  if price else "")
    return f"""
<article class="ep esingle business post-{post} type-business status-publish hentry business-category-{cat}">
 <div class="col-full">
  <h4 class="heading">{heading}</h4>
  <p class="location-block"><span class="meta-item location"><strong>General Location</strong>: Broward</span></p>
  <p>Confidential opportunity to acquire an established regenerative medicine and wellness business.</p>
  <a class="button btn-teal" href="https://www.healthcarebizsales.com/business/{slug}/">More Details</a>
 </div>
 <div class="col-full">
  {price_span}
  <span class="aae block meta-item"><strong>Adjusted Earnings</strong>: $280,390</span>
  <span class="gar block meta-item"><strong>Gross Sales</strong>: {gross}</span>
  <span class="sbtype block meta-item"><strong>Facilty</strong>: No</span>
  <a class="button btn-normal" href="https://www.healthcarebizsales.com/inquiry/?sub={heading.replace(' ', '+')}&amp;refid=b164877790">Inquire Here</a>
 </div>
 <div class="clr"></div>
</article>"""


PAGE = ('<html><body><section class="business-list"><div class="content-indent">'
        + _article("Medical Clinic for Sale in Broward", "medical-clinic-for-sale-in-broward")
        + _article("Medical Practice for Sale in Broward", "medical-practice-for-sale-in-broward",
                   price="$900,000", post=3390, gross="$1,573,018")
        + _article("SOLD! Dental GP and Cosmetic", "sold-dental-gp-and-cosmetic",
                   price="$370,000", post=1800, cat="dental-practices")
        + _article("Sold! pharmacy", "sold-pharmacy", price=None, post=900, cat="pharmaceutical")
        + _article("SOLD-GP/Pediatric MD Practice", "sold-gppediatric-md-practice",
                   price=None, post=139)
        + "</div></section></body></html>")


def _rows():
    soup = BeautifulSoup(PAGE, "html.parser")
    sel = card_selector_override("healthcarebizsales.com")
    return [r for r in (ListingExtractor.from_card(e, BASE, NAME) for e in soup.select(sel)) if r]


# ── CTA rule (general) ────────────────────────────────────────────────────────
@pytest.mark.parametrize("t", ["Inquire Here", "INQUIRE HERE", "Inquire", "Enquire Now",
                               "Learn More", "Learn More »", "Click Here For Details",
                               "Request More Info", "View This Listing", "Contact Us Today"])
def test_cta_text_is_never_a_title(t):
    assert is_cta_text(t)
    assert is_junk_title(t)


@pytest.mark.parametrize("t", [
    "Medical Clinic for Sale in Collier, FL", "Contact Lens Practice for Sale",
    "View Ridge Dental Practice", "Get Fit Gym Franchise", "Learning Center for Sale",
    "Inquire Within Bakery & Cafe", "Call Center Business", "Apply Pest Control",
    "Read's Hardware Store", "See Spot Run Dog Grooming"])
def test_real_titles_are_not_cta(t):
    assert not is_cta_text(t)
    assert not is_junk_title(t)


def test_price_half_alone_never_titled_inquire_here():
    soup = BeautifulSoup(PAGE, "html.parser")
    half = soup.select("div.col-full")[1]          # figures + "Inquire Here"
    assert (best_card_title(half, BASE) or "").lower() != "inquire here"


def test_category_slug_rule_still_fires_but_spares_status_prefix():
    assert is_junk_title("Food/Liquor-Liquor Store")
    assert not is_junk_title("SOLD-GP/Pediatric MD Practice")


# ── one row per listing ───────────────────────────────────────────────────────
def test_override_is_opt_in():
    assert card_selector_override("healthcarebizsales.com") == "article.type-business"
    assert card_selector_override("www.healthcarebizsales.com") == "article.type-business"
    assert card_selector_override("healthcarebizbrokers.com") is None
    assert card_selector_override("") is None


def test_one_row_per_detail_url_with_heading_and_figures():
    rows = _rows()
    assert len(rows) == 5
    urls = [r["url"] for r in rows]
    assert len(set(urls)) == 5 and len({r["id"] for r in rows}) == 5
    assert all("/business/" in u and "/inquiry/" not in u for u in urls)
    r = rows[0]
    assert r["title"] == "Medical Clinic for Sale in Broward"
    assert r["url"] == "https://www.healthcarebizsales.com/business/medical-clinic-for-sale-in-broward/"
    assert (r["asking_price"], r["cash_flow"], r["revenue"]) == (650000.0, 280390.0, 609752.0)
    assert r["status"] == "active" and "broker_status_text" not in r
    assert not any(x["title"].lower().startswith("inquire") for x in rows)


def test_id_keys_on_detail_url_not_title():
    r = _rows()[0]
    assert r["id"] == stable_listing_id("Inquire Here", r["url"], BASE, "healthcarebizsales.com")


# ── labelled figures ─────────────────────────────────────────────────────────
def test_price_is_the_price_field_not_the_largest_figure():
    by = {r["url"].rstrip("/").rsplit("/", 1)[-1]: r for r in _rows()}
    r = by["medical-practice-for-sale-in-broward"]
    assert (r["asking_price"], r["cash_flow"], r["revenue"]) == (900000.0, 280390.0, 1573018.0)


def test_card_without_price_field_has_no_price():
    by = {r["url"].rstrip("/").rsplit("/", 1)[-1]: r for r in _rows()}
    r = by["sold-pharmacy"]                       # Adjusted Earnings + Gross Sales only
    assert r["asking_price"] is None
    assert (r["cash_flow"], r["revenue"]) == (280390.0, 609752.0)


def test_detail_page_does_not_refill_figures_for_layout_domain():
    r = {"title": "Sold! pharmacy", "broker_domain": "healthcarebizsales.com",
         "asking_price": None, "cash_flow": None, "revenue": None}
    html = "<html><body><h1>Sold! pharmacy</h1><p>Gross Sales $2,500,000</p></body></html>"
    out = ListingExtractor.enrich_from_detail(dict(r), html)
    assert out["asking_price"] is None and out["revenue"] is None


def test_detail_page_still_fills_figures_elsewhere():
    r = {"title": "Pizza Shop", "broker_domain": "example-broker.com",
         "asking_price": None, "cash_flow": None, "revenue": None}
    html = "<html><body><h1>Pizza Shop</h1><p>Asking Price: $250,000</p></body></html>"
    assert ListingExtractor.enrich_from_detail(dict(r), html)["asking_price"] == 250000.0


def test_layout_is_opt_in():
    assert card_layout("www.healthcarebizsales.com")["fields"]["asking_price"] == "span.price"
    assert card_layout("example-broker.com") is None


# ── SOLD! marker ──────────────────────────────────────────────────────────────
def test_sold_prefix_marks_sold_and_keeps_marker_verbatim():
    by = {r["url"].rstrip("/").rsplit("/", 1)[-1]: r for r in _rows()}
    s = by["sold-dental-gp-and-cosmetic"]
    assert s["status"] == "sold" and s["broker_status_text"] == "SOLD!"
    assert s["title"] == "SOLD! Dental GP and Cosmetic"           # title untouched
    assert by["sold-pharmacy"]["status"] == "sold"
    assert by["sold-pharmacy"]["broker_status_text"] == "Sold!"
    g = by["sold-gppediatric-md-practice"]
    assert g["title"] == "SOLD-GP/Pediatric MD Practice" and g["status"] == "sold"


@pytest.mark.parametrize("t,want", [
    ("SOLD! Dental Office", "SOLD!"), ("Sold – home healthcare company", "Sold –"),
    ("UNDER CONTRACT: Pharmacy", "UNDER CONTRACT:"), ("Sold…Law firm For Sale", "Sold…"),
    ("Sold With Real Estate Included", None), ("Soldier Field Sports Bar", None),
    ("Pending Approval Medical Lab", None), ("Medical Clinic for Sale", None)])
def test_title_status_marker(t, want):
    assert title_status_marker(t) == want


# ── dangling " |" ─────────────────────────────────────────────────────────────
@pytest.mark.parametrize("t,want", [
    ("Medical Clinic for Sale in Collier, FL |", "Medical Clinic for Sale in Collier, FL"),
    ("Food Truck | Diesel | Pro-Fire |", "Food Truck | Diesel | Pro-Fire"),
    ("High-Traffic Convenience Store | $300K Revenue", "High-Traffic Convenience Store | $300K Revenue"),
    ("Dental Practice - Miami", "Dental Practice - Miami"), ("", "")])
def test_strip_dangling_sep(t, want):
    assert strip_dangling_sep(t) == want


def test_detail_page_title_template_artefact():
    soup = BeautifulSoup("<html><head><title>Medical Clinic for Sale in Collier, FL |</title>"
                         "</head><body><p>x</p></body></html>", "html.parser")
    assert ListingExtractor.best_title_from_detail(soup) == "Medical Clinic for Sale in Collier, FL"


# ── marker allowlist (decision 11) and UNDER CONTRACT = active (decision 6) ───
def _one(heading, slug, base=BASE, name=NAME):
    soup = BeautifulSoup("<html><body>" + _article(heading, slug) + "</body></html>",
                         "html.parser")
    return ListingExtractor.from_card(soup.select_one("article"), base, name)


def test_marker_allowlist_is_healthcarebizsales_only():
    assert TITLE_STATUS_MARKER_DOMAINS == {"healthcarebizsales.com"}
    assert domain_title_status_marker("healthcarebizsales.com", "SOLD! X") == "SOLD!"
    assert domain_title_status_marker("www.healthcarebizsales.com", "SOLD! X") == "SOLD!"
    assert domain_title_status_marker("www.WeSellRestaurants.com", "SOLD! X") is None
    assert domain_title_status_marker("", "UNDER CONTRACT: X") is None


@pytest.mark.parametrize("marker,want", [
    ("SOLD!", "sold"), ("Sold –", "sold"), ("SOLD", "sold"), ("Sold…", "sold"),
    ("UNDER CONTRACT:", "active"), ("Under Contract -", "active"),
    ("PENDING -", "active"), ("SALE PENDING", "active")])
def test_marker_status_mapping(marker, want):
    assert title_marker_status(marker) == want


def test_under_contract_marker_is_active_with_verbatim_label():
    r = _one("UNDER CONTRACT: Pharmacy in Tampa", "under-contract-pharmacy-in-tampa")
    assert r["status"] == "active" and r["broker_status_text"] == "UNDER CONTRACT:"
    assert r["title"] == "UNDER CONTRACT: Pharmacy in Tampa"
    r = _one("PENDING - Dental Office in Naples", "pending-dental-office-in-naples")
    assert r["status"] == "active" and r["broker_status_text"] == "PENDING -"


OTHER = "https://www.example-broker.com/listings/"


def test_marker_rule_off_for_other_brokers():
    # Same cards at a non-allowlisted broker: no broker_status_text, and the
    # status is whatever the generic rules gave before this patch.
    r = _one("Sold! pharmacy", "sold-pharmacy", OTHER, "example-broker.com")
    assert r["status"] == "active" and "broker_status_text" not in r
    # Generic path since j3-v6_generic_status (decision 6): all-caps SOLD is
    # still sold (now with the broker's word kept); UNDER CONTRACT is active
    # with its label. The any-case "Sold!" rule above stays off (decision 11).
    r = _one("SOLD! Dental GP and Cosmetic", "sold-dental", OTHER, "example-broker.com")
    assert r["status"] == "sold" and r.get("broker_status_text") in (None, "SOLD")
    r = _one("UNDER CONTRACT: Pharmacy in Tampa", "uc-pharmacy", OTHER, "example-broker.com")
    assert r["status"] == "active" and r.get("broker_status_text") in (None, "UNDER CONTRACT")
