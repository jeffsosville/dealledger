"""Source check: the public listing page carries no marketplace data path and makes no
claims the data can't support. Run: python3 tests/test_listing_page_source.py (no network)."""
import os
import re

PAGE = os.path.join(os.path.dirname(__file__), "..", "pages", "listing", "[id].tsx")

# Each pattern is something the page must never contain again, with the reason.
FORBIDDEN = {
    r"listing_history": "marketplace snapshot table",
    r"broker_master": "marketplace broker profiles",
    r"bbs_account_id|bizbuysell|bizquest|sold_listings|soldListingsCount": "marketplace fields",
    r"schema\.org/SoldOut": "asserts a sale (DealLedger never asserts sold)",
    r"\bdateModified\s*:": "no reliable modification timestamp yet",
    r"Times Listed": "removed: not backed by data",
    r"label:\s*'Today'": "invented price-change date",
    r"Reduced \$\{": "price reduction badge with no dated history",
    r"PriceChart|PRICE HISTORY": "price history is not tracked",
}

METHODOLOGY = (
    "DealLedger displays the latest recorded asking price from the broker&rsquo;s listing. "
    "Historical price changes are not currently tracked."
)


def _source():
    with open(PAGE, encoding="utf-8") as f:
        return f.read()


def test_no_forbidden_patterns():
    src = _source()
    hits = [f"{pat}  ({why})" for pat, why in FORBIDDEN.items() if re.search(pat, src)]
    assert not hits, "listing page contains:\n  " + "\n  ".join(hits)


def test_methodology_copy():
    flat = re.sub(r"\s+", " ", _source())
    assert METHODOLOGY in flat, "methodology copy changed or missing"


def test_registry_and_mv_kept():
    src = _source()
    # The listings registry, its URLs and the public-record view stay in place.
    assert ".from('listings')" in src
    assert ".from('mv_listings_page')" in src
    assert ".from('broker_directory')" in src


if __name__ == "__main__":
    test_no_forbidden_patterns()
    test_methodology_copy()
    test_registry_and_mv_kept()
    print("listing page source: ok")
