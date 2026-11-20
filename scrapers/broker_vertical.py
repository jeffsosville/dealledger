"""
broker_vertical.py — default vertical for single-vertical brokers.

WHY (T1 2026-11-20, proposed)
A broker that only sells car washes or only sells gas stations still had its
listings filed by keyword counting over the description: carwashtraders.com
had car washes as automotive / technology / hvac / vending / restaurant,
petroleumrealty.net had gas stations as technology / restaurant, staxre.com
had them as healthcare / technology. The site's type pages
(mv_seo_listings) only rescue a car wash whose TITLE says "car wash"
(dl_vertical_from_title, sql/2026-11-02_seo_title_verticals.sql), so
"Wildcatter Wash | Dilley" or "#2394 Palm Beach County" never reach the car
wash / gas station pages.

THE RULE (one function, called by both writers - principle 15)
  broker_default_vertical(domain, title) returns the broker's default
  vertical, or None (= leave the scraper's vertical alone) when
    - the domain is not in BROKER_DEFAULT_VERTICAL, or
    - the TITLE says otherwise: title_vertical(title) - the same ordered
      rules as dl_vertical_from_title() in the database - names a different
      vertical ("Car Wash & Laundry" at a gas broker -> car_wash wins;
      "Truck & Pet Wash" -> pet, so the default steps aside).
  Title only, never the description (principle 5).

Slugs are the ones the site already uses: 'car_wash' and 'gas_station'
(mv_seo_listings / dl_vertical_from_title). mv_seo_listings passes any
non-'other' listings_direct.vertical through unchanged, so a row written
with 'car_wash' lands on the car wash pages without a SQL change.

Adding a broker: only one whose own listings page is car washes only / gas
stations only, checked by a person. A mixed broker (reputedbrokerage.com,
autocenter-sales.com) does not belong here.
"""

import re
from typing import Optional

# bare host -> vertical slug. Evidence for each in claude/staging/2026-11-20/t1-notes.md.
BROKER_DEFAULT_VERTICAL = {
    "carwashtraders.com": "car_wash",     # "Washes For Sale", 112 cards, all car washes (+1 lot, 3 leases not emitted as such)
    "mycarwashbroker.com": "car_wash",    # Ross Brothers Car Wash Brokers, NY car wash listings page
    "carwashbrokers.com": "car_wash",     # Car Wash Brokers, Inc.
    "petroleumrealty.net": "gas_station", # 5 gas stations + 1 development parcel (not emitted), 2026-11-20
    "gasstationsusa.com": "gas_station",  # "Selling gas stations in Florida since 1980"
    "buygasstations.com": "gas_station",  # "Gas Station Specialist"; non-gas blocks not emitted
    "thegasbroker.com": "gas_station",    # "Explore Active Gas Stations For Sale", 7 of 7
    "staxre.com": "gas_station",          # "Gas Stations for Sale in Florida", 24 of 24 fuel / c-store
}

# Ordered; first match wins. Mirrors public.dl_vertical_from_title() (2026-11-02)
# so the scraper and the site agree on what a title "says".
_TITLE_RULES = [
    ("car_wash", r"car ?wash"),
    ("atm", r"\batm (route|portfolio|business)|\batms\b"),
    ("pest_control", r"pest control|extermina|termite|mosquito control"),
    ("laundromat", r"laundromat|coin laundry|wash ?(&|and) ?fold"),
    ("vending", r"\bvending|micro.?market"),
    ("cleaning", r"janitorial|commercial cleaning|cleaning (service|company|business|franchise)|"
                 r"maid service|carpet clean|window clean"),
    ("pet", r"\bpets?\b|grooming|kennel|veterinar|animal hospital|dog (daycare|boarding|training)"),
    ("healthcare", r"adult day|home (care|health)|senior care|assisted living"),
    ("childcare", r"day ?care|child ?care|preschool|early learning|montessori|learning center"),
    ("salon_spa", r"\b(hair )?salon|barber|\bspa\b|med ?spa|medical spa|\bnail|\blash|waxing|massage|tanning"),
    ("gas_station", r"gas station|c-store|convenience store|fuel station|truck stop"),
    ("liquor_store", r"\bliquor|wine (shop|store)|package store|bottle shop|beer (store|distributor)"),
    ("accounting", r"\bcpa\b|accounting|bookkeeping|\btax (prep|practice|preparation|service|resolution)"),
    ("fitness", r"\bgym\b|fitness|yoga|pilates|crossfit|martial arts|\bdojo\b|boxing studio"),
    ("sign_print", r"\bsign (shop|company|business)|\bsigns\b|\bprinting|print shop|screen ?print|embroidery"),
    ("trucking", r"trucking|freight|\bltl\b|logistics"),
    ("hvac", r"\bhvac\b|heating (and|&) (air|cooling)|air conditioning"),
    ("plumbing", r"plumbing|plumber"),
    ("landscaping", r"landscap|lawn (care|service)|tree service"),
    ("roofing", r"roofing"),
    ("pool_service", r"pool (service|route|cleaning)"),
    ("electrical", r"electrical contractor|electrician"),
    ("technology", r"e-?commerce|amazon|\bfba\b|shopify|\bsaas\b"),
]
_COMPILED = [(v, re.compile(p, re.I)) for v, p in _TITLE_RULES]


def _bare(domain: Optional[str]) -> str:
    d = (domain or "").strip().lower()
    if "://" in d:
        d = d.split("://", 1)[1]
    d = d.split("/", 1)[0]
    return d[4:] if d.startswith("www.") else d


def title_vertical(title: Optional[str]) -> Optional[str]:
    """The vertical a title names on its own, or None (dl_vertical_from_title)."""
    if not title:
        return None
    for v, rx in _COMPILED:
        if rx.search(title):
            return v
    return None


def broker_default_vertical(domain: Optional[str], title: Optional[str]) -> Optional[str]:
    """The broker's default vertical for this row, or None to keep the
    scraper's own vertical (broker not single-vertical, or the title names a
    different vertical)."""
    default = BROKER_DEFAULT_VERTICAL.get(_bare(domain))
    if not default:
        return None
    named = title_vertical(title)
    if named and named != default:
        return None
    return default
