# Wanted Brokers

Brokers we want to add to DealLedger, prioritized by coverage and data quality.

Statuses checked against `listings_direct` / `broker_sources` on 24 Sep 2026.

**Want to contribute?** Pick one from this list and follow the [adding a broker guide](adding-a-broker.md).

---

## Tier 1: National Networks (High Priority)

These are large franchise networks with the most listings.

| Broker | Website | Est. Listings | Status |
|--------|---------|---------------|--------|
| Transworld Business Advisors | tworld.com | 2,000+ | ✅ Covered (specialized) |
| Sunbelt Business Brokers | sunbeltnetwork.com | 1,500+ | ✅ Covered (specialized) |
| Murphy Business | murphybusiness.com | 1,000+ | ✅ Covered (specialized) |
| VR Business Brokers | vrbusinessbrokers.com | 800+ | ⚠️ Scraper exists, returning 0 listings |
| First Choice Business Brokers | fcbb.com | 600+ | ✅ Covered (specialized) |
| The Business Exchange | businessexchange.com | 500+ | ⬜ Needed |
| Synergy Business Brokers | synergybb.com | 400+ | ✅ Covered |
| Viking Mergers | vikingmergers.com | 300+ | ✅ Covered |

---

## Tier 2: Regional Leaders

Strong coverage in specific states/regions.

| Broker | Website | Region | Status |
|--------|---------|--------|--------|
| Calder Associates | calderassociates.com | NJ/NY | 🟡 Listings page found, not yet crawlable |
| Soldsmart Business Brokers | soldsmart.com | TX | ⬜ Needed |
| Business Team | businessteam.com | CA | ⬜ Needed |
| Hartland Business Brokers | hartlandbusinessbrokers.com | Midwest | ⬜ Needed |
| DMC Real Estate | dmcrealestate.com | FL | ⬜ Needed |

---

## Tier 3: Vertical Specialists

Brokers focused on specific industries (cleaning, laundromats, etc.)

| Broker | Website | Vertical | Status |
|--------|---------|----------|--------|
| Laundry Owners Warehouse | lowbrokers.com | Laundromat | ⬜ Needed |
| Coin Laundry Association | coinlaundry.org/listings | Laundromat | ⬜ Needed |
| PWS Laundry | pwslaundry.com | Laundromat | ⬜ Needed |
| Route Consultant | listings.routeconsultant.com | Vending | ✅ Covered |
| Restaurant Realty | restaurantrealty.com | Restaurant | ✅ Covered |

---

## Tier 4: Additional Coverage

Smaller brokers that add geographic or vertical coverage.

*This list will grow as we identify more brokers.*

---

## How to Claim a Broker

1. Comment on the tracking issue or open a new one
2. Update this file in your PR to show "🟡 In progress"
3. Submit your scraper PR
4. We'll update to "✅ Complete" when merged

---

## Not Sure Where to Start?

Pick any **Tier 1** broker — they have the most listings and the highest impact.

If you're new to scraping, **Sunbelt** tends to have clean, consistent HTML.

---

## Broker Doesn't Qualify?

A broker should be excluded if:

- Requires login to view listings
- Fewer than 5 active listings
- Aggregator (not the original source)
- No longer operating

If you're unsure, open an issue and we'll check.
