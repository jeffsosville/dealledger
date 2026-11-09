# DealLedger public listings: data dictionary

**What this is:** every active, published US business-for-sale listing DealLedger has collected from business brokers' own websites. One row per listing (copies of the same listing on a broker's site are merged). Rebuilt daily and published only after an automated check against the database passes (`verification.json`).

**Schema version 2** (2026-11-10): adds `listing_number`, `dealledger_url` and the listing-age evidence fields used on dealledger.org (`age_basis`, `age_date`, `listed_after`); removes `first_seen` and `listed_on_basis`.

**License:** CC0 1.0. No rights reserved, no attribution required (a citation is appreciated: see `CITATION.cff`).

## Download

| File | URL |
|---|---|
| CSV | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.csv |
| JSON | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.json |
| Listings that left observation | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings_left_observation.csv |
| Row count, coverage, build time | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/meta.json |
| Checks this release passed | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/verification.json |

Earlier days are in this file's git history.

## Fields

| Field | Type | Meaning |
|---|---|---|
| `listing_number` | integer | DealLedger's public listing number; the listing's page is `dealledger_url` |
| `dealledger_url` | text | `https://dealledger.org/listing/{listing_number}`: the record, its evidence and any published price changes |
| `id` | text | DealLedger's internal row ID |
| `title` | text | Listing title as the broker wrote it |
| `asking_price` | number, USD | Asking price as published |
| `cash_flow` | number, USD | Seller's discretionary earnings / cash flow as published. Brokers define this differently; treat it as the broker's figure, not an audited one |
| `revenue` | number, USD | Gross revenue / sales as published. Some large franchise brokers never publish revenue, so it is blank for all of their listings |
| `city` | text | City, when the broker gives one. Many brokers hide the exact location |
| `state` | text | Two-letter US state |
| `vertical` | text | Industry, assigned by DealLedger from the title (rules, then a language model). `other` means none of our categories fit |
| `broker_name` | text | Brokerage name |
| `broker_domain` | text | The brokerage website the listing came from |
| `url` | text | The listing's page on the broker's site. Always the original source |
| `age_basis` | text | The strongest evidence we have for when the listing appeared, the same as dealledger.org shows. `window`: complete crawls of the broker's site show it appeared between `listed_after` and `age_date`. `observed`: `age_date` is the first day a DealLedger crawl saw it; it was listed on or before then. `in_db`: `age_date` is the day the record was first written to our database; it was listed on or before then. Counts are in `meta.json` |
| `age_date` | date | See `age_basis`. For `observed` and `in_db` it is a minimum: the true listing date may be earlier |
| `listed_after` | date | For `window` rows only: the last complete crawl of the broker's site that did not show the listing (at most 45 days before `age_date`) |
| `last_seen` | date | Latest day a crawl saw the listing live |

## What's included and excluded

- **Not included:** broker-written descriptions and photos. They belong to the brokers; `url` links to the original page.
- **Included:** listings on brokers' own websites that are currently active and pass our listing checks.
- **Excluded:** real estate and commercial property, for-sale-by-owner, aggregator and marketplace reposts, and pages flagged for review. Listings that are no longer on the broker's site are in `listings_left_observation.csv` (below), not this file.
- **Not exported:** days on market. `age_basis`/`age_date` say what we can prove about when a listing appeared; they are not a days-on-market figure.
- **Checked before every release:** each row is active, published and broker-direct in our database at export time; no URL is on a marketplace, aggregator, digital-business platform, real-estate portal or broker MLS; listing-age fields equal what dealledger.org shows. The results are in `verification.json`; if any check fails, the previous day's files stay in place.

## Known gaps

Coverage per field is in `meta.json`. Revenue and city are the thinnest. Fixes are tracked as GitHub issues, and corrections are welcome.

Methodology: [`docs/METHODOLOGY.md`](../../docs/METHODOLOGY.md). Bad row? Open an issue or email info@dealledger.org.

## listings_left_observation.csv

Listings that were on a broker's own website and then **left observation**: they were absent from three consecutive complete crawls of that broker's site (a complete crawl succeeded and saw at least 80% of the broker's usual listing count). This file is how the record keeps what happened to a listing after it stopped being advertised. Leaving observation is **not** evidence that a business sold; a broker may withdraw, pause or relist it. If a listing reappears, it returns to `listings.csv` and drops out of this file.

| Field | Meaning |
|---|---|
| listing_number … url | Same as `listings.csv` |
| age_basis, age_date, listed_after | Same as `listings.csv`, for when the listing appeared |
| last_seen | Last day a DealLedger crawl saw the listing |
| left_observation_on | Day the listing was recorded as having left observation |
| left_observation_reason | Why (currently always: absent from 3 complete crawls of the broker's site) |
