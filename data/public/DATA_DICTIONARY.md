# DealLedger public listings: data dictionary

**What this is:** every active, published US business-for-sale listing DealLedger has collected from business brokers' own websites. One row per listing (copies of the same listing on a broker's site are merged). Rebuilt daily.

**License:** CC0 1.0. No rights reserved, no attribution required (a citation is appreciated: see `CITATION.cff`).

## Download

| File | URL |
|---|---|
| CSV | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.csv |
| JSON | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/listings.json |
| Row count, coverage, build time | https://raw.githubusercontent.com/jeffsosville/dealledger/main/data/public/meta.json |

Earlier days are in this file's git history.

## Fields

| Field | Type | Meaning |
|---|---|---|
| `id` | text | Stable DealLedger listing ID |
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
| `first_seen` | date | First day a DealLedger crawl saw the listing. **Not necessarily the date it was listed:** read it with `listed_on_basis` |
| `last_seen` | date | Latest day a crawl saw the listing live |
| `listed_on_basis` | text | `observed`: a complete crawl of the broker shortly before `first_seen` did not see it, so it was listed between `listed_after` and `first_seen`. `floor`: it was listed on or before `first_seen` (for example, it was already up when we first crawled the broker). Counts are in `meta.json` |
| `listed_after` | date | For `observed` rows, the last complete crawl that did not see the listing. Empty for `floor` rows |

## What's included and excluded

- **Not included:** broker-written descriptions and photos. They belong to the brokers; `url` links to the original page.
- **Included:** listings on brokers' own websites that are currently active and pass our listing checks.
- **Excluded:** real estate and commercial property, for-sale-by-owner, aggregator and marketplace reposts, and pages flagged for review. Listings that are no longer on the broker's site are in `listings_left_observation.csv` (below), not this file.
- **Not exported yet:** days on market. Listing dates are hard to observe directly, and we won't publish a figure until the method is settled.

## Known gaps

Coverage per field is in `meta.json`. Revenue and city are the thinnest. Fixes are tracked as GitHub issues, and corrections are welcome.

Methodology: [`docs/METHODOLOGY.md`](../../docs/METHODOLOGY.md). Bad row? Open an issue or email info@dealledger.org.

## listings_left_observation.csv

Listings that were on a broker's own website and then **left observation**: they were absent from three consecutive complete crawls of that broker's site (a complete crawl succeeded and saw at least 80% of the broker's usual listing count). This file is how the record keeps what happened to a listing after it stopped being advertised. Leaving observation is **not** evidence that a business sold; a broker may withdraw, pause or relist it. If a listing reappears, it returns to `listings.csv` and drops out of this file.

| Field | Meaning |
|---|---|
| id … url | Same as `listings.csv` |
| first_seen | First day a DealLedger crawl saw the listing |
| last_seen | Last day a DealLedger crawl saw the listing |
| left_observation_on | Day the listing was recorded as having left observation |
| left_observation_reason | Why (currently always: absent from 3 complete crawls of the broker's site) |
