# DealLedger public listings: data dictionary

**What this is:** every active, published US business-for-sale listing DealLedger has collected from business brokers' own websites. One row per listing. Rebuilt daily.

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
| `first_seen` | timestamp, UTC | When DealLedger first saw the listing. **Not the date it was listed.** Listings already up when we first crawled a broker get that crawl date |
| `last_seen` | timestamp, UTC | When DealLedger last saw the listing live |
| `description` | text | Start of the broker's description, whitespace collapsed, up to 1,000 characters |

## What's included and excluded

- **Included:** listings on brokers' own websites that are currently active and pass our listing checks.
- **Excluded:** real estate and commercial property, for-sale-by-owner, aggregator and marketplace reposts, sold or withdrawn listings, and pages flagged for review.
- **Not exported yet:** days on market. Listing dates are hard to observe directly, and we won't publish a figure until the method is settled.

## Known gaps

Coverage per field is in `meta.json`. Revenue and city are the thinnest. Fixes are tracked as GitHub issues, and corrections are welcome.

Methodology: [`docs/METHODOLOGY.md`](../../docs/METHODOLOGY.md). Bad row? Open an issue or email info@dealledger.org.
