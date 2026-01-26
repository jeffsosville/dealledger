# DealLedger Methodology

This document describes how DealLedger collects, verifies, and classifies business-for-sale listings.

Our methodology is public because verification requires transparency. If you disagree with our approach, open an issue.

---

## Core Principles

1. **Broker-direct only** — We scrape original broker websites, not aggregators. BizBuySell, BizQuest, and similar aggregators contain duplicates, stale listings, and unverifiable data. Broker-direct is the source of truth.

2. **Append-only history** — We never delete records. If a listing disappears, we mark it `removed`. If it reappears, we mark it `relisted`. The full history is always preserved.

3. **Observable facts only** — We record what we observe: URLs, prices, dates, text. We do not infer intent, verify financials, or judge quality.

4. **Reproducible** — Anyone can run our scrapers and methodology against the same sources and get the same results.

---

## Data Collection

### What We Scrape

For each broker website, we extract:
- Listing URLs
- Listing titles and descriptions
- Asking prices
- Location information
- Business details (revenue, cash flow, employees, etc.)
- Deal terms (seller financing, franchise, etc.)
- Broker contact information

### Scrape Frequency

- **Active brokers**: Daily
- **Inactive brokers** (no changes in 30 days): Weekly
- **New broker onboarding**: Manual review, then automated

### Source Verification

A listing is only included if:
1. It appears on a broker's primary website (not an aggregator)
2. The URL is accessible and returns valid content
3. Minimum required fields are present (title, broker, status)

---

## Normalization

Raw scraper output is normalized to the [standard schema](SCHEMA.md).

### Price Normalization

- All prices converted to USD integers
- "Contact for Price" → `asking_price: null`, `price_hidden: true`
- Price ranges → lower bound used, noted in flags
- Non-USD prices → converted at scrape-time exchange rate

### Location Normalization

- State names → two-letter codes (e.g., "Texas" → "TX")
- Countries → ISO 3166-1 alpha-2 (e.g., "United States" → "US")
- "Confidential" locations → `location_hidden: true`

### Vertical Classification

Listings are classified into verticals based on:
1. Broker's category (if reliable)
2. Title keywords
3. Description analysis

When ambiguous, we use `other` and flag for manual review.

---

## Deduplication

### Within-Broker Duplicates

Same broker, same listing appearing at multiple URLs:
- Keep the canonical URL (usually the oldest)
- Record aliases in metadata

### Cross-Broker Duplicates

Same business listed with multiple brokers:
- **We do not merge these** — each broker's listing is a separate record
- We flag `duplicate_suspected` when similarity score exceeds threshold
- Determination of "same business" requires manual verification

### Similarity Detection

We compute similarity based on:
- Normalized title (Levenshtein distance)
- Location match
- Price within 20%
- Financial metrics within 25%

Threshold for `duplicate_suspected` flag: 85% similarity score.

---

## Change Detection

### Content Hashing

Each listing gets a SHA-256 hash of normalized content:
```
hash = sha256(
  title + asking_price + revenue + cash_flow + 
  city + state + description_first_500_chars
)
```

When hash changes, we record the diff in history.

### Price Changes

Price changes are always recorded with:
- Previous price
- New price
- Timestamp
- Percent change

### Status Changes

Status transitions are tracked:
```
active → removed    (listing disappeared)
active → sold       (marked sold by broker)
active → pending    (under contract)
removed → active    (relisted - triggers flag)
removed → relisted  (explicit relist status)
```

---

## Verification & Confidence

### Confidence Score

Each listing gets a confidence score (0.0 - 1.0) based on:

| Factor | Weight |
|--------|--------|
| Source URL accessible | 0.30 |
| Price present | 0.15 |
| Location present | 0.15 |
| Financials present | 0.20 |
| Broker verified | 0.10 |
| Recently observed (< 7 days) | 0.10 |

Score = sum of applicable weights.

### Warning Flags

Listings are flagged (not removed) for:

| Flag | Trigger |
|------|---------|
| `price_drop_50` | Price dropped >50% from any historical price |
| `relist_detected` | Listing reappeared after being marked removed/sold |
| `stale_90` | No changes observed in 90+ days but still "active" |
| `duplicate_suspected` | >85% similarity to another listing |
| `source_404` | Source URL returned 404 on most recent scrape |
| `price_suspicious` | Price/revenue ratio outside normal bounds |

Flags are informational. We don't remove listings based on flags.

---

## What We Don't Do

### No Financial Verification

We report what brokers claim. We do not:
- Verify revenue or cash flow
- Audit financial statements  
- Confirm business existence

### No Quality Judgment

We do not rate or rank listings. We report observable facts.

### No Broker Endorsement

Listing a broker does not imply endorsement. We scrape publicly available data.

### No Aggregator Data

We do not scrape BizBuySell, BizQuest, BusinessBroker.net, or similar aggregators. These sources contain:
- Duplicate listings from multiple brokers
- Stale listings not updated when sold
- Listings without verifiable broker sources

Aggregator data pollutes the ledger. Broker-direct only.

---

## Broker Inclusion Criteria

A broker is included if:

1. **Public listings page** — Listings visible without login
2. **Scrapable** — Standard HTML (JS-rendered is fine)
3. **Minimum activity** — At least 5 active listings
4. **Identifiable** — Clear company name and contact info

We do not include:
- Brokers requiring login to view listings
- Marketplaces without identifiable broker sources
- Individual FSBO listings (for now)

---

## Dispute Process

If you believe our data is incorrect:

1. **Open an issue** with evidence
2. We will investigate within 7 days
3. If confirmed, we will:
   - Correct the record (with history preserved)
   - Document the correction in changelog
   - Credit the reporter (if desired)

We do not remove listings on request without evidence. The ledger is append-only.

---

## Methodology Changes

Changes to this methodology are:
- Documented in CHANGELOG.md
- Announced before implementation (when possible)
- Applied prospectively (historical data not re-processed unless critical)

Major methodology changes trigger a new data version.

---

## Replication

To replicate our results:

1. Clone the repository
2. Run scrapers against the same broker list
3. Apply normalization and deduplication
4. Compare output to published snapshot

Differences may occur due to:
- Timing (listings change)
- Scraper version differences
- Edge cases in normalization

If you find systematic differences, open an issue. We want to know.

---

*Last updated: 2026-01-23*
*Methodology version: 1.0.0*
