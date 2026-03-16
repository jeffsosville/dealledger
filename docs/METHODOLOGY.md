# DealLedger Methodology

This document describes how DealLedger collects, timestamps, and publishes small business listing data.

Our methodology is public because the value of this dataset depends on transparency and reproducibility. If you disagree with our approach, open an issue.

---

## Core Principles

1. **Observable facts only** — We record what we observe: listing IDs, prices, dates, locations, descriptions. We do not verify financials, judge quality, or rank listings.

2. **Append-only history** — We never delete records. If a listing disappears, we mark it `removed`. If it reappears, we flag it. The full history is always preserved.

3. **Open data** — All data is published under CC0. Anyone can download, query, and build on it.

4. **Reproducible** — The methodology is documented, the code is open source, and the calibration anchors are published. Anyone can verify our results.

---

## Two Data Layers

DealLedger collects listings from two complementary sources: **marketplace listings** and **broker-direct listings**. Together they form the most comprehensive open record of small businesses for sale in America.

---

## Layer 1: Marketplace Listings

DealLedger ingests listings from BizBuySell, the largest small business marketplace in the United States, owned by CoStar Group. BizBuySell aggregates listings from thousands of business brokers nationwide and represents the broadest cross-section of the market.

**What we capture:**
- Listing ID, title, asking price, cash flow
- Location (city, state)
- Broker account identifier
- Listing URL
- Price reduced flag
- Category / business type

**Storage:** Supabase — publicly queryable via REST API.

**Scrape frequency:**
- Daily: New and recently active listings
- Weekly: Full market sweep across all categories and states

**Current coverage:** 40,552+ listings, updated daily.

---

## Layer 2: Broker-Direct Listings

DealLedger also scrapes listings directly from individual business broker websites — bypassing marketplaces entirely. This captures listings that brokers publish on their own sites but may not syndicate to aggregators, and provides a source of truth independent of marketplace data.

**What we capture:**
- All fields available on the broker's site
- Direct source URL (broker website, not marketplace)
- Scrape timestamp

**Storage:** `data/ledger.jsonl` in the GitHub repository — append-only flat file, committed daily.

**Current coverage:** 9,600+ listings across 1,700+ tracked brokers, 50 states.

**Broker inclusion criteria:**
- Public listings page (no login required)
- At least 3 active listings
- Identifiable company name

---

## Listing Timestamp Methodology

Marketplace listings do not expose publication dates through standard interfaces. DealLedger recovers estimated listing dates using a reverse-engineering methodology based on sequential listing IDs.

### How It Works

Every BizBuySell listing is assigned a sequential integer ID embedded in its URL:

```
https://www.bizbuysell.com/business-opportunity/[slug]/[LISTING_ID]/
```

These IDs are issued in monotonically increasing order. By measuring the rate at which new IDs appear over time and calibrating against known anchor points, we estimate when any listing was first published.

### Calibration Model

| Parameter | Value |
|-----------|-------|
| Anchor listing ID | 2,367,857 |
| Anchor date | May 14, 2025 |
| Observed rate | 373.8 new listings per day |
| Mean error | ±12 days |

**Formula:**

```
estimated_listed_date = anchor_date + ((listing_id - anchor_id) / rate)
days_on_market = today - estimated_listed_date
```

The model is recalibrated periodically. Calibration history is maintained in the repository.

### DOM Classification

| Bucket | Days | Signal |
|--------|------|--------|
| Fresh | 0–30 | Recently listed |
| Recent | 31–90 | Active market |
| Aging | 91–180 | Slowing |
| Stale | 181–365 | Negotiating leverage |
| Zombie | 365+ | Distressed / long-tail |

### Limitations

- Listings with non-standard IDs (franchise ads, sponsored placements) are excluded
- Model applies to listing IDs in range 1,000,000–3,500,000
- Estimated dates reflect first publication, not most recent update
- Broker-direct listings use `first_seen` (scrape date) as the observed date

---

## Zombie Rate

A **zombie listing** is any listing with `days_on_market > 365` and no confirmed sale signal.

As of March 2026: **28.1% zombie rate** across 40,552 tracked marketplace listings. Median DOM: 256 days.

The zombie rate is published monthly in the DealLedger SMB Liquidity Report.

---

## Disappearance Detection

When a listing present in a previous scrape is absent from a current scrape, it is flagged `removed`. Possible reasons include:

- Business sold
- Listing expired or withdrawn
- Listing relisted under a new ID
- Broker site restructured

DealLedger records the observable fact (disappearance) without inferring cause. A future verification layer will attempt to distinguish sold vs. expired vs. relisted.

---

## Historical Snapshots

| Snapshot | Date | Listings | Layer |
|----------|------|----------|-------|
| State-by-state | May 2025 | 32,012 | Marketplace |
| Partial market | August 2025 | 10,000 | Marketplace |
| Partial market | October 2025 | 10,000 | Marketplace |
| Full market | November 2025 | 41,703 | Marketplace |
| Broker-direct | Ongoing | 9,600+ | Broker-direct |
| Cleaning vertical | March 2026 | 1,161 | Marketplace |

---

## What We Don't Do

- **No financial verification** — We report what listings claim. We do not audit financials.
- **No quality ranking** — We do not rate, rank, or recommend listings or brokers.
- **No broker endorsement** — Scraping a broker does not imply endorsement.
- **No lead capture** — We do not collect buyer or seller contact information.
- **No paywalled data** — Everything we publish is from publicly accessible sources.

---

## Data Access

**Marketplace listings — Supabase REST API:**
```
GET https://kqckuedsyyosmccushyd.supabase.co/rest/v1/listings
Headers: apikey: [anon key — see repository]
```

**Broker-direct listings — GitHub:**
```
https://github.com/jeffsosville/dealledger/blob/main/data/ledger.jsonl
```

**Bulk download:** CSV and JSON exports at dealledger.org

**License:** CC0 — No rights reserved. Use freely for any purpose.

---

## Dispute Process

If you believe our data is incorrect, open a GitHub issue with evidence. We investigate and correct the record with history preserved and the correction documented.

---

## Roadmap

- Merge marketplace and broker-direct into a single unified ledger
- Relist detection and sell-through rate estimation
- Monthly SMB Liquidity Report (state, category, price band breakdowns)
- Broker performance scoring (DOM by broker, sell-through rate)
- API v2 with filtering by DOM, state, category, price range

---

*Last updated: March 2026 — Methodology version: 2.0.0*
