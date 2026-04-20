# DealLedger Methodology

This document describes how DealLedger collects, timestamps, and publishes small business listing data.

Our methodology is public because the value of this dataset depends on transparency and reproducibility. The calibration mechanics are proprietary and maintained internally.

---

## Core Principles

1. **Observable facts only** — We record what we observe: listing IDs, prices, dates, locations. We do not verify financials, judge quality, or rank listings.

2. **Append-only history** — We never delete records. If a listing disappears, we mark it `removed`. The full history is always preserved.

3. **Open data** — All data is published under CC0. Anyone can download, query, and build on it.

4. **No opinions** — We do not rate brokers, recommend listings, or capture leads. This is a record, not a marketplace.

---

## Data Sources

DealLedger ingests listings from the largest small business marketplace in the United States, owned by CoStar Group. We supplement with buyer interest data from a second CoStar-owned platform.

**What we capture:**
- Listing ID, title, asking price, cash flow, revenue
- Location (city, state)
- Broker account identifier
- Days on market (recovered — see below)
- Buyer interest signals (view counts)
- Quality signals

**Scrape frequency:**
- Daily: New and recently active listings
- Weekly: Full market sweep across all states

**Current coverage:** 47,000+ active listings, updated daily.

---

## Listing Timestamp Methodology

Marketplace listings do not expose publication dates. DealLedger recovers estimated listing dates using a proprietary reverse-engineering methodology based on sequential listing IDs.

**What we publish:**
- `estimated_listed_date` — recovered first publication date
- `days_on_market` — calculated from estimated listed date to today
- Mean error: ±12 days

**What we don't publish:**
- Calibration anchor points
- Rate calculations
- Internal model parameters

The methodology has been validated against known listing dates and maintained since May 2025. Calibration is updated periodically.

### DOM Classification

| Bucket | Days | Signal |
|--------|------|--------|
| Fresh | 0–30 | Recently listed |
| Recent | 31–90 | Active market |
| Aging | 91–365 | Slowing |
| Stale | 366–730 | Negotiating leverage |
| Zombie | 730+ | Distressed / long-tail |

---

## Buyer Interest Data

DealLedger publishes cumulative view counts sourced from a second major marketplace platform operated by the same parent company. View counts represent profile page visits as reported by that platform.

**Coverage:** ~95% of active listings have view count data.

**What this tells you:**
- High views + low DOM = Hot listing (moving fast)
- Low views + low DOM = Hidden Gem (under the radar)
- High views + high DOM = Overpriced? (interest but no offers)
- Low views + high DOM = Dead (ignored by market)

---

## Quality Scoring

Every listing receives a quality score (0–100) based on observable signals:

**Positive signals:**
- Listing comes from a registered broker in our verified broker database
- Broker has a documented transaction history
- Cash flow is disclosed
- Price/cash flow multiple is within normal range (1–5x)
- Asking price is within normal business range
- Has buyer interest data
- Recently listed

**Negative signals:**
- No broker attribution
- Asking price below cash flow (data integrity flag)
- Missing location
- Extended time on market with no activity

**Quality tiers:**
- **Verified** — High confidence, registered broker, complete data
- **Likely Real** — Good signals, minor gaps
- **Unverified** — Incomplete data, treat with caution
- **Likely Junk** — Multiple red flags

Quality scores are recalculated with each weekly refresh.

---

## Broker Verification

DealLedger maintains a database of 7,440+ registered business brokers sourced from public broker registries. Listings matched to this database receive higher quality scores.

Broker inclusion is based on:
- Public registration on major business sale platforms
- Documented transaction history
- Active listing presence

**We do not endorse brokers.** Inclusion in our database reflects public registration only.

---

## What We Don't Do

- **No financial verification** — We report what listings claim.
- **No quality ranking** — Scores reflect data completeness, not business quality.
- **No broker endorsement** — Our database is not a recommendation.
- **No lead capture** — We do not collect buyer or seller contact information.
- **No paywalled data** — Everything we publish is from publicly accessible sources.

---

## Current Statistics (April 2026)

- **47,000+** active listings tracked
- **33,484** verified broker listings (Verified + Likely Real tiers)
- **212 days** average time on market
- **94%** of verified listings have buyer interest data
- **172** Hot listings (high demand, moving fast)
- **2,456** Hidden Gems (fresh, low visibility)

---

## Data Access

**REST API:**
```
GET https://kqckuedsyyosmccushyd.supabase.co/rest/v1/listings
Headers: apikey: [anon key — see repository]
```

**Bulk download:** CSV and JSON exports at dealledger.org

**License:** CC0 — No rights reserved. Use freely for any purpose.

---

## Dispute Process

If you believe our data is incorrect, open a GitHub issue with evidence. We investigate and correct the record with history preserved.

---

*Last updated: April 2026 — Methodology version: 3.0.0*
