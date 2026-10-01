# DealLedger Methodology

How DealLedger collects, dates and publishes listings of businesses for sale.

The method is public because the value of the record depends on anyone being
able to check it. Every date in the open dataset is an observation you can
reproduce from the daily snapshots in this repository.

---

## Core Principles

1. **Observable facts only** — We record what we see: listing URLs, titles,
   prices, locations, and the dates we saw them. We do not verify financials,
   judge quality, or rank listings.
2. **Append-only history** — Nothing is deleted. A listing that disappears is
   marked inactive, never removed. The history is the point.
3. **Open data** — Published under CC0. Download it, query it, build on it.
4. **No opinions** — Not a marketplace. A record.

---

## Data Sources

DealLedger reads business-for-sale listings directly from brokers' own
websites — about 770 of them as of 24 September 2026, and rising as discovery adds
more. Every listing is traceable to the broker page it came from.

**What we capture:** title, URL, asking price, cash flow, revenue where
published, location, broker domain, and the dates we first and last saw the
listing.

**Cadence:** the broker index is refreshed on a rotation; each producing
broker is crawled at least weekly, and listings appearing or disappearing
between crawls are recorded as changes.

---

## Date Recovery

Almost nobody publishes the date a listing went up. We produce two kinds of
date and label which is which.

### Observed dates

For a listing on a broker's own site, the date is the first day we saw it.
That is an observation. Where a listing was already up the first time we
crawled that broker, we can only say it is **at least** that old, and we show
it as a floor (`176+ days`) rather than a bare number.

### What the dataset does not contain

The open dataset contains observed dates only. The lookup box on
dealledger.org can also give a rough, clearly labelled estimate for some
marketplace listing URLs; those estimates come from a separate calibration,
are not part of the CC0 dataset, and are never mixed into it.

---

## Relist Detection

When a listing disappears and a similar one appears later, we flag the new
listing as relisted. Detection combines broker identity, geography, price
band, category and content fingerprinting. A relist is meaningful: the
business came back to market, often at a different price, after failing to
sell.

---

## Data Quality

Rows are gated before they enter the index:

- **Junk filter** — nav fragments, status badges, price fragments, error
  pages and other page furniture are rejected, never published as listings.
- **Residential/IDX filter** — MLS numbers, bed and bath counts and
  street-address titles are residential property, not businesses. Tested on
  row content rather than domain, because blocking a domain only catches the
  feed you already found.
- **Financial sanity** — one number cannot be three different financials. A
  value repeated across asking price, cash flow and revenue survives only as
  the asking price; cash flow at or above revenue is dropped. A missing number
  is honest, a wrong one is not.

---

## What We Don't Do

- **No financial verification** — we report what the broker published.
- **No ranking or endorsement** — inclusion is not a recommendation.
- **No lead capture** — we collect no buyer or seller contact information.
- **No paywalled data** — everything published comes from publicly accessible
  pages.

---

## Limitations

- Coverage is incomplete. Brokers not yet in our source list, off-market
  deals and private-network listings are not captured.
- Observed history starts in March 2026, when broker-direct crawling began.
  A listing that was already up when we first crawled its broker is shown as
  a floor ("176+ days"): at least that old, possibly older.
- A listing is only as fresh as the last crawl of its broker. Each producing
  broker is crawled at least weekly.
- Listings can be reposted or refreshed in ways that affect apparent age. We
  detect some of these, not all.

---

## Data Access

**REST API**

```
GET https://kqckuedsyyosmccushyd.supabase.co/rest/v1/mv_listings_page
Headers: apikey: [anon key — published in the site source]
```

**License:** CC0. No rights reserved, no attribution required.

---

## Corrections

If the record is wrong, open a GitHub issue with evidence. We correct it and
keep the history.

---

*Last updated: September 2026 — Methodology version: 5.0.0*
