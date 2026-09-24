# DealLedger Methodology

How DealLedger collects, dates and publishes listings of businesses for sale.

The method is public because the value of the record depends on anyone being
able to check it. The calibration values are not published; everything about
how they are built and how well they perform is.

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

### Estimated dates

Marketplace listings carry sequential listing numbers: 2,400,000 was created
before 2,450,000. Given pairs of (number, date) — anchors — any other number
can be placed on that line. We hold **474 anchors spanning listing 170,683 in
2009 to 2,446,967 in 2026**.

Anchors come from two independent places:

1. **Our own crawl** — listings we watched appear.
2. **Internet Archive captures** — a capture of a listing page proves that
   listing existed on the capture date, which makes the capture an *upper
   bound* on when it went up.

Because numbers are sequential, a capture of a higher number also bounds every
lower number. We sweep from the highest number down, carrying the running
minimum date, so each number inherits the tightest bound available. Dates
derived this way are bounds, so the age they imply is a floor: at least this
long, possibly longer.

**Published:** the method, above, in full, and each record's resulting
`estimated_listed_date`.
**Not published:** the anchor values themselves. That is the calibration.

---

## How Accurate the Dates Are

We measure rather than assert, and publish the results including the ones that
don't flatter us.

### Test one — against our own observations

Where we watched a marketplace listing appear during routine crawling, the
listing number's prediction can be compared against the day we saw it.
~49,000 pairs, grouped by the month we first saw the listing:

| First seen | Pairs | Median error | Within 12 days |
|---|---|---|---|
| Mar 2026 | 2,172 | 72 days | 10% |
| Apr 2026 | 3,707 | 82 days | 11% |
| May 2026 | 31,024 | 66 days | 20% |
| **Jun 2026** | **6,860** | **1 day** | **89%** |
| **Jul 2026** | **749** | **8 days** | **76%** |

March to May are not a measure of the model. Those months include a bulk
ingest of listings that had been up long before we first saw them, so our
sighting is late by construction and the estimate looks early. June and July
are steady-state crawling, where our sighting lands close to the real posting
date — and there the estimate is accurate to within a day or two.

### Test two — against an independent source

Eight Internet Archive captures carry listing numbers inside the calibrated
range, so the archive and the calibration can be compared on the same
listings. A capture must fall on or after the day a listing went up, so every
gap should be positive:

| Listing | Archive capture | Our estimate | Gap |
|---|---|---|---|
| 2,446,967 | Dec 18, 2025 | Dec 2, 2025 | +16 days |
| 2,346,203 | Apr 4, 2025 | Mar 16, 2025 | +19 days |
| 2,341,158 | Apr 4, 2025 | Mar 2, 2025 | +33 days |
| 2,405,299 | Sep 24, 2025 | Aug 22, 2025 | +33 days |
| 2,338,082 | Apr 4, 2025 | Feb 22, 2025 | +41 days |
| 2,394,113 | Jan 2, 2026 | Jul 23, 2025 | +163 days |
| 2,334,126 | Sep 24, 2025 | Feb 11, 2025 | +225 days |
| 2,434,006 | Oct 31, 2025 | Nov 4, 2025 | **−4 days** |

Seven of eight positive. The tight cluster (16–41 days) is the informative
part: the Archive captured those pages a few weeks after we say they were
listed, which is how archive crawling behaves. The two large gaps are pages
the Archive reached months later. The single negative is four days, inside
interpolation rounding.

**What this shows:** two methods built from different evidence — our own
crawl, and a public archive — agree within a few weeks on the same listings,
and neither is systematically early or late by months.
**What it doesn't:** it is not a certified accuracy figure, eight is a small
sample, and estimates remain estimates.

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
- Estimated dates carry meaningful error and are not for legal, regulatory or
  transactional use. Measured accuracy, and the conditions it holds under, are
  above.
- Archive-derived dates are upper bounds. A listing shown as "at least 783
  days" may be older; it cannot be newer.
- Between listing 2,199,972 (May 2024) and 2,312,165 (December 2024) we hold
  no anchor; dates in that range are interpolated across the gap.
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

*Last updated: September 2026 — Methodology version: 4.0.0*
