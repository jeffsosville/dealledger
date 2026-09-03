# BizQuest account_id match — a ranked crawl queue and a DOM spine

Measured 2026-09-03 against `kqckuedsyyosmccushyd`.

**Short version:** for two thirds of BizBuySell's live index you can already name the
broker and you already have their website. Nobody has used it, because everyone has
been looking at `broker_company`, which is empty.

---

## 1. The field everyone is looking at is the wrong one

`bizquest_listings`, active rows only:

| Field | Populated | Share |
|---|---|---|
| `broker_company` | 585 | 1.3% |
| `broker_contact` | 2,031 | 4.7% |
| **`account_id`** | **34,472** | **79%** |
| `url` | 43,629 | 100% |
| flagged `is_fsbo` | 43,044 | 98.7% |

The 98.7% FSBO rate is not real. `is_fsbo` is being derived from an empty
`broker_company`, and `broker_company` is empty because the BizQuest parser is not
capturing it — not because the listings are for sale by owner. At least 29,166 of
them are demonstrably broker-listed.

**Two consequences.** Any analysis segmenting on `is_fsbo` is wrong today. And the
actual broker identity was in `account_id` the whole time.

## 2. `account_id` joins straight to `broker_master`

```sql
select count(*)
from bizquest_listings b
join broker_master bm
  on bm.account = nullif(regexp_replace(b.account_id,'[^0-9]','','g'),'')::bigint
where b.is_active;
```

| | |
|---|---|
| Active BizQuest listings with an `account_id` | 34,472 |
| Distinct accounts | 3,135 |
| **Joining cleanly to `broker_master.account`** | **29,166** |
| Distinct brokers thereby identified | 2,397 |
| Of those, with a website on file | 29,166 (all) |

29,166 of 43,629 — **67% of BizBuySell's live index** — is attributable to a named
broker whose site URL you already hold. For context: 153 brokers produced in the last
7 days. This names 2,397.

## 3. What it unlocks

**A ranked crawl queue.** Order brokers by how many BBS listings they hold, descending.
Principle 10 says crawl what works before what doesn't — this tells you what a broker
is *worth* before you spend a fetch on them. Strictly better than staleness ordering
or working `v_broker_crawl_candidates` blind.

**A real coverage metric.** Per broker: "we hold X of the Y listings they run on
BizBuySell." For 2,397 brokers. That is the number the coverage row has never had.

**The DOM cross-match spine.** Once listings are matched within a broker (tens of rows,
not 36k × 43k), the BizQuest listing number yields a real listed date via the existing
anchored interpolation. `bbs_listing_id` is currently null on all 36,176 active direct
rows, so this path supplies nothing today.

## 4. Caveat on the gap number — do not quote it yet

A first pass suggested 2,545 brokers holding 21,842 BBS listings we hold nothing from.
**That is an upper bound and is known to be wrong.**

The join matched `broker_master.companyurl`/`url` against
`listings_direct.broker_domain` literally, and fails two ways:

- Many `broker_master` rows store a relative BBS profile path
  (`/business-broker/alex-beringer/fxg-management/39998/`) rather than a website.
- Franchise sub-brands don't match. It called "Sunbelt Business Advisors /
  sunbeltmidwest.com" and several Transworld regional offices uncovered, while we hold
  1,089 rows under `sunbeltnetwork.com` and 4,530 under `tworld.com`.

The method is right; the domain normalization is not. Needs real-website extraction
for rows holding a BBS profile path, plus a franchise-parent mapping. Until then the
honest statement is "2,397 identified brokers, coverage per broker not yet measurable."

## 5. Sequential listing IDs — a second dating route

Domains where essentially every listing URL carries a 4+ digit numeric ID:

| Domain | Active rows | With numeric ID | First crawled |
|---|---|---|---|
| vestedbb.com | 3,631 | 3,631 | 2026-07-15 |
| wesellrestaurants.com | 1,669 | 1,669 | 2026-07-09 |
| fcbb.com | 1,090 | 1,090 | 2026-07-07 |
| sunbeltnetwork.com | 1,089 | 1,089 | 2026-03-24 |
| execbb.com | 1,062 | 1,062 | 2026-04-27 |
| hedgestone.com | 874 | 874 | 2026-04-17 |
| murphybusiness.com | 576 | 576 | 2026-07-07 |
| websiteclosers.com | 540 | 540 | 2026-03-24 |
| linkbusiness.com | 456 | 456 | 2026-04-27 |
| idx.michelephillipsrealtor.com | 498 | 498 | 2026-07-03 |

~11,500 rows, roughly a third of the active index. If a broker numbers sequentially by
creation, per-broker anchored interpolation works exactly like the BBS listing-number
method — same `dom_anchors` machinery, one calibration set per broker.

The anchors are free and self-accumulating: every listing observed *appearing* on a
broker already under regular crawl is a genuine `(date, id)` pair. FCBB has been
crawled since 2026-07-07, so two months of real appearances are already available, and
it improves nightly.

**Not yet verified:** that these IDs are monotonic in creation order. The ranges were
compared as strings in this pass, not numerically. First step is a numeric
monotonicity check per domain against known `first_seen` dates — if IDs and dates
don't correlate for a broker, that broker is out.

Transworld is the counter-example: 4,530 rows but only 68 numeric URLs. Slug URLs, so
it needs the on-page-date route instead.

## 6. Suggested order

1. **Fix the BizQuest parser** to capture `broker_company`, and re-derive `is_fsbo`
   from something real. Cheap, unblocks everything downstream.
2. **Build the account_id → broker_master join as a view**, with proper domain
   normalization and franchise roll-up. Output: ranked crawl queue plus per-broker
   coverage.
3. **Numeric monotonicity check** on the ten domains above; interpolate where it holds.
4. **On-page published date** at scrape time (JSON-LD `datePosted`,
   `article:published_time`, sitemap `lastmod`) for everyone else — including
   Transworld, the largest single domain.

Items 1 and 2 change what "work the backlog" means. Today it is worked blind by
staleness; after this it is worked in descending order of measured value.
