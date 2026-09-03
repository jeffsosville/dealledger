# Corrections to DEALLEDGER_MISSION.md

**Read this alongside the mission doc.** Verified against the database 2026-09-03.
The mission doc's state table overstates the remaining gap by roughly 3x, and any
session reasoning from it will draw the wrong strategic conclusion.

---

## 1. "BizBuySell's index is ~133,000" — wrong

The live BizBuySell/BizQuest index is **~43,600 listings**, not ~133,000.

Measured from `bizquest_listings`, a complete snapshot crawled 2026-09-02:

```
total rows        44,174
distinct numbers  44,174
active            43,629
last_seen         2026-09-02 (all rows fresh)
```

BizQuest is a pass-through of BizBuySell on the same listing numbers, so this is a
direct read of the competitor's standing inventory.

**Where 133,000 came from:** the `listings` table's total row count. That table holds
87,538 distinct BizBuySell listing numbers accumulated between 2025-08-28 and
2026-07-16, plus broker-direct rows. 87,538 is eleven months of *churn* — every
listing number ever observed, including everything since sold or expired — not
inventory at a point in time.

## 2. The coverage gap is much smaller than stated

| | Listings | Share of BBS |
|---|---|---|
| BBS / BizQuest live index | 43,629 | — |
| DealLedger `listings_direct` active | 36,176 | **83%** |
| Bridged and live on dealledger.org | 26,505 | **61%** |

The mission doc's framing — "~28,000 active vs every active US listing," with BBS at
133,000 — implies roughly 20% coverage and a five-fold climb. The measured figure is
83% of the competitor index on the raw table.

The gap to parity on the raw index is **~7,500 listings**.

Caveat: 83% is size parity, not proof of holding the same 43,629 listings. The overlap
is unmeasured — see `docs/bizquest-account-match.md` for the path to measuring it.

## 3. The binding constraint is internal, not external

**9,671 rows sit in `listings_direct` and do not reach the site.** They are lost
between `listings_direct` (36,176 active) and `listings` (26,505 active broker-direct)
— rejected by `looks_like_real_listing()` / `is_listing_junk()` in
`bridge_direct_to_listings()`, or never bridged at all.

That is larger than the entire remaining gap to BizBuySell parity.

So the doc's "two gaps, and the second is the surprise" needs a third entry, and it is
the biggest of the three: the gates. Those same gates are what timed out the bridge for
six nights (see `docs/incident-2026-09-02-bridge-timeout.md`) and what let 959 Pavilion
placeholder rows onto the homepage. Per principle 8, their false-reject rate has never
been measured, and 9,671 rows is the size of the thing nobody has looked at.

## 4. Other state-table figures now stale

| Doc says | Measured 2026-09-03 |
|---|---|
| ~28,000 broker-direct active | 36,176 in `listings_direct`; 26,505 bridged |
| 155 of 740 brokers producing | 153 producing in 7 days; 197 produced on Sept 2 alone |
| ~21,800 untagged by vertical | 19,251 |
| 1,171 brokers never crawled | `v_broker_crawl_candidates` returns 1,505 rows — reconcile before quoting either |
| ~10,000 listings in the uncrawled backlog | not verified by measurement; inherited figure |

## 5. Unverified figures still circulating

These appear in the mission doc and have been repeated as though measured. None has
been confirmed against the database:

- `NO_PATTERN` on "roughly half of successful fetches" — the `crawl_failures` /
  `crawl_runs` tables are orphaned from the scraper and hold zero rows, so there is
  no stored basis for this. It may be true; it is not currently measurable.
- "~10,000 active listings" behind the uncrawled broker backlog.
- The ~103-day sold vs ~17-day advertised DOM finding. This is the headline
  publishable claim and it should be re-derived before it is published again,
  particularly now that median DOM on the direct index reads 2 days because of the
  Sept 2 backlog cohort.

## 6. Suspected data bug worth an hour

`bizquest_listings` flags **43,044 of 43,629 active rows (98.7%) as `is_fsbo`**. That
cannot be true of BizBuySell, which is predominantly broker-listed. The flag is derived
from an empty `broker_company` that the parser never populates — `account_id` is
present on 34,472 rows and 29,166 of them join to a named broker in `broker_master`.

This matters strategically: FSBO vs broker-listed determines how much of the
competitor's index is reachable by broker-direct sourcing at all. Full detail in
`docs/bizquest-account-match.md`.

---

## Method note

Everything in sections 1–4 and 6 comes from queries run against
`kqckuedsyyosmccushyd` on 2026-09-03. Section 5 lists figures that could *not* be
verified. The distinction is the point: the mission doc's own standing constraint is
that every published number has a stated method, and the 133,000 had none.
