# Broker coverage, corrected — domain normalization + franchise roll-up

Item 0(a). Method: PostgREST REST API only (no direct SQL/DB connection available
this session — see `docs/job-audit.md`). Pulled `broker_master` (8,123 rows),
`listings_direct` (71,702 rows, all statuses), and `bizquest_listings` (44,174 rows)
in full via pagination and replicated the join/aggregation logic in Python. The SQL
in `docs/broker-coverage-view.sql` encodes the same logic but has **not been run**
— see the warning at the top of that file.

## The corrected gap number

The first-pass estimate in `docs/bizquest-account-match.md` §4 — **2,545 brokers /
21,842 BBS listings uncovered** — was known-bad (literal domain match, no franchise
roll-up). The real picture, after normalization and roll-up:

| Bucket | Broker-master accounts | Domains (post-rollup) | BBS listings behind them |
|---|---|---|---|
| Covered (DealLedger holds ≥1 active row for that domain) | — | 334 | 13,784 |
| Uncovered (domain resolved, zero DealLedger rows) | — | 735 | 6,490 |
| **Domain unresolvable** (broker_master has no usable website field) | 1,007 | — | 8,892 |

Of the 2,397 brokers named by the account_id join, 4,959 of the 7,440 distinct
`broker_master.account` values (66.7%) resolve to a usable domain at all (including
the Transworld/Sunbelt roll-up); the rest store only a relative BBS profile path
(`broker_master.url` is a relative path in 100% of rows checked — it is never a
website; the real site, when present, lives in `companyurl`, populated on 5,605 of
8,123 rows).

**Honest total:** the old number said "2,545 brokers, 21,842 listings, uncovered."
The corrected, decomposed version is **735 domains confirmed uncovered (6,490
listings)**, plus **1,007 accounts (8,892 listings) we can't classify yet** because
their `broker_master` record has no resolvable website — not necessarily uncovered,
just unmeasured. Combined upper bound if all of the unresolvable ones turn out
uncovered too: 1,742 broker-clusters / 15,382 listings — still 29% of the original
21,842 figure, not because the original was directionally wrong but because most of
what it counted as "uncovered" was actually already covered under a
differently-named domain (Transworld and Sunbelt regional offices alone account for
most of the swing).

## Franchise roll-up applied

Only two roll-ups, both with direct evidence, per the queue's "be conservative"
instruction:

- **Transworld**: 752 `broker_master` rows have "transworld" in `companyname`.
  Their `companyurl` values are fragmented across `tworld.com` (626),
  `tworldcolorado.com` (38), `tworlddfwcentral.com` (24), `tworldma.com` (11),
  `transworldmaine.com` (5), `norcalbizsales.com` (18),
  `buysellbusinessomaha.com` (2), plus 25 with no companyurl at all. `listings_direct`
  shows **zero** active rows under any of the regional variants and 4,530 under
  `tworld.com` alone — confirming DealLedger's crawl aggregates the whole network
  there. All 752 rows roll up to `tworld.com`.
- **Sunbelt**: 228 rows have "sunbelt" in `companyname`, fragmented across
  `sunbeltnetwork.com` (109) and ~15 regional domains (`sunbeltofflorida.com`,
  `sunbeltcharleston.com`, `sunbelttexas.com`, etc.), plus 12 with no companyurl.
  `listings_direct` holds 1,089 rows under `sunbeltnetwork.com` and small handfuls
  under a few regional domains directly (`sunbeltofflorida.com`: 10,
  `sunbelthuntsville.com`: 6, `sunbeltms.com`: 6, `thesunbeltbrokers.com`: 4) — those
  regional ones are genuinely, separately crawled, so only the `sunbeltnetwork.com`
  roll-up target was used; the small directly-crawled regional domains keep their
  own counts rather than being merged in, since merging them would double-count
  against listings DealLedger already attributes to the parent domain in a way I
  could not fully untangle without per-listing-URL inspection.

No other roll-ups were applied. Scanning `companyname` for other "regional office of
one brand" patterns (e.g. multiple companynames sharing a suffix) would likely find
more, but each one needs the same confirm-in-`listings_direct` step Transworld and
Sunbelt got, and that wasn't done for anything beyond what the queue explicitly
named — flagged as follow-up, not fabricated here.

## What's still approximate

- **The "unknown domain" bucket (1,007 accounts / 8,892 listings) is not the same
  claim as "uncovered."** Some of these brokers may already be crawled under a
  domain DealLedger holds — we simply can't derive that domain from
  `broker_master` as stored. Resolving this requires following the BBS profile
  path (e.g. `/business-broker/matt-millsaps/hedgestone/41794/`) to the actual
  agent/firm page to extract a real website, which is out of scope tonight.
- Domain matching is exact-string after normalization (strip protocol/`www.`/path).
  It will not catch e.g. a broker whose BizQuest-listed site is `brokerx.com` but
  whose DealLedger crawl target is `www.brokerx.net` (rebrand, or two live domains)
  — no evidence of this in a spot check, but it wasn't exhaustively ruled out.
- `broker_master.account` is agent/person-level (has `personId`, `firstname`,
  `lastname`), not firm-level — a single firm can have multiple accounts (multiple
  agents), each with their own row and potentially inconsistent `companyurl`
  values. The aggregation groups by resolved domain, not by account, which handles
  this correctly for ranking purposes, but "distinct brokers" in the strict sense
  of distinct firms is not exactly `count(distinct account)`.
- `listings_direct.broker_domain` itself has at least one un-normalized duplicate
  live (`www.sunbeltnetwork.com`, 5 rows, vs `sunbeltnetwork.com`, 1,089) — a
  smaller version of the same normalization problem, on the ingestion side. Worth
  fixing at write time separately from this view.

## Ranked crawl queue — top 20 by BBS listings held

Includes both covered and uncovered so the "value" ranking is visible; `gap` is
`bbs_listings_held − dealledger_active_listings`, floored at 0 (a domain can show
more DealLedger rows than matched BBS listings — DealLedger's crawl often picks up
more of a broker's site than what that same broker mirrors onto BizQuest).

| domain | bbs_listings_held | dealledger_active | gap | broker_master accounts |
|---|---:|---:|---:|---:|
| tworld.com (Transworld network) | 3,554 | 4,530 | 0 | 132 |
| wesellrestaurants.com | 1,095 | 1,670 | 0 | 31 |
| sunbeltnetwork.com (Sunbelt network) | 967 | 1,094 | 0 | 60 |
| hedgestone.com | 522 | 879 | 0 | 4 |
| vestedbb.com | 493 | 3,631 | 0 | 1 |
| murphybusiness.com | 314 | 576 | 0 | 49 |
| fcbb.com | 307 | 1,090 | 0 | 24 |
| eastcoastbusinessbrokers.com | 168 | 82 | **86** | 1 |
| routeconsultant.com | 163 | 0 | **163** | 1 |
| execbb.com | 142 | 1,062 | 0 | 1 |
| restaurantforsales.com | 134 | 156 | 0 | 1 |
| business-team.com | 127 | 187 | 0 | 6 |
| restaurantrealty.com | 126 | 626 | 0 | 1 |
| ninbb.com | 109 | 387 | 0 | 1 |
| poolroutesales.com | 107 | 2 | **105** | 1 |
| stavfranchiseconsulting.com | 100 | 0 | **100** | 1 |
| kensingtoncompany.com | 99 | 309 | 0 | 1 |
| calhouncompanies.com | 98 | 569 | 0 | 1 |
| salehgroup.com | 96 | 1 | **95** | 1 |
| bizbizbiz.com | 96 | 163 | 0 | 1 |

Reading this: most of the highest-value brokers by BBS presence are **already**
well-covered or over-covered by DealLedger's existing crawl (gap = 0) — the
account-id match mostly confirms strength where it already exists rather than
revealing new territory at the very top. The real crawl queue value is in the
735-domain uncovered tail (routeconsultant.com, stavfranchiseconsulting.com,
poolroutesales.com, salehgroup.com above are the first real targets it surfaces) and
in resolving the 1,007-account unknown-domain bucket, not in the top of this table.
